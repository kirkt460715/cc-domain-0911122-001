import copy

from registry_bridge.errors import Conflict, InvalidMessage, LeaseLost, StaleRevision
from registry_bridge.feeds.receiver import receive
from registry_bridge.ledger import leases, projection
from registry_bridge.query import consumer
from registry_bridge.query.subscriptions import Subscriptions
from tests.support import LedgerCase, registration, transition


class ConsumerTests(LedgerCase):
    def test_commit_order_delivers_independent_issuer_then_late_predecessors(self):
        receive(self.store, transition(2, 2, 'suspend'))
        receive(self.store, registration(issuer='south'))
        self.assertEqual(consumer.stable_ceiling(self.store), 1)
        self.assertEqual(consumer.changes(self.store)['rows'][0]['issuer'], 'south')
        receive(self.store, registration())
        batch = consumer.changes(self.store)
        self.assertEqual(batch['next_position'], 3)
        self.assertEqual(len(batch['rows']), 3)
        self.assertEqual(batch['rows'][1]['asset']['state'], 'active')
        self.assertEqual(batch['rows'][2]['asset']['state'], 'suspended')

    def test_checkpoint_compare_and_swap_and_monotonicity(self):
        receive(self.store, registration())
        receive(self.store, registration(2, asset='asset2'))
        first = consumer.advance(self.store, 'reader', 1, 0)
        self.assertEqual(first['revision'], 1)
        with self.assertRaises(StaleRevision):
            consumer.advance(self.store, 'reader', 2, 0)
        with self.assertRaises(Conflict):
            consumer.advance(self.store, 'reader', 0, 1)
        with self.assertRaises(Conflict):
            consumer.advance(self.store, 'reader', 3, 1)
        self.assertEqual(consumer.advance(self.store, 'reader', 1, 1), first)
        self.assertEqual(consumer.advance(self.store, 'reader', 2, 1)['revision'], 2)

    def test_fenced_checkpoint_requires_matching_lease(self):
        receive(self.store, registration())
        token = leases.acquire(self.store, 'consumer:reader', 'worker', ttl=2)
        wrong = leases.acquire(self.store, 'consumer:other', 'worker')
        with self.assertRaises(Conflict):
            consumer.advance(self.store, 'reader', 1, 0, token=wrong)
        self.clock.tick(2)
        with self.assertRaises(LeaseLost):
            consumer.advance(self.store, 'reader', 1, 0, token=token)
        fresh = leases.acquire(self.store, 'consumer:reader', 'worker2')
        self.assertEqual(consumer.advance(self.store, 'reader', 1, 0, token=fresh)['position'], 1)

    def test_export_ack_requires_digest_and_matching_checkpoint(self):
        receive(self.store, registration())
        export = consumer.create_export(self.store, 'reader')
        self.assertEqual(export['count'], 1)
        with self.assertRaises(Conflict):
            consumer.acknowledge_export(self.store, export['export_id'], 'bad', 0)
        result = consumer.acknowledge_export(self.store, export['export_id'], export['sha256'], 0)
        self.assertEqual(result['checkpoint']['position'], 1)
        repeated = consumer.acknowledge_export(self.store, export['export_id'], export['sha256'], 0)
        self.assertEqual(repeated['status'], 'already_acknowledged')
        self.assertEqual(consumer.lag(self.store)[0]['lag'], 0)

    def test_export_page_limits_preserve_progress(self):
        for seq in range(1, 5):
            receive(self.store, registration(seq, asset=f'asset{seq}'))
        first = consumer.create_export(self.store, 'reader', limit=2)
        self.assertEqual(first['upper'], 2)
        self.assertEqual(first['count'], 2)
        self.assertEqual(consumer.create_export(self.store, 'reader', limit=2)['export_id'], first['export_id'])
        consumer.acknowledge_export(self.store, first['export_id'], first['sha256'], 0)
        second = consumer.create_export(self.store, 'reader', limit=2)
        self.assertEqual(second['lower'], 2)
        self.assertEqual(second['upper'], 4)
        self.assertNotEqual(second['export_id'], first['export_id'])


class SubscriptionTests(LedgerCase):
    def setUp(self):
        super().setUp()
        self.subscriptions = Subscriptions(self.store)

    def test_holder_change_emits_removal_for_previous_subscriber(self):
        self.subscriptions.create('owner-one', holders=['holder1'])
        receive(self.store, registration())
        first = self.subscriptions.poll('owner-one')
        self.assertEqual(first['payload']['messages'][0]['action'], 'upsert')
        self.subscriptions.acknowledge(first['delivery_id'], first['digest'])
        receive(self.store, transition(2, 2, 'amend', changes={'holder': 'holder2'}))
        second = self.subscriptions.poll('owner-one')
        self.assertEqual(len(second['payload']['messages']), 1)
        self.assertEqual(second['payload']['messages'][0]['action'], 'remove')
        self.assertIsNone(second['payload']['messages'][0]['asset'])
        self.assertEqual(second['payload']['messages'][0]['revision'], 2)

    def test_state_filter_emits_remove_then_reappearance(self):
        self.subscriptions.create('active-only', states=['active'])
        receive(self.store, registration())
        receive(self.store, transition(2, 2, 'suspend'))
        receive(self.store, transition(3, 3, 'restore'))
        delivery = self.subscriptions.poll('active-only')
        self.assertEqual([row['action'] for row in delivery['payload']['messages']], ['upsert', 'remove', 'upsert'])
        self.assertEqual(delivery['lower_position'], 0)
        self.assertEqual(delivery['upper_position'], 3)
        self.assertEqual(self.subscriptions.backlog('active-only')['unscanned_positions'], 3)

    def test_redelivery_uses_identical_payload_and_increments_attempts(self):
        self.subscriptions.create('reader')
        receive(self.store, registration())
        first = self.subscriptions.poll('reader')
        receive(self.store, registration(2, asset='asset2'))
        repeated = self.subscriptions.poll('reader')
        self.assertEqual(repeated['delivery_id'], first['delivery_id'])
        self.assertEqual(repeated['payload'], first['payload'])
        self.assertEqual(repeated['attempts'], 2)
        self.subscriptions.acknowledge(first['delivery_id'], first['digest'])
        next_delivery = self.subscriptions.poll('reader')
        self.assertEqual(next_delivery['lower_position'], 1)
        self.assertEqual(next_delivery['upper_position'], 2)

    def test_filtered_empty_delivery_still_advances_scan(self):
        self.subscriptions.create('south-only', issuers=['south'])
        receive(self.store, registration())
        delivery = self.subscriptions.poll('south-only')
        self.assertEqual(delivery['payload']['messages'], [])
        self.assertEqual(delivery['upper_position'], 1)
        self.subscriptions.acknowledge(delivery['delivery_id'], delivery['digest'])
        self.assertEqual(self.subscriptions.get('south-only')['position'], 1)
        self.assertEqual(self.subscriptions.backlog('south-only')['unscanned_positions'], 0)

    def test_pending_delivery_blocks_filter_reconfiguration(self):
        config = self.subscriptions.create('reader')
        receive(self.store, registration())
        delivery = self.subscriptions.poll('reader')
        with self.assertRaises(Conflict):
            self.subscriptions.configure('reader', 1, issuers=['south'], holders=[], states=[])
        self.subscriptions.acknowledge(delivery['delivery_id'], delivery['digest'])
        updated = self.subscriptions.configure('reader', 1, issuers=['south'], holders=[], states=[])
        self.assertEqual(updated['revision'], 2)
        self.assertEqual(updated['position'], 1)
        with self.assertRaises(StaleRevision):
            self.subscriptions.configure('reader', 1, issuers=[], holders=[], states=[])

    def test_wrong_ack_digest_does_not_move_subscription(self):
        self.subscriptions.create('reader')
        receive(self.store, registration())
        delivery = self.subscriptions.poll('reader')
        with self.assertRaises(Conflict):
            self.subscriptions.acknowledge(delivery['delivery_id'], '0' * 64)
        self.assertEqual(self.subscriptions.get('reader')['position'], 0)
        self.assertEqual(self.subscriptions.delivery(delivery['delivery_id'])['state'], 'pending')
        acknowledged = self.subscriptions.acknowledge(delivery['delivery_id'], delivery['digest'])
        self.assertEqual(acknowledged['status'], 'acknowledged')
        self.assertEqual(self.subscriptions.acknowledge(delivery['delivery_id'], delivery['digest'])['status'], 'already_acknowledged')
