from registry_bridge.errors import Conflict, LeaseLost, StaleRevision
from registry_bridge.feeds.receiver import receive
from registry_bridge.feeds.contracts import ContractGate
from registry_bridge.ledger import leases, issuers, audit
from registry_bridge.reconcile.worker import RecoveryQueue
from tests.support import LedgerCase, registration, transition


class LeaseTests(LedgerCase):
    def test_acquire_renew_release_and_generation(self):
        first = leases.acquire(self.store, 'worker', 'alice', ttl=10)
        self.assertEqual(first['generation'], 1)
        with self.assertRaises(Conflict):
            leases.acquire(self.store, 'worker', 'bob')
        self.clock.tick(5)
        renewed = leases.renew(self.store, first, ttl=20)
        self.assertGreater(renewed['expires_at'], first['expires_at'])
        leases.release(self.store, renewed)
        next_token = leases.acquire(self.store, 'worker', 'bob')
        self.assertEqual(next_token['generation'], 2)
        with self.assertRaises(LeaseLost):
            leases.assert_held(self.store, first)

    def test_exact_expiry_fences_previous_holder(self):
        token = leases.acquire(self.store, 'worker', 'alice', ttl=5)
        self.clock.tick(5)
        with self.assertRaises(LeaseLost):
            leases.renew(self.store, token)
        self.assertFalse(leases.list_leases(self.store)[0]['active'])
        replacement = leases.acquire(self.store, 'worker', 'alice')
        self.assertGreater(replacement['generation'], token['generation'])
        with self.assertRaises(LeaseLost):
            leases.release(self.store, token)
        self.assertEqual(leases.assert_held(self.store, replacement)['owner'], 'alice')


class RecoveryTests(LedgerCase):
    def setUp(self):
        super().setUp()
        self.queue = RecoveryQueue(self.store)

    def test_recovery_drains_pending_and_finishes(self):
        receive(self.store, registration(), drain=False)
        self.queue.enqueue('job1', 'north')
        claim = self.queue.claim('worker')
        self.assertEqual(claim['job']['state'], 'running')
        result = self.queue.execute('job1', claim['token'])
        self.assertEqual(result['state'], 'complete')
        self.assertEqual(len(result['result']['advanced']), 1)
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 1)
        self.assertIsNone(self.queue.claim('another'))
        self.assertTrue(audit.verify(self.store)['ok'])

    def test_gap_waits_then_resume_after_missing_input(self):
        receive(self.store, transition(2, 2, 'suspend'), drain=False)
        self.queue.enqueue('job1', 'north', max_attempts=3)
        claim = self.queue.claim('worker')
        waiting = self.queue.execute('job1', claim['token'])
        self.assertEqual(waiting['state'], 'waiting')
        self.assertEqual(waiting['result']['blockage']['kind'], 'gap')
        receive(self.store, registration(), drain=False)
        resumed = self.queue.resume('job1', waiting['revision'])
        self.assertEqual(resumed['state'], 'queued')
        second = self.queue.claim('worker2')
        self.assertEqual(self.queue.execute('job1', second['token'])['state'], 'complete')
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 2)

    def test_expired_claim_requeues_and_rejects_stale_worker(self):
        self.queue.enqueue('job1', 'north')
        original = self.queue.claim('old', ttl=2)
        self.clock.tick(2)
        self.assertEqual(self.queue.recover_expired(), ['job1'])
        replacement = self.queue.claim('new')
        with self.assertRaises(LeaseLost):
            self.queue.execute('job1', original['token'])
        self.assertEqual(replacement['job']['attempts'], 2)
        self.assertEqual(self.queue.execute('job1', replacement['token'])['state'], 'complete')

    def test_retry_budget_and_failure_details(self):
        self.queue.enqueue('job1', 'north', max_attempts=1)
        claim = self.queue.claim('worker')
        result = self.queue.fail('job1', claim['token'], {'code': 'io_failure', 'reason': 'synthetic'})
        self.assertEqual(result['state'], 'failed')
        self.assertEqual(result['error']['code'], 'io_failure')
        self.assertIsNone(self.queue.claim('another'))
        self.assertEqual(len(self.queue.list(state='failed')), 1)
        with self.assertRaises(Conflict):
            self.queue.resume('job1', result['revision'])

    def test_priority_and_per_issuer_serialization(self):
        self.queue.enqueue('low', 'north', priority=1)
        self.queue.enqueue('high', 'north', priority=10)
        self.queue.enqueue('other', 'south', priority=2)
        first = self.queue.claim('worker1')
        second = self.queue.claim('worker2')
        self.assertEqual(first['job']['job_id'], 'high')
        self.assertEqual(second['job']['job_id'], 'other')
        self.assertIsNone(self.queue.claim('worker3'))
        self.queue.execute('high', first['token'])
        self.assertEqual(self.queue.claim('worker3')['job']['job_id'], 'low')

    def test_cancel_and_idempotent_enqueue(self):
        created = self.queue.enqueue('job', 'north')
        self.assertEqual(self.queue.enqueue('job', 'north'), created)
        with self.assertRaises(Conflict):
            self.queue.enqueue('job', 'south')
        cancelled = self.queue.cancel('job', created['revision'], 'Operator cancellation')
        self.assertEqual(cancelled['state'], 'cancelled')
        with self.assertRaises(StaleRevision):
            self.queue.cancel('job', created['revision'], 'Outdated review')
        self.assertIsNone(self.queue.claim('worker'))


class ContractTests(LedgerCase):
    def setUp(self):
        super().setUp()
        self.gate = ContractGate(self.store)
        self.config = self.gate.configure(
            'north', 0, allowed_operations=['register', 'amend', 'revoke'],
            required_evidence=['source_agreement'], allowed_regions=['CN'], max_gap=2,
        )

    def test_valid_registration_passes_contract(self):
        result = self.gate.receive(registration())
        self.assertTrue(result['accepted'])
        self.assertEqual(result['contract_revision'], 1)
        self.assertEqual(result['result']['disposition'], 'applied')
        self.assertEqual(self.gate.rejections('north'), [])

    def test_contract_reports_all_independent_reasons(self):
        value = registration(5)
        value['payload']['scope']['regions'] = ['US']
        value['payload']['evidence'][0]['kind'] = 'other_evidence'
        result = self.gate.receive(value)
        self.assertFalse(result['accepted'])
        codes = {item['code'] for item in result['reasons']}
        self.assertEqual(codes, {'sequence_gap_limit', 'evidence_missing', 'region_not_accepted'})
        self.assertEqual(self.store.scalar('SELECT COUNT(*) FROM events'), 0)
        self.assertEqual(len(self.gate.rejections('north')), 1)
        self.assertEqual(issuers.get(self.store, 'north')['last_sequence'], 0)

    def test_contract_change_uses_optimistic_revision(self):
        with self.assertRaises(StaleRevision):
            self.gate.configure('north', 0, allowed_operations=['register'], required_evidence=['source_agreement'], allowed_regions=['CN'])
        updated = self.gate.configure('north', 1, allowed_operations=['register', 'suspend'], required_evidence=['source_agreement'], allowed_regions=['CN'])
        self.assertEqual(updated['revision'], 2)
        self.gate.receive(registration())
        result = self.gate.receive(transition(2, 2, 'suspend'))
        self.assertTrue(result['accepted'])
        self.assertEqual(result['result']['disposition'], 'applied')
