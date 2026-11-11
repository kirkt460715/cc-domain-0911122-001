import copy
import sqlite3

from registry_bridge.errors import Conflict, InvalidTransition, NotFound, StaleRevision, StreamBlocked
from registry_bridge.feeds.receiver import receive, drain_stream, event, pending
from registry_bridge.ledger import audit, issuers, projection
from registry_bridge.query import catalog, consumer
from registry_bridge.reconcile import resolution, rebuild, invariants
from tests.support import LedgerCase, registration, transition


class LifecycleTests(LedgerCase):
    def test_register_suspend_restore_amend_revoke(self):
        receive(self.store, registration())
        receive(self.store, transition(2, 2, 'suspend'))
        self.assertEqual(projection.current(self.store, 'north', 'asset1')['state'], 'suspended')
        receive(self.store, transition(3, 3, 'restore'))
        self.assertEqual(projection.current(self.store, 'north', 'asset1')['state'], 'active')
        receive(self.store, transition(4, 4, 'amend', changes={'holder': 'holder2'}))
        receive(self.store, transition(5, 5, 'revoke'))
        asset = projection.current(self.store, 'north', 'asset1')
        self.assertEqual(asset['revision'], 5)
        self.assertEqual(asset['holder'], 'holder2')
        self.assertEqual(asset['state'], 'revoked')
        self.assertEqual(len(projection.history(self.store, 'north', 'asset1')), 5)
        self.assertTrue(rebuild.check(self.store)['ok'])
        self.assertTrue(invariants.inspect_all(self.store)['ok'])

    def test_late_sequence_is_buffered_then_drained(self):
        later = receive(self.store, transition(2, 2, 'suspend'))
        self.assertEqual(later['disposition'], 'pending')
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 0)
        self.assertEqual(resolution.gaps(self.store, 'north')['gaps'], [{'from': 1, 'to': 1, 'count': 1}])
        first = receive(self.store, registration())
        self.assertEqual(len(first['advanced']), 2)
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 2)
        self.assertEqual(projection.current(self.store, 'north', 'asset1')['state'], 'suspended')
        self.assertEqual(consumer.stable_ceiling(self.store), 2)

    def test_duplicate_does_not_append_audit_or_projection(self):
        first = receive(self.store, registration())
        count = audit.verify(self.store)['entries']
        second = receive(self.store, registration())
        self.assertEqual(second['status'], 'duplicate')
        self.assertEqual(second['position'], first['position'])
        self.assertEqual(audit.verify(self.store)['entries'], count)
        self.assertEqual(len(projection.history(self.store, 'north', 'asset1')), 1)

    def test_same_event_with_different_content_is_quarantined(self):
        receive(self.store, registration())
        altered = registration()
        altered['payload']['title'] = 'Conflicting title'
        result = receive(self.store, altered)
        self.assertEqual(result['status'], 'conflict')
        conflict = resolution.conflict(self.store, result['conflict_id'])
        self.assertEqual(conflict['kind'], 'event_identity')
        self.assertEqual(conflict['status'], 'open')
        self.assertNotEqual(projection.current(self.store, 'north', 'asset1')['title'], 'Conflicting title')
        repeated = receive(self.store, altered)
        self.assertEqual(repeated['conflict_id'], result['conflict_id'])
        self.assertEqual(len(resolution.open_conflicts(self.store)), 1)

    def test_same_sequence_different_event_is_not_overwritten(self):
        receive(self.store, registration())
        alternative = registration(event='alternative')
        result = receive(self.store, alternative)
        self.assertEqual(result['status'], 'conflict')
        self.assertIsNone(event(self.store, 'alternative'))
        self.assertEqual(event(self.store, 'north:event1')['sequence'], 1)
        self.assertEqual(resolution.conflict(self.store, result['conflict_id'])['kind'], 'sequence')

    def test_revision_rejection_blocks_until_deliberate_skip(self):
        receive(self.store, registration())
        rejected = receive(self.store, transition(2, 4, 'amend'))
        self.assertEqual(rejected['disposition'], 'rejected')
        receive(self.store, transition(3, 2, 'suspend'))
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 1)
        self.assertEqual(resolution.explain_blockage(self.store, 'north')['kind'], 'rejected')
        result = resolution.skip_rejected(self.store, 'north:event2', 'Issuer confirms malformed revision')
        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 3)
        self.assertEqual(projection.current(self.store, 'north', 'asset1')['revision'], 2)
        self.assertEqual(event(self.store, 'north:event2')['disposition'], 'skipped')
        self.assertTrue(invariants.inspect_all(self.store)['ok'])

    def test_skip_is_idempotent_only_for_same_decision(self):
        receive(self.store, transition(1, 2, 'revoke'))
        first = resolution.skip_rejected(self.store, 'north:event1', 'Invalid predecessor')
        second = resolution.skip_rejected(self.store, 'north:event1', 'Invalid predecessor')
        self.assertEqual(first['status'], 'skipped')
        self.assertEqual(second['status'], 'already_skipped')
        with self.assertRaises(Conflict):
            resolution.skip_rejected(self.store, 'north:event1', 'Different reason')
        with self.assertRaises(NotFound):
            resolution.skip_rejected(self.store, 'missing', 'Unknown event')

    def test_revocation_is_terminal_and_backwards_time_rejected(self):
        receive(self.store, registration())
        receive(self.store, transition(2, 2, 'revoke'))
        result = receive(self.store, transition(3, 3, 'restore'))
        self.assertEqual(result['disposition'], 'rejected')
        self.assertEqual(projection.current(self.store, 'north', 'asset1')['state'], 'revoked')
        south = registration(issuer='south')
        receive(self.store, south)
        backward = transition(2, 2, 'amend', issuer='south')
        backward['effective_at'] = '2026-10-31T23:59:59Z'
        outcome = receive(self.store, backward)
        self.assertEqual(outcome['disposition'], 'rejected')
        self.assertEqual(projection.current(self.store, 'south', 'asset1')['revision'], 1)

    def test_double_suspend_rejected_without_extra_history(self):
        receive(self.store, registration())
        receive(self.store, transition(2, 2, 'suspend'))
        outcome = receive(self.store, transition(3, 3, 'suspend'))
        self.assertEqual(outcome['disposition'], 'rejected')
        self.assertEqual(len(projection.history(self.store, 'north', 'asset1')), 2)
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 2)
        self.assertEqual(len(pending(self.store, 'north')), 1)

    def test_limited_drain_retains_recoverable_pending(self):
        for sequence in range(1, 5):
            receive(self.store, registration(sequence, asset=f'asset{sequence}'), drain=False)
        result = drain_stream(self.store, 'north', limit=2)
        self.assertEqual(len(result), 2)
        self.assertEqual(issuers.progress(self.store, 'north')['pending'], 2)
        self.assertEqual(consumer.stable_ceiling(self.store), 2)
        self.assertEqual(len(drain_stream(self.store, 'north')), 2)
        self.assertEqual(consumer.stable_ceiling(self.store), 4)

    def test_disabled_issuer_preserves_state(self):
        config = issuers.get(self.store, 'north')
        disabled = issuers.set_enabled(self.store, 'north', False, config['revision'])
        with self.assertRaises(StreamBlocked):
            receive(self.store, registration())
        self.assertEqual(self.store.scalar('SELECT COUNT(*) FROM events'), 0)
        with self.assertRaises(StaleRevision):
            issuers.set_enabled(self.store, 'north', True, config['revision'])
        issuers.set_enabled(self.store, 'north', True, disabled['revision'])
        self.assertEqual(receive(self.store, registration())['disposition'], 'applied')

    def test_same_asset_key_is_namespaced_by_issuer(self):
        receive(self.store, registration())
        receive(self.store, registration(issuer='south', holder='holder2'))
        self.assertEqual(projection.current(self.store, 'north', 'asset1')['holder'], 'holder1')
        self.assertEqual(projection.current(self.store, 'south', 'asset1')['holder'], 'holder2')
        self.assertEqual(catalog.metrics(self.store)['assets'], 2)
        self.assertTrue(rebuild.check(self.store)['ok'])

    def test_equal_effective_times_choose_highest_revision(self):
        receive(self.store, registration())
        change = transition(2, 2, 'suspend')
        change['effective_at'] = registration()['effective_at']
        receive(self.store, change)
        at = catalog.asset(self.store, 'north', 'asset1', instant='2026-11-01T00:00:00Z')
        self.assertEqual(at['revision'], 2)
        self.assertEqual(at['state'], 'suspended')
        self.assertEqual(catalog.timeline(self.store, 'north', 'asset1')[0]['valid_until'], at['effective_at'])

    def test_nested_rollback_keeps_outer_transaction_usable(self):
        with self.store.transaction():
            receive(self.store, registration())
            try:
                with self.store.transaction():
                    receive(self.store, registration(2, asset='asset2'))
                    raise RuntimeError('injected crash')
            except RuntimeError:
                pass
            receive(self.store, transition(2, 2, 'suspend'))
        self.assertIsNone(projection.current(self.store, 'north', 'asset2'))
        self.assertEqual(projection.current(self.store, 'north', 'asset1')['state'], 'suspended')
        self.assertTrue(rebuild.check(self.store)['ok'])

    def test_outer_rollback_removes_events_and_audit(self):
        original = audit.verify(self.store)
        with self.assertRaises(RuntimeError):
            with self.store.transaction():
                receive(self.store, registration())
                raise RuntimeError('interrupted transaction')
        self.assertEqual(self.store.scalar('SELECT COUNT(*) FROM events'), 0)
        self.assertEqual(audit.verify(self.store), original)
        self.assertEqual(issuers.get(self.store, 'north')['last_sequence'], 0)

    def test_projection_rebuild_repairs_derived_damage(self):
        receive(self.store, registration())
        receive(self.store, transition(2, 2, 'amend'))
        expected = rebuild.fingerprint(self.store)
        self.store.execute("UPDATE assets SET title='damaged'")
        self.assertFalse(rebuild.check(self.store)['ok'])
        result = rebuild.rebuild(self.store)
        self.assertEqual(result['repaired'], 1)
        self.assertEqual(rebuild.fingerprint(self.store), expected)
        self.assertTrue(rebuild.check(self.store)['ok'])
        self.assertTrue(audit.verify(self.store)['ok'])

    def test_audit_tampering_blocks_rebuild(self):
        from registry_bridge.errors import IntegrityFailure
        receive(self.store, registration())
        self.store.execute("UPDATE audit SET details='{}' WHERE audit_id=1")
        self.assertFalse(audit.verify(self.store)['ok'])
        with self.assertRaises(IntegrityFailure):
            rebuild.rebuild(self.store)
        self.assertEqual(projection.current(self.store, 'north', 'asset1')['revision'], 1)

    def test_backup_reopens_with_equivalent_state(self):
        from registry_bridge.ledger.storage import Store
        receive(self.store, registration())
        destination = self.path.parent / 'backup.db'
        result = self.store.backup(destination)
        self.assertGreater(result['bytes'], 0)
        with Store(destination) as backup:
            self.assertEqual(rebuild.fingerprint(backup), rebuild.fingerprint(self.store))
            self.assertTrue(backup.integrity()['ok'])
        with self.assertRaises(Exception):
            self.store.backup(destination)
