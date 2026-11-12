from registry_bridge.errors import Conflict
from registry_bridge.feeds.bundle import import_bundle
from registry_bridge.feeds.message import parse_message
from registry_bridge.feeds.receiver import receive
from registry_bridge.ledger import audit, issuers
from registry_bridge.reconcile.backfill import BackfillManager
from registry_bridge.reconcile.exchange import ExchangeClient, ViewExpired
from registry_bridge.reconcile.resolution import open_conflicts
from lab.demo import request
from lab.http import serving
from lab.provider import Provider
from tests.support import LedgerCase, registration


def remote_bundle(base, lower, upper):
    return request(
        base,
        f'/bundle?epoch=exchange-1&revision=1&lower={lower}&upper={upper}',
    )


class BackfillTests(LedgerCase):
    def setUp(self):
        super().setUp()
        # The lab exchange signs its north bundles with this display name;
        # bundle import requires the local issuer description to agree.
        self.store.execute(
            "UPDATE issuers SET display_name=? WHERE issuer='north'", ('合成北方',),
        )
        self.provider = Provider()
        self.manager = BackfillManager(self.store)

    def test_full_backfill_uses_frozen_view_and_sparse_requests(self):
        with serving(self.provider) as base:
            plan = self.manager.plan('north', base, run_id='run-1', segment_width=8)
            self.assertEqual(plan['state'], 'planned')
            self.assertEqual(plan['view'], {
                'epoch': 'exchange-1', 'revision': 1, 'ceiling': 32, 'frozen': True,
            })
            self.assertEqual(plan['requests_used'], 1)  # the /view freeze only
            status = self.manager.execute('run-1')
        self.assertEqual(status['state'], 'complete')
        # one /view + four segment bundles, never 32 per-event downloads
        self.assertEqual(status['requests_used'], 5)
        self.assertEqual(status['range_counts']['backfilled'], 4)
        self.assertEqual(status['sequences']['backfilled'], 32)
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 32)
        self.assertEqual(status['ledger']['blockage']['kind'], 'caught_up')
        self.assertEqual(self.store.scalar('SELECT COUNT(*) FROM events'), 32)
        self.assertTrue(audit.verify(self.store)['ok'])

    def test_matching_segments_are_never_downloaded_again(self):
        with serving(self.provider) as base:
            self.manager.plan('north', base, run_id='first', segment_width=8)
            self.manager.execute('first')
            calls_before = len(self.provider.calls)
            second = self.manager.plan('north', base, run_id='second', segment_width=8)
            status = self.manager.execute('second')
        bundle_calls = [c for c in self.provider.calls[calls_before:] if c['path'] == '/bundle']
        self.assertEqual(bundle_calls, [])
        self.assertEqual(status['range_counts']['matched'], 4)
        self.assertEqual(status['sequences']['matched'], 32)
        # /view plus four summary probes only
        self.assertEqual(status['requests_used'] - second['requests_used'], 4)

    def test_sparse_gaps_fill_only_missing_segments(self):
        with serving(self.provider) as base:
            import_bundle(self.store, remote_bundle(base, 1, 8))
            import_bundle(self.store, remote_bundle(base, 25, 32))
            plan = self.manager.plan('north', base, run_id='sparse', segment_width=8)
            status = self.manager.execute('sparse')
        self.assertEqual(status['state'], 'complete')
        self.assertEqual(status['range_counts']['matched'], 2)
        self.assertEqual(status['range_counts']['backfilled'], 2)
        self.assertEqual(status['sequences']['matched'], 16)
        self.assertEqual(status['sequences']['backfilled'], 16)
        self.assertEqual(status['sequences']['unfinished'], 0)
        self.assertEqual(self.store.scalar('SELECT COUNT(*) FROM events'), 32)
        self.assertEqual(status['ledger']['gaps'], [])

    def test_divergent_single_event_is_narrowed_without_whole_rewrite(self):
        # Local sequence 12 holds a different immutable event under the same id.
        receive(self.store, registration(12, asset='local-only-asset'))
        with serving(self.provider) as base:
            status = self.manager.execute(
                self.manager.plan('north', base, run_id='diverge', segment_width=8)['run_id'],
            )
        self.assertEqual(status['state'], 'complete')
        conflicted = [r for r in status['ranges'] if r['state'] == 'conflicted']
        self.assertEqual([(r['lower'], r['upper']) for r in conflicted], [(12, 12)])
        self.assertEqual(status['sequences']['conflicted'], 1)
        self.assertEqual(status['sequences']['backfilled'], 31)
        detail = conflicted[0]['detail']
        self.assertEqual(detail['conflicts'][0]['sequence'], 12)
        self.assertNotEqual(detail['local_digest'], detail['remote_digest'])
        # the local immutable event survives untouched
        local = self.store.one(
            "SELECT event_id,asset_id FROM events WHERE issuer='north' AND sequence=12",
        )
        self.assertEqual(local['event_id'], 'north:event12')
        self.assertEqual(local['asset_id'], 'local-only-asset')
        self.assertEqual([c['kind'] for c in open_conflicts(self.store)], ['event_identity'])
        # the conflicting range is terminal evidence, never a repaired range
        terminal = {r['state'] for r in status['ranges']}
        self.assertIn('conflicted', terminal)
        self.assertNotEqual(status['range_counts']['backfilled'] +
                            status['range_counts']['matched'], 32)
        self.assertTrue(audit.verify(self.store)['ok'])

    def test_conflicted_range_blocks_overwrite_but_other_ranges_continue(self):
        receive(self.store, registration(20, asset='local-twenty'))
        with serving(self.provider) as base:
            status = self.manager.execute(
                self.manager.plan('north', base, run_id='mixed', segment_width=4)['run_id'],
            )
        conflicted = [r for r in status['ranges'] if r['state'] == 'conflicted']
        self.assertEqual([(r['lower'], r['upper']) for r in conflicted], [(20, 20)])
        # ranges before and after the conflict were still repaired
        self.assertGreaterEqual(status['range_counts']['backfilled'], 1)
        self.assertEqual(status['range_counts']['conflicted'], 1)
        remote_twenty = parse_message(self.provider.messages[19])
        stored = self.store.one(
            "SELECT fingerprint FROM events WHERE issuer='north' AND sequence=20",
        )
        self.assertNotEqual(stored['fingerprint'], remote_twenty.fingerprint)

    def test_view_expiry_leaves_unfinished_work_and_requires_new_plan(self):
        with serving(self.provider) as base:
            plan = self.manager.plan('north', base, run_id='old-view', segment_width=8)
            self.assertEqual(plan['view']['revision'], 1)
            request(base, '/control', {'expire': True})
            status = self.manager.execute('old-view')
            self.assertEqual(status['state'], 'expired')
            self.assertEqual(status['pause_reason'], 'view_expired')
            self.assertEqual(status['sequences']['unfinished'], 32)
            # retrying the stale run must not adopt the new revision
            again = self.manager.execute('old-view')
            self.assertEqual(again['state'], 'expired')
            with self.assertRaises(Conflict):
                self.manager.extend_budget('old-view', 999)
            # the old work list remains on record as evidence
            self.assertGreaterEqual(status['range_counts']['pending'], 3)
            fresh = self.manager.plan('north', base, run_id='new-view', segment_width=8)
            self.assertEqual(fresh['view']['revision'], 2)
            finished = self.manager.execute('new-view')
            self.assertEqual(finished['state'], 'complete')
            self.assertEqual(finished['sequences']['backfilled'], 32)

    def test_budget_pause_persists_and_resumes_without_reapplying_bundles(self):
        with serving(self.provider) as base:
            self.manager.plan('north', base, run_id='budgeted', segment_width=8, request_budget=3)
            paused = self.manager.execute('budgeted')
            self.assertEqual(paused['state'], 'paused')
            self.assertEqual(paused['pause_reason'], 'request_budget_exhausted')
            self.assertEqual(paused['requests_used'], 3)
            with self.assertRaises(Conflict):
                self.manager.extend_budget('budgeted', 2)
            self.manager.extend_budget('budgeted', 100)
            finished = self.manager.execute('budgeted')
        self.assertEqual(finished['state'], 'complete')
        self.assertEqual(finished['requests_used'], 5)
        self.assertEqual(self.store.scalar('SELECT COUNT(*) FROM backfill_bundles'), 4)
        self.assertEqual(self.store.scalar(
            "SELECT SUM(accepted) FROM backfill_bundles WHERE run_id='budgeted'"), 32)

    def test_restart_resumes_persisted_work_list_and_skips_imported_bundles(self):
        import tempfile
        from pathlib import Path
        from registry_bridge.ledger.storage import Store
        path = Path(tempfile.mkdtemp()) / 'resume.db'
        with serving(self.provider) as base:
            with Store(path) as first:
                issuers.add(first, 'north', '合成北方')
                manager = BackfillManager(first)
                manager.plan('north', base, run_id='restart', segment_width=8, request_budget=3)
                manager.execute('restart')
            with Store(path) as second:
                manager = BackfillManager(second)
                manager.extend_budget('restart', 100)
                status = manager.execute('restart')
                self.assertEqual(status['state'], 'complete')
                self.assertEqual(status['requests_used'], 5)
                self.assertEqual(issuers.progress(second, 'north')['last_sequence'], 32)

    def test_partial_segment_is_narrowed_instead_of_bulk_downloaded(self):
        # A non-empty differing segment cannot use the bulk-empty shortcut;
        # it must be narrowed until leaves identify the exact missing events.
        with serving(self.provider) as base:
            # local holds only sequence 3 inside segment [1,8]
            import_bundle(self.store, request(
                base, '/bundle?epoch=exchange-1&revision=1&lower=3&upper=3'))
            status = self.manager.execute(
                self.manager.plan('north', base, run_id='partial', segment_width=8)['run_id'],
            )
        self.assertEqual(status['state'], 'complete')
        # The populated [1,8] segment itself must not use the bulk-empty
        # shortcut; only fully empty child ranges may.
        populated_segment = next(r for r in status['ranges'] if r['lower'] == 1 and r['upper'] == 8)
        self.assertEqual(populated_segment['state'], 'split')
        self.assertNotEqual(populated_segment.get('detail', {}).get('mode'), 'bulk_empty')
        # sequence 3 was already present and matched, the other leaves were fetched
        self.assertEqual(self.store.scalar('SELECT COUNT(*) FROM events'), 32)
        self.assertEqual(status['sequences']['unfinished'], 0)
        # the split parents remain on record as evidence of the narrowing
        self.assertGreaterEqual(status['range_counts']['split'], 1)

    def test_duplicate_delivery_is_idempotent_not_a_conflict(self):
        with serving(self.provider) as base:
            self.manager.plan('north', base, run_id='one', segment_width=16)
            self.manager.execute('one')
            second = self.manager.plan('north', base, run_id='two', segment_width=16)
            status = self.manager.execute('two')
        self.assertEqual(status['state'], 'complete')
        self.assertEqual(status['range_counts']['matched'], 2)
        self.assertEqual(open_conflicts(self.store), [])
        self.assertEqual(self.store.scalar('SELECT COUNT(*) FROM events'), 32)

    def test_plan_rejects_mismatched_issuer_and_unknown_local_issuer(self):
        from registry_bridge.errors import NotFound
        with serving(self.provider) as base:
            with self.assertRaises(Conflict):
                self.manager.plan('south', base, run_id='wrong-issuer')
        with serving(Provider()) as base:
            with self.assertRaises(NotFound):
                BackfillManager(self.store).plan('unregistered', base, run_id='no-issuer')

    def test_list_and_status_report_evidence(self):
        with serving(self.provider) as base:
            self.manager.plan('north', base, run_id='listed', segment_width=8)
            self.manager.execute('listed')
        runs = self.manager.list_runs()
        self.assertEqual([r['run_id'] for r in runs], ['listed'])
        status = self.manager.status('listed')
        self.assertEqual(status['request_budget'], 100)
        self.assertEqual(status['range_counts']['backfilled'], 4)
        self.assertTrue(all('state' in r for r in status['ranges']))


class ExchangeClientTests(LedgerCase):
    def test_frozen_view_410_surfaces_as_view_expired(self):
        with serving(Provider()) as base:
            client = ExchangeClient(base)
            client.view()
            request(base, '/control', {'expire': True})
            with self.assertRaises(ViewExpired):
                client.summary('exchange-1', 1, 1, 4)
            self.assertEqual(client.requests_used, 2)

    def test_unreachable_endpoint_is_classified(self):
        from registry_bridge.reconcile.exchange import ExchangeUnavailable
        client = ExchangeClient('http://127.0.0.1:1', timeout=0.2)
        with self.assertRaises(ExchangeUnavailable):
            client.view()
