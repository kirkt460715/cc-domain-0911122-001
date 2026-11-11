import copy
import csv
import io
import json

from registry_bridge.errors import Conflict, IntegrityFailure, InvalidMessage, NotFound
from registry_bridge.feeds.receiver import receive
from registry_bridge.ledger import projection
from registry_bridge.query import catalog, exporting, knowledge
from registry_bridge.reconcile import cross_registry, inspection, invariants
from tests.support import LedgerCase, registration, transition


class CatalogTests(LedgerCase):
    def test_revision_and_time_queries_are_distinct(self):
        receive(self.store, registration())
        self.clock.tick()
        receive(self.store, transition(2, 2, 'suspend'))
        old = catalog.asset(self.store, 'north', 'asset1', instant='2026-11-01T12:00:00Z')
        self.assertEqual(old['state'], 'active')
        self.assertEqual(catalog.asset(self.store, 'north', 'asset1', revision=2)['state'], 'suspended')
        with self.assertRaises(NotFound):
            catalog.asset(self.store, 'north', 'asset1', instant='2026-10-31T00:00:00Z')
        with self.assertRaises(InvalidMessage):
            catalog.asset(self.store, 'north', 'asset1', instant='2026-11-02T00:00:00Z', revision=2)

    def test_keyset_pages_have_no_repeated_asset(self):
        for seq in range(1, 6):
            receive(self.store, registration(seq, asset=f'asset{seq}'))
        first = catalog.catalog(self.store, limit=2)
        second = catalog.catalog(self.store, limit=2, cursor=first['next_cursor'])
        third = catalog.catalog(self.store, limit=2, cursor=second['next_cursor'])
        keys = [row['asset_id'] for page in (first, second, third) for row in page['items']]
        self.assertEqual(keys, ['asset1', 'asset2', 'asset3', 'asset4', 'asset5'])
        self.assertIsNone(third['next_cursor'])
        self.assertEqual(catalog.decode_cursor(first['next_cursor']), ('north', 'asset2'))
        with self.assertRaises(InvalidMessage):
            catalog.decode_cursor('%%%')

    def test_authorization_reports_every_failed_dimension(self):
        receive(self.store, registration())
        result = catalog.authorize(self.store, 'north', 'asset1', purpose='research', region='CN', right='hold', instant='2026-11-05T00:00:00Z')
        self.assertTrue(result['allowed'])
        denied = catalog.authorize(self.store, 'north', 'asset1', purpose='marketing', region='US', right='operate', instant='2028-01-01T00:00:00Z')
        self.assertFalse(denied['allowed'])
        self.assertEqual({row['code'] for row in denied['reasons']}, {'expired', 'purpose_denied', 'region_denied', 'right_denied'})
        receive(self.store, transition(2, 2, 'suspend'))
        suspended = catalog.authorize(self.store, 'north', 'asset1', purpose='research', region='CN', right='hold', instant='2026-11-05T00:00:00Z')
        self.assertEqual(suspended['reasons'], [{'code': 'inactive', 'state': 'suspended'}])

    def test_cross_registry_overlapping_claims_need_review(self):
        receive(self.store, registration())
        receive(self.store, registration(issuer='south', holder='holder2'))
        claims = cross_registry.dataset_claims(self.store, 'a' * 64)
        self.assertEqual(len(claims['claims']), 2)
        self.assertEqual(claims['review_count'], 1)
        self.assertEqual(claims['comparisons'][0]['classification'], 'overlapping_claims')
        self.assertFalse(claims['authoritative'])
        self.assertEqual(len(cross_registry.scan(self.store)['items']), 1)

    def test_disjoint_claims_and_equivalent_registrations(self):
        left = registration()
        right = registration(issuer='south', holder='holder2')
        right['payload']['scope']['regions'] = ['US']
        receive(self.store, left)
        receive(self.store, right)
        claims = cross_registry.dataset_claims(self.store, 'a' * 64)
        self.assertEqual(claims['comparisons'][0]['classification'], 'disjoint_claims')
        self.assertEqual(claims['review_count'], 0)
        new = transition(2, 2, 'amend', issuer='south', changes={'holder': 'holder1', 'scope': left['payload']['scope']})
        receive(self.store, new)
        claims = cross_registry.dataset_claims(self.store, 'a' * 64)
        self.assertEqual(claims['comparisons'][0]['classification'], 'equivalent')

    def test_knowledge_time_excludes_not_yet_applied_corrections(self):
        received = self.clock()
        receive(self.store, registration())
        self.clock.tick(60)
        receive(self.store, transition(2, 2, 'amend', changes={'title': 'New title'}))
        old = knowledge.view_at_knowledge(self.store, 'north', 'asset1', received, '2026-11-10T00:00:00Z')
        latest = knowledge.view_at_knowledge(self.store, 'north', 'asset1', self.clock(), '2026-11-10T00:00:00Z')
        self.assertEqual(old['asset']['revision'], 1)
        self.assertEqual(latest['asset']['revision'], 2)
        self.assertEqual(knowledge.changed_between(self.store, received, self.clock())['count'], 1)
        self.assertEqual(len(knowledge.observed_lifecycle(self.store, 'north', 'asset1')['events']), 2)

    def test_inspection_reports_evidence_holder_expiry_and_impact(self):
        receive(self.store, registration())
        scope = registration()['payload']['scope']
        scope['purposes'] = ['repair']
        receive(self.store, transition(2, 2, 'amend', changes={'scope': scope}))
        impact = inspection.scope_change_impact(self.store, 'north', 'asset1', 1, 2)
        self.assertEqual(impact['removed']['purposes'], ['research'])
        self.assertTrue(impact['requires_consumer_review'])
        self.assertEqual(len(inspection.evidence_index(self.store, 'b' * 64)['references']), 1)
        self.assertEqual(inspection.holder_inventory(self.store, 'holder1')['datasets'], 1)
        schedule = inspection.expiry_schedule(self.store, '2027-01-01T00:00:00Z', '2028-01-01T00:00:00Z')
        self.assertEqual(schedule['total'], 1)
        trace = inspection.event_trace(self.store, 'north:event2')
        self.assertEqual(trace['snapshot']['revision'], 2)
        self.assertGreaterEqual(len(trace['audit']), 2)
        self.assertEqual(invariants.incident_bundle(self.store, ['north:event2'])['payload']['traces'][0], trace)


class SnapshotTests(LedgerCase):
    def test_snapshot_hash_is_deterministic_and_tamper_evident(self):
        receive(self.store, registration())
        first = exporting.snapshot(self.store)
        self.assertEqual(exporting.snapshot(self.store), first)
        self.assertTrue(exporting.verify_snapshot(first)['ok'])
        tampered = copy.deepcopy(first)
        tampered['manifest']['assets'][0]['title'] = 'tampered'
        with self.assertRaises(IntegrityFailure):
            exporting.verify_snapshot(tampered)
        self.assertEqual(first['count'], 1)

    def test_snapshot_compare_explains_new_and_changed_assets(self):
        receive(self.store, registration())
        before = exporting.snapshot(self.store)
        receive(self.store, transition(2, 2, 'suspend'))
        receive(self.store, registration(3, asset='asset2'))
        after = exporting.snapshot(self.store)
        result = exporting.compare_snapshots(before, after)
        self.assertEqual(result['count'], 2)
        self.assertEqual([row['kind'] for row in result['changes']], ['changed', 'added'])
        self.assertEqual(result['changes'][0]['before']['state'], 'active')
        self.assertEqual(result['changes'][0]['after']['state'], 'suspended')

    def test_export_atomic_file_and_csv_formula_safety(self):
        value = registration()
        value['payload']['title'] = '=SUM(1,2)'
        receive(self.store, value)
        path = self.path.parent / 'snapshot.json'
        result = exporting.write_snapshot(self.store, path)
        self.assertEqual(result['count'], 1)
        loaded = exporting.read_snapshot(path)
        self.assertTrue(exporting.verify_snapshot(loaded)['ok'])
        with self.assertRaises(Conflict):
            exporting.write_snapshot(self.store, path)
        text = exporting.to_csv(loaded)
        rows = list(csv.DictReader(io.StringIO(text)))
        self.assertEqual(rows[0]['title'], "'=SUM(1,2)")
        self.assertEqual(list(self.path.parent.glob('.snapshot.json.*')), [])

    def test_snapshot_at_time_uses_historical_holder_filter(self):
        receive(self.store, registration())
        receive(self.store, transition(2, 2, 'amend', changes={'holder': 'holder2'}))
        old = catalog.catalog(self.store, holder='holder1', instant='2026-11-01T12:00:00Z')
        current = catalog.catalog(self.store, holder='holder1')
        self.assertEqual(old['count'], 1)
        self.assertEqual(current['count'], 0)
        snapshot = exporting.snapshot(self.store, instant='2026-11-01T12:00:00Z')
        self.assertEqual(snapshot['manifest']['assets'][0]['holder'], 'holder1')
        self.assertEqual(snapshot['manifest']['assets'][0]['revision'], 1)
