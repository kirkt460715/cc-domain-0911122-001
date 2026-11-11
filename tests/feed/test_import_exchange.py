import copy
import json
from pathlib import Path

from registry_bridge.errors import Conflict, IntegrityFailure, InvalidMessage, NotFound
from registry_bridge.feeds import bundle, importer
from registry_bridge.feeds.message import canonical, digest, parse_message
from registry_bridge.feeds.adapters import encode_csv
from registry_bridge.feeds.receiver import receive
from registry_bridge.ledger import audit, issuers, projection
from registry_bridge.ledger.storage import Store
from tests.support import LedgerCase, registration, transition


class ImportTests(LedgerCase):
    def test_partial_import_records_dead_letters(self):
        data = (canonical(registration()) + '\n{broken\n' + canonical(registration(2, asset='second')) + '\n').encode()
        result = importer.import_bytes(self.store, data, 'feed-one')
        self.assertEqual(result['state'], 'partial')
        self.assertEqual(result['total'], 3)
        self.assertEqual(result['accepted'], 2)
        self.assertEqual(result['rejected'], 1)
        dead = importer.list_dead_letters(self.store)
        self.assertEqual(len(dead), 1)
        self.assertEqual(dead[0]['line_number'], 2)
        self.assertEqual(issuers.progress(self.store, 'north')['last_sequence'], 2)

    def test_atomic_import_rolls_back_prior_rows_and_audit(self):
        data = (canonical(registration()) + '\ninvalid\n').encode()
        before = audit.verify(self.store)
        with self.assertRaises(InvalidMessage):
            importer.import_bytes(self.store, data, 'atomic-feed', atomic=True)
        self.assertEqual(self.store.scalar('SELECT COUNT(*) FROM events'), 0)
        self.assertEqual(self.store.scalar('SELECT COUNT(*) FROM imports'), 0)
        self.assertEqual(importer.list_dead_letters(self.store), [])
        self.assertEqual(audit.verify(self.store), before)

    def test_identical_file_import_reuses_receipt(self):
        data = (canonical(registration()) + '\n').encode()
        first = importer.import_bytes(self.store, data, 'repeat')
        second = importer.import_bytes(self.store, data, 'repeat')
        self.assertEqual(first['import_id'], second['import_id'])
        self.assertTrue(second['reused'])
        self.assertEqual(len(importer.import_history(self.store)), 1)
        third = importer.import_bytes(self.store, data, 'another-source')
        self.assertEqual(third['duplicate'], 1)
        self.assertEqual(third['accepted'], 0)

    def test_deadletter_replay_is_traceable_and_idempotent(self):
        importer.import_bytes(self.store, b'bad\n', 'broken-source')
        dead = importer.list_dead_letters(self.store)[0]
        result = importer.replay_dead_letter(self.store, dead['dead_id'], registration())
        self.assertEqual(result['status'], 'accepted')
        self.assertEqual(importer.list_dead_letters(self.store), [])
        replayed = importer.list_dead_letters(self.store, unreplayed=False)
        self.assertEqual(replayed[0]['replayed_as'], 'north:event1')
        repeated = importer.replay_dead_letter(self.store, dead['dead_id'], registration())
        self.assertEqual(repeated['status'], 'already_replayed')
        with self.assertRaises(Conflict):
            importer.replay_dead_letter(self.store, dead['dead_id'], registration(2, asset='other'))
        self.assertTrue(audit.verify(self.store)['ok'])

    def test_unknown_issuer_is_deadletter_not_implicit_registration(self):
        data = (canonical(registration(issuer='unknown')) + '\n').encode()
        result = importer.import_bytes(self.store, data, 'unknown-feed')
        self.assertEqual(result['rejected'], 1)
        self.assertIsNone(issuers.get(self.store, 'unknown', required=False))
        self.assertEqual(importer.list_dead_letters(self.store)[0]['error']['code'], 'not_found')
        self.assertEqual(self.store.scalar('SELECT COUNT(*) FROM events'), 0)

    def test_import_conflict_counts_and_atomic_rollback(self):
        receive(self.store, registration())
        altered = registration()
        altered['payload']['holder'] = 'someone-else'
        data = (canonical(altered) + '\n').encode()
        partial = importer.import_bytes(self.store, data, 'conflict-feed')
        self.assertEqual(partial['conflicts'], 1)
        self.assertEqual(partial['state'], 'partial')
        with self.assertRaises(Conflict):
            importer.import_bytes(self.store, data, 'atomic-conflict', atomic=True)
        self.assertEqual(len(importer.import_history(self.store)), 1)
        self.assertEqual(projection.current(self.store, 'north', 'asset1')['holder'], 'holder1')

    def test_csv_import_and_file_size_boundary(self):
        content = encode_csv([parse_message(registration())]).encode()
        path = self.path.parent / 'feed.csv'
        path.write_bytes(content)
        result = importer.import_file(self.store, path, 'csv-source', format='csv')
        self.assertEqual(result['accepted'], 1)
        self.assertEqual(result['state'], 'complete')
        with self.assertRaises(InvalidMessage):
            importer.inspect_file(path, maximum=len(content) - 1)
        raw, sha = importer.inspect_file(path, maximum=len(content))
        self.assertEqual(raw, content)
        self.assertEqual(len(sha), 64)

    def test_invalid_csv_fails_without_partial_receipt(self):
        with self.assertRaises(InvalidMessage):
            importer.import_bytes(self.store, b'wrong,header\n1,2\n', 'bad-csv', format='csv')
        self.assertEqual(self.store.scalar('SELECT COUNT(*) FROM imports'), 0)
        with self.assertRaises(InvalidMessage):
            importer.import_bytes(self.store, b'\xff', 'bad-encoding', format='csv')
        self.assertEqual(self.store.scalar('SELECT COUNT(*) FROM imports'), 0)


class BundleTests(LedgerCase):
    def test_bundle_import_into_independent_store(self):
        receive(self.store, registration())
        receive(self.store, transition(2, 2, 'suspend'))
        package = bundle.create_bundle(self.store, 'north', 1, 2)
        self.assertEqual(len(bundle.verify_bundle(package)), 2)
        with Store(':memory:') as target:
            result = bundle.import_bundle(target, package, allow_register=True)
            self.assertEqual(result['accepted'], 2)
            self.assertEqual(projection.current(target, 'north', 'asset1')['state'], 'suspended')
            repeated = bundle.import_bundle(target, package)
            self.assertEqual(repeated['duplicate'], 2)
            self.assertEqual(bundle.stream_summary(target, 'north'), bundle.stream_summary(self.store, 'north'))

    def test_bundle_requires_explicit_issuer_registration(self):
        receive(self.store, registration())
        package = bundle.create_bundle(self.store, 'north', 1, 1)
        with Store(':memory:') as target:
            with self.assertRaises(Conflict):
                bundle.import_bundle(target, package)
            self.assertEqual(target.scalar('SELECT COUNT(*) FROM issuers'), 0)
            self.assertEqual(target.scalar('SELECT COUNT(*) FROM events'), 0)

    def test_bundle_content_tampering_detected(self):
        receive(self.store, registration())
        package = bundle.create_bundle(self.store, 'north', 1, 1)
        package['body']['messages'][0]['payload']['title'] = 'tampered'
        with self.assertRaises(IntegrityFailure):
            bundle.verify_bundle(package)
        package['sha256'] = digest(package['body'])
        self.assertEqual(bundle.verify_bundle(package)[0].payload['title'], 'tampered')
        self.assertNotEqual(projection.current(self.store, 'north', 'asset1')['title'], 'tampered')

    def test_bundle_sequence_checks_survive_recomputed_digest(self):
        receive(self.store, registration())
        package = bundle.create_bundle(self.store, 'north', 1, 1)
        package['body']['messages'][0]['sequence'] = 2
        package['sha256'] = digest(package['body'])
        with self.assertRaises(IntegrityFailure):
            bundle.verify_bundle(package)
        package['body']['messages'][0]['sequence'] = 1
        package['body']['messages'][0]['issuer'] = 'south'
        package['sha256'] = digest(package['body'])
        with self.assertRaises(IntegrityFailure):
            bundle.verify_bundle(package)

    def test_bundle_conflict_rolls_back_all_imported_messages(self):
        receive(self.store, registration())
        receive(self.store, registration(2, asset='second'))
        package = bundle.create_bundle(self.store, 'north', 1, 2)
        with Store(':memory:') as target:
            issuers.add(target, 'north', 'Synthetic North')
            conflicting = registration(2, asset='other')
            receive(target, conflicting, drain=False)
            before = audit.verify(target)
            with self.assertRaises(Conflict):
                bundle.import_bundle(target, package)
            self.assertEqual(target.scalar('SELECT COUNT(*) FROM events'), 1)
            self.assertEqual(audit.verify(target), before)
            self.assertIsNone(projection.current(target, 'north', 'asset1'))

    def test_gapped_bundle_cannot_be_created(self):
        receive(self.store, registration(3, asset='third'))
        with self.assertRaises(Conflict):
            bundle.create_bundle(self.store, 'north', 1, 3)
        self.assertEqual(bundle.stream_summary(self.store, 'north')['event_count'], 1)
        self.assertEqual(bundle.stream_summary(self.store, 'north')['last_sequence'], 3)

    def test_segment_comparison_identifies_different_range(self):
        for sequence in range(1, 6):
            receive(self.store, registration(sequence, asset=f'asset{sequence}'))
        segments = bundle.segments(self.store, 'north', width=2)
        self.assertEqual([(x['lower'], x['upper']) for x in segments], [(1, 2), (3, 4), (5, 5)])
        self.assertEqual(bundle.differing_ranges(self.store, 'north', segments), [])
        segments[1]['sha256'] = '0' * 64
        differences = bundle.differing_ranges(self.store, 'north', segments)
        self.assertEqual(len(differences), 1)
        self.assertEqual(differences[0]['lower'], 3)
        self.assertEqual(differences[0]['local_count'], 2)
        summary = bundle.stream_summary(self.store, 'north')
        self.assertTrue(bundle.verify_remote_summary(self.store, 'north', summary)['matches'])
