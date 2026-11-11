import copy
import io
import json
import unittest

from registry_bridge.errors import InvalidMessage
from registry_bridge.feeds.message import (
    canonical, decode_json, digest, identifier, integer, parse_message,
    string_set, text, timestamp, validate_evidence,
)
from registry_bridge.feeds.adapters import adapt_envelope, csv_records, encode_csv, ndjson_records
from tests.support import registration, transition


class MessageProtocolTests(unittest.TestCase):
    def test_registration_canonicalizes_scope_and_timezone(self):
        value = registration()
        value['effective_at'] = '2026-11-01T08:00:00+08:00'
        value['payload']['scope']['purposes'] = ['research', 'repair']
        parsed = parse_message(value)
        self.assertEqual(parsed.effective_at, '2026-11-01T00:00:00.000000Z')
        self.assertEqual(parsed.payload['scope']['purposes'], ['repair', 'research'])
        self.assertEqual(parsed.revision, 1)
        self.assertEqual(parsed.fingerprint, digest(parsed.as_dict()))

    def test_payload_is_not_mutable_through_property(self):
        parsed = parse_message(registration())
        detached = parsed.payload
        detached['title'] = 'bad mutation'
        self.assertNotEqual(parsed.payload['title'], detached['title'])
        self.assertEqual(parse_message(parsed.as_dict()), parsed)

    def test_required_and_unknown_fields(self):
        missing = registration()
        del missing['issuer']
        with self.assertRaises(InvalidMessage) as error:
            parse_message(missing)
        self.assertIn('issuer', error.exception.context['missing'])
        unknown = registration()
        unknown['owner'] = 'misleading'
        with self.assertRaises(InvalidMessage):
            parse_message(unknown)
        self.assertEqual(registration()['issuer'], 'north')

    def test_boolean_sequence_and_unsupported_version(self):
        message = registration()
        message['sequence'] = True
        with self.assertRaises(InvalidMessage):
            parse_message(message)
        message['sequence'] = 1
        message['version'] = 2
        with self.assertRaises(InvalidMessage):
            parse_message(message)
        self.assertEqual(integer(1, 'value'), 1)

    def test_registration_and_later_revision_constraints(self):
        message = registration()
        message['revision'] = 2
        with self.assertRaises(InvalidMessage):
            parse_message(message)
        message = transition(2, 1, 'revoke')
        with self.assertRaises(InvalidMessage):
            parse_message(message)
        valid = parse_message(transition(2, 2, 'revoke'))
        self.assertEqual(valid.operation, 'revoke')

    def test_scope_expiry_is_strict(self):
        message = registration()
        message['payload']['scope']['expires_at'] = message['effective_at']
        with self.assertRaises(InvalidMessage):
            parse_message(message)
        message['payload']['scope']['expires_at'] = '2026-11-01T00:00:00.000001Z'
        parsed = parse_message(message)
        self.assertGreater(parsed.payload['scope']['expires_at'], parsed.effective_at)

    def test_amendment_requires_nonempty_known_fields(self):
        message = transition(2, 2, 'amend')
        message['payload']['changes'] = {}
        with self.assertRaises(InvalidMessage):
            parse_message(message)
        message['payload']['changes'] = {'irrelevant': 'x'}
        with self.assertRaises(InvalidMessage):
            parse_message(message)
        message['payload']['changes'] = {'holder': 'new-owner'}
        self.assertEqual(parse_message(message).payload['changes']['holder'], 'new-owner')

    def test_evidence_duplicates_and_digest_shape(self):
        evidence = registration()['payload']['evidence']
        with self.assertRaises(InvalidMessage):
            validate_evidence(evidence * 2)
        with self.assertRaises(InvalidMessage):
            validate_evidence([{'kind': 'source', 'sha256': 'A' * 64}])
        self.assertEqual(validate_evidence(evidence), evidence)
        self.assertEqual(len(validate_evidence(evidence)), 1)

    def test_unicode_normalization_and_controls(self):
        self.assertEqual(text('  e\u0301  ', 'name'), 'é')
        with self.assertRaises(InvalidMessage):
            text('name\x00secret', 'name')
        with self.assertRaises(InvalidMessage):
            text('   ', 'name')
        self.assertEqual(text('中文素材', 'name'), '中文素材')

    def test_strict_identifier_and_numeric_bounds(self):
        for value in ('', '../secret', 'a b', 'é', 'x' * 129):
            with self.subTest(value=value):
                with self.assertRaises(InvalidMessage):
                    identifier(value, 'id')
        self.assertEqual(identifier('a:b-c_d.e', 'id'), 'a:b-c_d.e')
        with self.assertRaises(InvalidMessage):
            integer(2**53, 'n')
        with self.assertRaises(InvalidMessage):
            integer(1.0, 'n')
        self.assertEqual(integer(0, 'n', 0), 0)

    def test_timezone_required_and_epoch_supported(self):
        for value in ('2026-11-11', '2026-11-11T00:00:00', 'not-time', '1969-01-01T00:00:00Z'):
            with self.subTest(value=value):
                with self.assertRaises(InvalidMessage):
                    timestamp(value)
        self.assertEqual(timestamp('1970-01-01T00:00:00Z'), '1970-01-01T00:00:00.000000Z')

    def test_duplicate_json_keys_are_rejected_at_any_depth(self):
        with self.assertRaises(InvalidMessage):
            decode_json('{"x":1,"x":2}')
        with self.assertRaises(InvalidMessage):
            decode_json('{"a":{"x":1,"x":2}}')
        self.assertEqual(decode_json(b'{"x":1}'), {'x': 1})
        with self.assertRaises(InvalidMessage):
            decode_json(b'\xff')

    def test_canonical_nonfinite_and_unserializable(self):
        with self.assertRaises(InvalidMessage):
            canonical({'value': float('nan')})
        with self.assertRaises(InvalidMessage):
            canonical({'value': object()})
        self.assertEqual(canonical({'b': 2, 'a': 1}), '{"a":1,"b":2}')
        self.assertEqual(digest({'b': 2, 'a': 1}), digest({'a': 1, 'b': 2}))

    def test_scope_sets_reject_duplicate_and_unknown_right(self):
        with self.assertRaises(InvalidMessage):
            string_set(['a', 'a'], 'values')
        with self.assertRaises(InvalidMessage):
            string_set([], 'values')
        value = registration()
        value['payload']['scope']['rights'] = ['sell_everything']
        with self.assertRaises(InvalidMessage):
            parse_message(value)
        self.assertEqual(string_set(['b', 'a'], 'values'), ['a', 'b'])


class AdapterTests(unittest.TestCase):
    def test_csv_roundtrip_preserves_quoted_payload(self):
        message = registration()
        message['payload']['title'] = '中文, "quoted" title'
        normalized = parse_message(message)
        content = encode_csv([normalized])
        rows = list(csv_records(io.StringIO(content)))
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0][2])
        self.assertEqual(rows[0][1], normalized)
        self.assertEqual(rows[0][1].payload['title'], '中文, "quoted" title')

    def test_csv_rejects_duplicate_header(self):
        with self.assertRaises(InvalidMessage):
            list(csv_records(io.StringIO('event_id,event_id\nx,y\n')))
        with self.assertRaises(InvalidMessage):
            list(csv_records(io.StringIO('')))
        good = encode_csv([parse_message(registration())])
        self.assertEqual(len(list(csv_records(io.StringIO(good)))), 1)

    def test_csv_bad_integer_becomes_row_error(self):
        good = encode_csv([parse_message(registration())])
        bad = good.replace('north,1,asset1', 'north,1.0,asset1')
        rows = list(csv_records(io.StringIO(bad)))
        self.assertIsNone(rows[0][1])
        self.assertIsInstance(rows[0][2], InvalidMessage)
        self.assertEqual(rows[0][0], 2)

    def test_ndjson_isolates_malformed_lines(self):
        content = (canonical(registration()) + '\ninvalid\n\n' + canonical(registration(2, asset='asset2')) + '\n').encode()
        rows = list(ndjson_records(io.BytesIO(content)))
        self.assertEqual([row[0] for row in rows], [1, 2, 4])
        self.assertIsNone(rows[0][2])
        self.assertIsInstance(rows[1][2], InvalidMessage)
        self.assertEqual(rows[2][1].asset_id, 'asset2')

    def test_ndjson_oversized_line_is_drained_once(self):
        content = b'x' * 100 + b'\n{}\n'
        rows = list(ndjson_records(io.BytesIO(content), max_bytes=20))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][3], '[oversized line]')
        self.assertEqual(rows[1][0], 2)
        self.assertIsInstance(rows[1][2], InvalidMessage)

    def test_ndjson_invalid_utf8_does_not_hide_following_line(self):
        content = b'\xff\n' + canonical(registration()).encode() + b'\n'
        rows = list(ndjson_records(io.BytesIO(content)))
        self.assertEqual(len(rows), 2)
        self.assertIsInstance(rows[0][2], InvalidMessage)
        self.assertEqual(rows[1][1].event_id, 'north:event1')

    def test_envelope_is_all_or_nothing(self):
        envelope = {'source': 'exchange', 'schema': 1, 'messages': [registration()]}
        self.assertEqual(len(adapt_envelope(envelope, 'exchange')), 1)
        with self.assertRaises(InvalidMessage):
            adapt_envelope(envelope, 'other')
        envelope['messages'].append({'invalid': True})
        with self.assertRaises(InvalidMessage) as failure:
            adapt_envelope(envelope, 'exchange')
        self.assertEqual(failure.exception.context['index'], 2)
