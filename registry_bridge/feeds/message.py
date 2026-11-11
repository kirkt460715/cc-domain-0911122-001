import datetime as dt
import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass

from registry_bridge.errors import InvalidMessage

IDENTIFIER = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z')
DIGEST = re.compile(r'[0-9a-f]{64}\Z')
OPERATIONS = frozenset({'register', 'amend', 'suspend', 'restore', 'revoke'})
RIGHTS = frozenset({'hold', 'process', 'operate'})
TOP_FIELDS = frozenset({
    'version', 'event_id', 'issuer', 'sequence', 'asset_id',
    'revision', 'operation', 'effective_at', 'payload',
})


def canonical(value):
    try:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True,
            separators=(',', ':'), allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise InvalidMessage('Value cannot be serialized canonically') from exc


def digest(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


def identifier(value, field):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise InvalidMessage('Invalid identifier', context={'field': field})
    return value


def integer(value, field, minimum=1, maximum=2**53 - 1):
    if isinstance(value, bool) or not isinstance(value, int):
        raise InvalidMessage('Expected integer', context={'field': field})
    if not minimum <= value <= maximum:
        raise InvalidMessage('Integer outside range', context={'field': field})
    return value


def text(value, field, maximum=512):
    if not isinstance(value, str):
        raise InvalidMessage('Expected text', context={'field': field})
    normalized = unicodedata.normalize('NFC', value).strip()
    if not normalized or len(normalized) > maximum:
        raise InvalidMessage('Invalid text length', context={'field': field})
    if any(ord(char) < 32 for char in normalized):
        raise InvalidMessage('Control character in text', context={'field': field})
    return normalized


def timestamp(value, field='effective_at'):
    if not isinstance(value, str):
        raise InvalidMessage('Expected timestamp', context={'field': field})
    try:
        parsed = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise InvalidMessage('Invalid timestamp', context={'field': field}) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise InvalidMessage('Timestamp requires timezone', context={'field': field})
    normalized = parsed.astimezone(dt.timezone.utc)
    if normalized.year < 1970:
        raise InvalidMessage('Timestamp predates supported epoch')
    return normalized.isoformat(timespec='microseconds').replace('+00:00', 'Z')


def string_set(value, field, *, allowed=None, maximum=100):
    if not isinstance(value, list) or not value or len(value) > maximum:
        raise InvalidMessage('Expected nonempty bounded list', context={'field': field})
    result = []
    for item in value:
        item = text(item, field, 100)
        if allowed is not None and item not in allowed:
            raise InvalidMessage('Unknown list item', context={'field': field, 'item': item})
        result.append(item)
    if len(result) != len(set(result)):
        raise InvalidMessage('Duplicate list item', context={'field': field})
    return sorted(result)


def strict_object(value, fields, required, field):
    if not isinstance(value, dict):
        raise InvalidMessage('Expected object', context={'field': field})
    unknown = set(value) - fields
    missing = required - set(value)
    if unknown or missing:
        raise InvalidMessage(
            'Object fields do not match schema',
            context={'field': field, 'unknown': sorted(unknown), 'missing': sorted(missing)},
        )
    return value


def validate_scope(value):
    fields = {'purposes', 'regions', 'rights', 'expires_at'}
    strict_object(value, fields, fields, 'scope')
    return {
        'purposes': string_set(value['purposes'], 'purposes'),
        'regions': string_set(value['regions'], 'regions'),
        'rights': string_set(value['rights'], 'rights', allowed=RIGHTS),
        'expires_at': timestamp(value['expires_at'], 'expires_at'),
    }


def validate_evidence(value):
    if not isinstance(value, list) or not 1 <= len(value) <= 50:
        raise InvalidMessage('Evidence requires one to fifty descriptors')
    result = []
    seen = set()
    for row in value:
        strict_object(row, {'kind', 'sha256'}, {'kind', 'sha256'}, 'evidence')
        kind = text(row['kind'], 'evidence.kind', 64)
        sha = row['sha256']
        if not isinstance(sha, str) or not DIGEST.fullmatch(sha):
            raise InvalidMessage('Evidence digest must be lowercase SHA256')
        if (kind, sha) in seen:
            raise InvalidMessage('Duplicate evidence descriptor')
        seen.add((kind, sha))
        result.append({'kind': kind, 'sha256': sha})
    return sorted(result, key=lambda row: (row['kind'], row['sha256']))


def validate_payload(operation, value):
    register = {'title', 'holder', 'scope', 'evidence', 'dataset_fingerprint'}
    if operation == 'register':
        strict_object(value, register, register, 'payload')
        sha = value['dataset_fingerprint']
        if not isinstance(sha, str) or not DIGEST.fullmatch(sha):
            raise InvalidMessage('Invalid dataset fingerprint')
        return {
            'title': text(value['title'], 'title'),
            'holder': identifier(value['holder'], 'holder'),
            'scope': validate_scope(value['scope']),
            'evidence': validate_evidence(value['evidence']),
            'dataset_fingerprint': sha,
        }
    if operation == 'amend':
        fields = {'reason', 'changes'}
        strict_object(value, fields, fields, 'payload')
        changes = value['changes']
        strict_object(changes, register, set(), 'changes')
        if not changes:
            raise InvalidMessage('Amendment requires actual changes')
        normalized = {}
        for key, item in changes.items():
            if key == 'title':
                normalized[key] = text(item, key)
            elif key == 'holder':
                normalized[key] = identifier(item, key)
            elif key == 'scope':
                normalized[key] = validate_scope(item)
            elif key == 'evidence':
                normalized[key] = validate_evidence(item)
            else:
                if not isinstance(item, str) or not DIGEST.fullmatch(item):
                    raise InvalidMessage('Invalid dataset fingerprint')
                normalized[key] = item
        return {'reason': text(value['reason'], 'reason'), 'changes': normalized}
    strict_object(value, {'reason'}, {'reason'}, 'payload')
    return {'reason': text(value['reason'], 'reason')}


@dataclass(frozen=True)
class Message:
    event_id: str
    issuer: str
    sequence: int
    asset_id: str
    revision: int
    operation: str
    effective_at: str
    payload_json: str

    @property
    def payload(self):
        return json.loads(self.payload_json)

    def as_dict(self):
        return {
            'version': 1,
            'event_id': self.event_id,
            'issuer': self.issuer,
            'sequence': self.sequence,
            'asset_id': self.asset_id,
            'revision': self.revision,
            'operation': self.operation,
            'effective_at': self.effective_at,
            'payload': self.payload,
        }

    @property
    def fingerprint(self):
        return digest(self.as_dict())


def parse_message(value):
    strict_object(value, TOP_FIELDS, TOP_FIELDS, 'message')
    version = integer(value['version'], 'version', 1, 1)
    operation = value['operation']
    if not isinstance(operation, str) or operation not in OPERATIONS:
        raise InvalidMessage('Unknown operation')
    effective = timestamp(value['effective_at'])
    payload = validate_payload(operation, value['payload'])
    scope = payload.get('scope') or payload.get('changes', {}).get('scope')
    if scope and scope['expires_at'] <= effective:
        raise InvalidMessage('Scope must expire after event effective time')
    revision = integer(value['revision'], 'revision')
    if operation == 'register' and revision != 1:
        raise InvalidMessage('Registration starts at revision one')
    if operation != 'register' and revision == 1:
        raise InvalidMessage('Non-registration requires later revision')
    return Message(
        event_id=identifier(value['event_id'], 'event_id'),
        issuer=identifier(value['issuer'], 'issuer'),
        sequence=integer(value['sequence'], 'sequence'),
        asset_id=identifier(value['asset_id'], 'asset_id'),
        revision=revision,
        operation=operation,
        effective_at=effective,
        payload_json=canonical(payload),
    )


def no_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise InvalidMessage('Duplicate JSON object key', context={'key': key})
        result[key] = value
    return result


def decode_json(raw):
    if isinstance(raw, bytes):
        try:
            raw = raw.decode('utf-8')
        except UnicodeDecodeError as exc:
            raise InvalidMessage('Input is not UTF-8') from exc
    if not isinstance(raw, str) or len(raw.encode('utf-8')) > 1024 * 1024:
        raise InvalidMessage('JSON message exceeds one MiB')
    try:
        return json.loads(raw, object_pairs_hook=no_duplicate_keys)
    except (ValueError, RecursionError) as exc:
        raise InvalidMessage('Malformed JSON') from exc
