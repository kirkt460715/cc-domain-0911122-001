import json

from registry_bridge.errors import Conflict, InvalidMessage, NotFound, StaleRevision
from registry_bridge.feeds.message import canonical, identifier, integer, string_set, text
from registry_bridge.feeds.receiver import receive
from registry_bridge.ledger import audit

DDL = '''
CREATE TABLE IF NOT EXISTS feed_contracts(
 issuer TEXT PRIMARY KEY REFERENCES issuers(issuer), revision INTEGER NOT NULL,
 allowed_operations TEXT NOT NULL, required_evidence TEXT NOT NULL,
 allowed_regions TEXT NOT NULL, max_gap INTEGER NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS contract_rejections(
 rejection_id INTEGER PRIMARY KEY AUTOINCREMENT, issuer TEXT NOT NULL,
 event_id TEXT NOT NULL, contract_revision INTEGER NOT NULL,
 reasons TEXT NOT NULL, message TEXT NOT NULL, at TEXT NOT NULL);
'''


class ContractGate:
    def __init__(self, store):
        self.store = store
        if store._depth:
            raise InvalidMessage('Contract gate must initialize outside a transaction')
        store.connection.executescript(DDL)

    def get(self, issuer, *, required=True):
        identifier(issuer, 'issuer')
        row = self.store.one('SELECT * FROM feed_contracts WHERE issuer=?', (issuer,))
        if row is None:
            if required:
                raise NotFound('Feed contract does not exist')
            return None
        for key in ('allowed_operations', 'required_evidence', 'allowed_regions'):
            row[key] = json.loads(row[key])
        return row

    def configure(self, issuer, revision, *, allowed_operations, required_evidence, allowed_regions, max_gap=10000):
        from registry_bridge.feeds.message import OPERATIONS
        from registry_bridge.ledger import issuers
        integer(revision, 'revision', 0)
        operations = string_set(allowed_operations, 'allowed_operations', allowed=OPERATIONS)
        evidence = string_set(required_evidence, 'required_evidence')
        regions = string_set(allowed_regions, 'allowed_regions')
        integer(max_gap, 'max_gap', 0, 1000000)
        with self.store.transaction():
            issuers.get(self.store, issuer)
            previous = self.get(issuer, required=False)
            expected = previous['revision'] if previous else 0
            if revision != expected:
                raise StaleRevision('Feed contract revision changed')
            self.store.execute(
                'INSERT INTO feed_contracts(issuer,revision,allowed_operations,required_evidence,allowed_regions,max_gap,updated_at) '
                'VALUES(?,?,?,?,?,?,?) ON CONFLICT(issuer) DO UPDATE SET revision=excluded.revision,'
                'allowed_operations=excluded.allowed_operations,required_evidence=excluded.required_evidence,'
                'allowed_regions=excluded.allowed_regions,max_gap=excluded.max_gap,updated_at=excluded.updated_at',
                (issuer, revision + 1, canonical(operations), canonical(evidence), canonical(regions), max_gap, self.store.clock()),
            )
            audit.append(self.store, 'contract.configured', 'operator', {'issuer': issuer, 'revision': revision + 1})
            return self.get(issuer)

    def assess(self, message):
        from registry_bridge.feeds.message import Message, parse_message
        from registry_bridge.ledger import issuers, projection
        message = message if isinstance(message, Message) else parse_message(message)
        contract = self.get(message.issuer)
        progress = issuers.get(self.store, message.issuer)
        reasons = []
        if message.operation not in contract['allowed_operations']:
            reasons.append({'code': 'operation_not_accepted', 'operation': message.operation})
        gap = message.sequence - progress['last_sequence'] - 1
        if gap > contract['max_gap']:
            reasons.append({'code': 'sequence_gap_limit', 'gap': gap, 'limit': contract['max_gap']})
        payload = message.payload
        changes = payload.get('changes', payload)
        if 'evidence' in changes:
            kinds = {item['kind'] for item in changes['evidence']}
            missing = sorted(set(contract['required_evidence']) - kinds)
            if missing:
                reasons.append({'code': 'evidence_missing', 'kinds': missing})
        if 'scope' in changes:
            rejected = sorted(set(changes['scope']['regions']) - set(contract['allowed_regions']))
            if rejected:
                reasons.append({'code': 'region_not_accepted', 'regions': rejected})
        return {'accepted': not reasons, 'reasons': reasons, 'contract_revision': contract['revision'], 'message': message}

    def receive(self, value):
        with self.store.transaction():
            assessment = self.assess(value)
            message = assessment.pop('message')
            if not assessment['accepted']:
                cursor = self.store.execute(
                    'INSERT INTO contract_rejections(issuer,event_id,contract_revision,reasons,message,at) VALUES(?,?,?,?,?,?)',
                    (message.issuer, message.event_id, assessment['contract_revision'],
                     canonical(assessment['reasons']), canonical(message.as_dict()), self.store.clock()),
                )
                audit.append(self.store, 'contract.rejected', 'receiver', {
                    'event_id': message.event_id, 'contract_revision': assessment['contract_revision'],
                    'reasons': assessment['reasons'],
                })
                return {**assessment, 'rejection_id': cursor.lastrowid}
            result = receive(self.store, message)
            return {**assessment, 'result': result}

    def rejections(self, issuer, after=0, limit=100):
        identifier(issuer, 'issuer')
        integer(after, 'after', 0)
        integer(limit, 'limit', 1, 1000)
        rows = self.store.all(
            'SELECT * FROM contract_rejections WHERE issuer=? AND rejection_id>? ORDER BY rejection_id LIMIT ?',
            (issuer, after, limit),
        )
        for row in rows:
            row['reasons'] = json.loads(row['reasons'])
            row['message'] = json.loads(row['message'])
        return rows
