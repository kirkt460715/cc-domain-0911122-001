import json

from registry_bridge.errors import Conflict, InvalidMessage, LeaseLost, NotFound, StaleRevision
from registry_bridge.feeds.message import canonical, identifier, integer
from registry_bridge.feeds.receiver import drain_stream
from registry_bridge.ledger import audit, leases

DDL = '''
CREATE TABLE IF NOT EXISTS recovery_jobs(
 job_id TEXT PRIMARY KEY, issuer TEXT NOT NULL REFERENCES issuers(issuer),
 state TEXT NOT NULL, priority INTEGER NOT NULL, attempts INTEGER NOT NULL,
 max_attempts INTEGER NOT NULL, owner TEXT, generation INTEGER,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL, result TEXT, error TEXT,
 revision INTEGER NOT NULL DEFAULT 1);
CREATE INDEX IF NOT EXISTS recovery_jobs_queue ON recovery_jobs(state,priority,created_at,job_id);
'''


class RecoveryQueue:
    def __init__(self, store):
        self.store = store
        if store._depth:
            raise InvalidMessage('Recovery queue must be initialized outside a transaction')
        store.connection.executescript(DDL)

    def enqueue(self, job_id, issuer, *, priority=0, max_attempts=3):
        identifier(job_id, 'job_id')
        identifier(issuer, 'issuer')
        integer(priority, 'priority', 0, 100)
        integer(max_attempts, 'max_attempts', 1, 100)
        from registry_bridge.ledger import issuers
        with self.store.transaction():
            issuers.get(self.store, issuer)
            previous = self.store.one('SELECT * FROM recovery_jobs WHERE job_id=?', (job_id,))
            if previous:
                if previous['issuer'] != issuer or previous['priority'] != priority or previous['max_attempts'] != max_attempts:
                    raise Conflict('Recovery job ID already identifies a different request')
                return self.decode(previous)
            now = self.store.clock()
            self.store.execute(
                "INSERT INTO recovery_jobs(job_id,issuer,state,priority,attempts,max_attempts,created_at,updated_at) "
                "VALUES(?,?,'queued',?,0,?,?,?)",
                (job_id, issuer, priority, max_attempts, now, now),
            )
            audit.append(self.store, 'recovery.enqueued', 'operator', {'job_id': job_id, 'issuer': issuer})
            return self.get(job_id)

    def decode(self, row):
        result = dict(row)
        for key in ('result', 'error'):
            result[key] = json.loads(result[key]) if result[key] else None
        return result

    def get(self, job_id):
        identifier(job_id, 'job_id')
        row = self.store.one('SELECT * FROM recovery_jobs WHERE job_id=?', (job_id,))
        if row is None:
            raise NotFound('Recovery job does not exist')
        return self.decode(row)

    def claim(self, owner, *, ttl=30):
        identifier(owner, 'owner')
        with self.store.transaction():
            self.recover_expired()
            rows = self.store.all(
                "SELECT * FROM recovery_jobs WHERE state='queued' ORDER BY priority DESC,created_at,job_id",
            )
            for row in rows:
                name = 'recovery:' + row['issuer']
                try:
                    token = leases.acquire(self.store, name, owner, ttl)
                except Conflict:
                    continue
                self.store.execute(
                    "UPDATE recovery_jobs SET state='running',owner=?,generation=?,attempts=attempts+1,"
                    'updated_at=?,revision=revision+1 WHERE job_id=?',
                    (owner, token['generation'], self.store.clock(), row['job_id']),
                )
                audit.append(self.store, 'recovery.claimed', owner, {'job_id': row['job_id'], 'token': token})
                return {'job': self.get(row['job_id']), 'token': token}
            return None

    def assert_claim(self, job_id, token):
        job = self.get(job_id)
        held = leases.assert_held(self.store, token)
        if job['state'] != 'running':
            raise Conflict('Recovery job is not running')
        if held['name'] != 'recovery:' + job['issuer']:
            raise LeaseLost('Recovery lease belongs to a different issuer')
        if job['owner'] != held['owner'] or job['generation'] != held['generation']:
            raise LeaseLost('Recovery job fencing token changed')
        return job

    def heartbeat(self, job_id, token, *, ttl=30):
        with self.store.transaction():
            self.assert_claim(job_id, token)
            updated = leases.renew(self.store, token, ttl)
            self.store.execute('UPDATE recovery_jobs SET updated_at=? WHERE job_id=?', (self.store.clock(), job_id))
            return updated

    def execute(self, job_id, token, *, limit=1000):
        integer(limit, 'limit', 1, 100000)
        with self.store.transaction():
            job = self.assert_claim(job_id, token)
            from registry_bridge.reconcile.resolution import explain_blockage
            advanced = drain_stream(self.store, job['issuer'], limit=limit)
            blockage = explain_blockage(self.store, job['issuer'])
            state = 'complete' if blockage['kind'] == 'caught_up' else 'waiting'
            result = {'advanced': advanced, 'blockage': blockage}
            self.store.execute(
                'UPDATE recovery_jobs SET state=?,result=?,error=NULL,updated_at=?,revision=revision+1 WHERE job_id=?',
                (state, canonical(result), self.store.clock(), job_id),
            )
            leases.release(self.store, token)
            audit.append(self.store, 'recovery.executed', job['owner'], {'job_id': job_id, 'state': state, 'count': len(advanced)})
            return self.get(job_id)

    def fail(self, job_id, token, error):
        if not isinstance(error, dict) or not isinstance(error.get('code'), str):
            raise InvalidMessage('Failure requires structured error code')
        canonical(error)
        with self.store.transaction():
            job = self.assert_claim(job_id, token)
            state = 'failed' if job['attempts'] >= job['max_attempts'] else 'queued'
            self.store.execute(
                'UPDATE recovery_jobs SET state=?,error=?,updated_at=?,revision=revision+1 WHERE job_id=?',
                (state, canonical(error), self.store.clock(), job_id),
            )
            leases.release(self.store, token)
            audit.append(self.store, 'recovery.failed', job['owner'], {'job_id': job_id, 'state': state, 'error': error})
            return self.get(job_id)

    def recover_expired(self):
        recovered = []
        with self.store.transaction():
            rows = self.store.all("SELECT * FROM recovery_jobs WHERE state='running' ORDER BY job_id")
            for row in rows:
                token = {'name': 'recovery:' + row['issuer'], 'owner': row['owner'], 'generation': row['generation']}
                try:
                    leases.assert_held(self.store, token)
                except LeaseLost:
                    state = 'failed' if row['attempts'] >= row['max_attempts'] else 'queued'
                    error = {'code': 'lease_expired', 'generation': row['generation']}
                    self.store.execute(
                        'UPDATE recovery_jobs SET state=?,error=?,updated_at=?,revision=revision+1 WHERE job_id=?',
                        (state, canonical(error), self.store.clock(), row['job_id']),
                    )
                    audit.append(self.store, 'recovery.expired', 'scheduler', {'job_id': row['job_id'], 'state': state})
                    recovered.append(row['job_id'])
            return recovered

    def resume(self, job_id, revision):
        integer(revision, 'revision')
        with self.store.transaction():
            job = self.get(job_id)
            if job['revision'] != revision:
                raise StaleRevision('Recovery job changed')
            if job['state'] != 'waiting':
                raise Conflict('Only waiting jobs can be resumed')
            if job['attempts'] >= job['max_attempts']:
                raise Conflict('Recovery retry budget is exhausted; enqueue a new deliberate job')
            self.store.execute(
                "UPDATE recovery_jobs SET state='queued',updated_at=?,revision=revision+1 WHERE job_id=?",
                (self.store.clock(), job_id),
            )
            audit.append(self.store, 'recovery.resumed', 'operator', {'job_id': job_id})
            return self.get(job_id)

    def cancel(self, job_id, revision, reason):
        from registry_bridge.feeds.message import text
        reason = text(reason, 'reason')
        with self.store.transaction():
            job = self.get(job_id)
            if job['revision'] != revision:
                raise StaleRevision('Recovery job changed')
            if job['state'] not in {'queued', 'waiting'}:
                raise Conflict('Only queued or waiting jobs may be cancelled')
            self.store.execute(
                "UPDATE recovery_jobs SET state='cancelled',error=?,updated_at=?,revision=revision+1 WHERE job_id=?",
                (canonical({'code': 'cancelled', 'reason': reason}), self.store.clock(), job_id),
            )
            audit.append(self.store, 'recovery.cancelled', 'operator', {'job_id': job_id, 'reason': reason})
            return self.get(job_id)

    def list(self, *, state=None, limit=100):
        integer(limit, 'limit', 1, 10000)
        states = {'queued', 'running', 'waiting', 'failed', 'complete', 'cancelled'}
        if state is not None and state not in states:
            raise InvalidMessage('Unknown recovery state')
        clause = 'WHERE state=?' if state else ''
        params = [state, limit] if state else [limit]
        return [self.decode(row) for row in self.store.all(
            f'SELECT * FROM recovery_jobs {clause} ORDER BY priority DESC,created_at,job_id LIMIT ?', params,
        )]
