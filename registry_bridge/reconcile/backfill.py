"""Sparse, evidence-driven backfill against a registry exchange.

A run freezes one exchange view (epoch, revision, ceiling) up front and never
mixes two views: when the frozen view expires the run is left unfinished and a
new run must be planned.  Ranges whose local segment digest already matches the
exchange summary are never downloaded; differing ranges are narrowed by binary
splitting and only the missing or divergent leaves are fetched as event
bundles.  Every step is persisted, so a run survives restarts and an exhausted
request budget, and bundles already imported are never applied twice.

This is deliberately not a "remote overwrites local" sync.  When the local
ledger already holds an immutable event at the same sequence (or reusing the
same event id) with different content, both sides are retained as evidence and
the range is marked ``conflicted``; automatic processing of that range stops
while other ranges continue.
"""

import json

from registry_bridge.errors import Conflict, InvalidMessage, NotFound, RegistryError
from registry_bridge.feeds import bundle
from registry_bridge.feeds.message import canonical, digest, identifier, integer, strict_object, text
from registry_bridge.feeds.receiver import drain_stream, receive
from registry_bridge.ledger import audit, issuers
from registry_bridge.reconcile import resolution
from registry_bridge.reconcile.exchange import ExchangeClient, ExchangeUnavailable, ViewExpired

DDL = '''
CREATE TABLE IF NOT EXISTS backfill_runs(
 run_id TEXT PRIMARY KEY, issuer TEXT NOT NULL, base_url TEXT NOT NULL,
 epoch TEXT NOT NULL, revision INTEGER NOT NULL, ceiling INTEGER NOT NULL,
 segment_width INTEGER NOT NULL, request_budget INTEGER NOT NULL,
 requests_used INTEGER NOT NULL DEFAULT 0,
 state TEXT NOT NULL, pause_reason TEXT,
 planned_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS backfill_ranges(
 run_id TEXT NOT NULL, lower INTEGER NOT NULL, upper INTEGER NOT NULL,
 state TEXT NOT NULL, detail TEXT,
 UNIQUE(run_id,lower,upper));
CREATE INDEX IF NOT EXISTS backfill_ranges_state ON backfill_ranges(run_id,state,lower);
CREATE TABLE IF NOT EXISTS backfill_bundles(
 run_id TEXT NOT NULL, lower INTEGER NOT NULL, upper INTEGER NOT NULL,
 bundle_sha256 TEXT NOT NULL, accepted INTEGER NOT NULL, duplicate INTEGER NOT NULL,
 imported_at TEXT NOT NULL, PRIMARY KEY(run_id,bundle_sha256));
'''

# Range lifecycle:
#   pending    -> not yet examined
#   processing -> a network call is in flight (crash-safe resume marker)
#   matched    -> local digest already equal to the exchange summary
#   split      -> narrowed into two pending child ranges
#   backfilled -> missing events fetched and applied (or proven identical)
#   conflicted -> local immutable event disagrees; evidence retained, stopped
TERMINAL_RANGE_STATES = {'matched', 'backfilled', 'conflicted'}
RUN_TERMINAL = {'complete', 'expired'}


class BackfillManager:
    def __init__(self, store):
        if store._depth:
            raise InvalidMessage('Backfill manager must initialize outside a transaction')
        self.store = store
        store.connection.executescript(DDL)

    # ------------------------------------------------------------------ plan
    def plan(self, issuer, base_url, *, run_id=None, request_budget=100, segment_width=8, actor='operator'):
        identifier(issuer, 'issuer')
        text(base_url, 'base_url', 512)
        integer(request_budget, 'request_budget', 1, 100000)
        integer(segment_width, 'segment_width', 1, 10000)
        issuers.get(self.store, issuer)
        client = ExchangeClient(base_url)
        view = client.view()
        strict_object(view, {'epoch', 'revision', 'ceiling', 'issuer'},
                      {'epoch', 'revision', 'ceiling', 'issuer'}, 'view')
        if view['issuer'] != issuer:
            raise Conflict('Exchange view describes a different issuer', context={
                'expected': issuer, 'remote': view['issuer'],
            })
        epoch = text(view['epoch'], 'epoch', 128)
        revision = integer(view['revision'], 'revision', 1)
        ceiling = integer(view['ceiling'], 'ceiling', 0)
        if run_id is None:
            run_id = digest({'issuer': issuer, 'epoch': epoch, 'revision': revision,
                             'ceiling': ceiling, 'at': self.store.clock()})[:32]
        identifier(run_id, 'run_id')
        now = self.store.clock()
        with self.store.transaction():
            if self.store.one('SELECT run_id FROM backfill_runs WHERE run_id=?', (run_id,)):
                raise Conflict('Backfill run id already exists', context={'run_id': run_id})
            self.store.execute(
                'INSERT INTO backfill_runs(run_id,issuer,base_url,epoch,revision,ceiling,'
                'segment_width,request_budget,requests_used,state,pause_reason,planned_at,updated_at) '
                "VALUES(?,?,?,?,?,?,?,?,?, 'planned', NULL, ?, ?)",
                (run_id, issuer, base_url, epoch, revision, ceiling, segment_width,
                 request_budget, client.requests_used, now, now),
            )
            if ceiling >= 1:
                for lower in range(1, ceiling + 1, segment_width):
                    upper = min(ceiling, lower + segment_width - 1)
                    self._insert_range(run_id, lower, upper, 'pending', None)
            audit.append(self.store, 'backfill.planned', actor, {
                'run_id': run_id, 'issuer': issuer, 'epoch': epoch, 'revision': revision,
                'ceiling': ceiling, 'request_budget': request_budget,
            })
        return self.status(run_id)

    # --------------------------------------------------------------- execute
    def execute(self, run_id, *, actor='operator'):
        identifier(run_id, 'run_id')
        run = self._get_run(run_id)
        if run['state'] in RUN_TERMINAL:
            return self.status(run_id)
        client = ExchangeClient(run['base_url'])
        # A new HTTP client starts its counter at zero; seed it with the
        # requests already billed to this run in previous processes.
        client.requests_used = run['requests_used']
        self._set_run_state(run_id, 'running', None)
        try:
            while True:
                run = self._get_run(run_id)
                if run['requests_used'] >= run['request_budget']:
                    self._set_run_state(
                        run_id, 'paused', 'request_budget_exhausted',
                        extra={'requests_used': run['requests_used'],
                               'request_budget': run['request_budget']},
                    )
                    break
                row = self.store.one(
                    "SELECT * FROM backfill_ranges WHERE run_id=? AND state IN ('pending','processing') "
                    'ORDER BY lower LIMIT 1', (run_id,),
                )
                if row is None:
                    self._finish(run_id, actor)
                    break
                self._advance(client, run, row)
                # Persist the billed request count even when an examined range
                # reached a terminal state without its own billing write.
                self._bill(run_id, client.requests_used)
        except BudgetReached:
            self._set_run_state(run_id, 'paused', 'request_budget_exhausted')
        except ViewExpired as exc:
            # The frozen view is gone. Keep every unfinished range and stop;
            # conclusions must never combine two different views.
            self._set_run_state(run_id, 'expired', 'view_expired',
                                extra={'error': exc.as_dict()})
            audit.append(self.store, 'backfill.expired', actor, {
                'run_id': run_id, 'requests_used': self._get_run(run_id)['requests_used'],
            })
        except ExchangeUnavailable as exc:
            self._set_run_state(run_id, 'paused', 'exchange_unavailable',
                                extra={'error': exc.as_dict()})
        except RegistryError as exc:
            # Protocol-level failure (malformed bundle, invalid summary...).
            # The work list stays intact so the run can be resumed after review.
            self._bill(run_id, client.requests_used)
            self._set_run_state(run_id, 'paused', 'protocol_error',
                                extra={'error': exc.as_dict()})
        return self.status(run_id)

    def _advance(self, client, run, row):
        lower, upper = row['lower'], row['upper']
        if row['state'] == 'pending':
            self._update_range(run['run_id'], lower, upper, 'processing', row['detail'])
        local_count = self.store.scalar(
            'SELECT COUNT(*) FROM events WHERE issuer=? AND sequence BETWEEN ? AND ?',
            (run['issuer'], lower, upper),
        )
        is_leaf = lower == upper
        if not is_leaf and local_count == 0:
            # Sparse, fully absent range: one bundle fills the whole segment
            # instead of walking it one summary at a time.
            self._download_bulk_empty(client, run, lower, upper)
            return
        summary = self._request(client, run, lambda: client.summary(
            run['epoch'], run['revision'], lower, upper))
        strict_object(summary, {'lower', 'upper', 'sha256', 'count', 'revision'},
                      {'lower', 'upper', 'sha256', 'count', 'revision'}, 'summary')
        local_digest = self._local_digest(run['issuer'], lower, upper)
        if local_digest == summary['sha256'] and local_count == summary['count']:
            self._terminal(run['run_id'], lower, upper, 'matched', {
                'lower': lower, 'upper': upper, 'sha256': local_digest,
                'count': local_count, 'requests': 1,
            })
            return
        if is_leaf:
            self._download_leaf(client, run, lower, upper, summary)
            return
        mid = (lower + upper) // 2
        with self.store.transaction():
            self._update_range(run['run_id'], lower, upper, 'split', {
                'reason': 'digest_differs', 'local_digest': local_digest,
                'remote_digest': summary['sha256'], 'local_count': local_count,
                'remote_count': summary['count'],
            })
            self._insert_range(run['run_id'], lower, mid, 'pending', None)
            self._insert_range(run['run_id'], mid + 1, upper, 'pending', None)
            self._bill(run['run_id'], client.requests_used)

    def _download_bulk_empty(self, client, run, lower, upper):
        value = self._request(client, run, lambda: client.bundle(
            run['epoch'], run['revision'], lower, upper))
        messages = bundle.verify_bundle(value)
        if [m.sequence for m in messages] != list(range(lower, upper + 1)):
            raise InvalidMessage('Exchange bundle does not cover the requested range')
        with self.store.transaction():
            result = bundle.import_bundle(self.store, value)
            self._record_bundle(run['run_id'], lower, upper, value['sha256'],
                                result['accepted'], result['duplicate'])
            self._terminal(run['run_id'], lower, upper, 'backfilled', {
                'lower': lower, 'upper': upper, 'bundle_sha256': value['sha256'],
                'accepted': result['accepted'], 'duplicate': result['duplicate'],
                'mode': 'bulk_empty',
            })
            self._bill(run['run_id'], client.requests_used)

    def _download_leaf(self, client, run, lower, upper, summary):
        recorded = self.store.one(
            'SELECT * FROM backfill_bundles WHERE run_id=? AND lower=? AND upper=?',
            (run['run_id'], lower, upper),
        )
        if recorded is None:
            value = self._request(client, run, lambda: client.bundle(
                run['epoch'], run['revision'], lower, upper))
            messages = bundle.verify_bundle(value)
            if len(messages) != 1 or messages[0].sequence != lower:
                raise InvalidMessage('Exchange bundle does not cover the requested leaf')
            incoming = messages[0]
            conflicts = []
            accepted = duplicate = 0
            # receive() keeps every local immutable event; a disagreement is
            # returned as a conflict, never as an overwrite or deletion.
            result = receive(self.store, incoming, drain=False)
            if result['status'] == 'conflict':
                conflicts.append({
                    'sequence': lower, 'conflict_id': result['conflict_id'],
                    'incoming_event_id': incoming.event_id,
                    'incoming_fingerprint': incoming.fingerprint,
                })
            elif result['status'] == 'duplicate':
                duplicate += 1
            else:
                accepted += 1
            local_digest = self._local_digest(run['issuer'], lower, upper)
            with self.store.transaction():
                if conflicts:
                    self._terminal(run['run_id'], lower, upper, 'conflicted', {
                        'lower': lower, 'upper': upper, 'conflicts': conflicts,
                        'remote_digest': summary['sha256'], 'local_digest': local_digest,
                        'bundle_sha256': value['sha256'],
                    })
                else:
                    if local_digest != summary['sha256']:
                        self._terminal(run['run_id'], lower, upper, 'conflicted', {
                            'lower': lower, 'upper': upper,
                            'reason': 'digest_still_differs_after_import',
                            'remote_digest': summary['sha256'], 'local_digest': local_digest,
                            'bundle_sha256': value['sha256'],
                        })
                    else:
                        self._record_bundle(run['run_id'], lower, upper, value['sha256'],
                                            accepted, duplicate)
                        self._terminal(run['run_id'], lower, upper, 'backfilled', {
                            'lower': lower, 'upper': upper,
                            'bundle_sha256': value['sha256'],
                            'accepted': accepted, 'duplicate': duplicate, 'mode': 'leaf',
                        })
                self._bill(run['run_id'], client.requests_used)
            return
        # Restart after a bundle import whose range update did not persist:
        # verify against the recorded evidence instead of downloading again.
        local_digest = self._local_digest(run['issuer'], lower, upper)
        state = 'backfilled' if local_digest == summary['sha256'] else 'conflicted'
        self._terminal(run['run_id'], lower, upper, state, {
            'lower': lower, 'upper': upper, 'reused_recorded_bundle': True,
            'bundle_sha256': recorded['bundle_sha256'],
            'remote_digest': summary['sha256'], 'local_digest': local_digest,
        })

    def _finish(self, run_id, actor):
        run = self._get_run(run_id)
        # Newly received contiguous events become effective only through the
        # existing continuous-sequence projection.
        advanced = drain_stream(self.store, run['issuer'])
        ranges = self._ranges(run_id)
        conflicted = [r for r in ranges if r['state'] == 'conflicted']
        with self.store.transaction():
            self.store.execute(
                "UPDATE backfill_runs SET state='complete',pause_reason=NULL,updated_at=? WHERE run_id=?",
                (self.store.clock(), run_id),
            )
            self._bill(run_id, self._get_run(run_id)['requests_used'])
        audit.append(self.store, 'backfill.completed', actor, {
            'run_id': run_id, 'conflicted_ranges': len(conflicted),
            'projected_events': len(advanced),
            'requests_used': self._get_run(run_id)['requests_used'],
        })

    # ---------------------------------------------------------------- queries
    def status(self, run_id):
        identifier(run_id, 'run_id')
        run = self._get_run(run_id)
        ranges = self._ranges(run_id)
        counts = {state: 0 for state in
                  ('pending', 'processing', 'split', 'matched', 'backfilled', 'conflicted')}
        covered = {'matched': 0, 'backfilled': 0, 'conflicted': 0}
        for row in ranges:
            counts[row['state']] += 1
            if row['state'] in covered:
                covered[row['state']] += row['upper'] - row['lower'] + 1
        open_ranges = [r for r in ranges if r['state'] in ('pending', 'processing')]
        progress = issuers.progress(self.store, run['issuer'])
        return {
            'run_id': run_id,
            'issuer': run['issuer'],
            'view': {
                'epoch': run['epoch'], 'revision': run['revision'], 'ceiling': run['ceiling'],
                'frozen': True,
            },
            'state': run['state'],
            'pause_reason': run['pause_reason'],
            'request_budget': run['request_budget'],
            'requests_used': run['requests_used'],
            'range_counts': counts,
            'sequences': {
                'matched': covered['matched'],
                'backfilled': covered['backfilled'],
                'conflicted': covered['conflicted'],
                # Only leaf work remains; split parents are fully covered by children.
                'unfinished': sum(r['upper'] - r['lower'] + 1 for r in open_ranges),
            },
            'ranges': ranges,
            'ledger': {
                'progress': progress,
                'blockage': resolution.explain_blockage(self.store, run['issuer']),
                'gaps': resolution.gaps(self.store, run['issuer'], ceiling=run['ceiling'])['gaps'],
            },
        }

    def list_runs(self, *, limit=50):
        integer(limit, 'limit', 1, 1000)
        rows = self.store.all(
            'SELECT run_id,issuer,epoch,revision,ceiling,state,pause_reason,'
            'request_budget,requests_used,planned_at,updated_at '
            'FROM backfill_runs ORDER BY planned_at DESC,run_id DESC LIMIT ?', (limit,),
        )
        return [dict(row) for row in rows]

    def extend_budget(self, run_id, request_budget, *, actor='operator'):
        identifier(run_id, 'run_id')
        integer(request_budget, 'request_budget', 1, 100000)
        with self.store.transaction():
            run = self._get_run(run_id)
            if run['state'] == 'expired':
                raise Conflict('Expired view cannot be resumed; plan a new run against the current view')
            if run['state'] in RUN_TERMINAL:
                raise Conflict('Run has already finished', context={'state': run['state']})
            if request_budget <= run['request_budget']:
                raise Conflict('Budget can only be raised', context={
                    'current': run['request_budget'], 'requested': request_budget,
                })
            self.store.execute(
                'UPDATE backfill_runs SET request_budget=?,updated_at=? WHERE run_id=?',
                (request_budget, self.store.clock(), run_id),
            )
            audit.append(self.store, 'backfill.budget_extended', actor, {
                'run_id': run_id, 'request_budget': request_budget,
            })
        return self.status(run_id)

    # ---------------------------------------------------------------- helpers
    def _request(self, client, run, call):
        if client.requests_used >= run['request_budget']:
            self._set_run_state(
                run['run_id'], 'paused', 'request_budget_exhausted',
                extra={'requests_used': client.requests_used,
                       'request_budget': run['request_budget']},
            )
            raise BudgetReached(run['run_id'])
        return call()

    def _bill(self, run_id, used):
        self.store.execute(
            'UPDATE backfill_runs SET requests_used=?,updated_at=? WHERE run_id=?',
            (used, self.store.clock(), run_id),
        )

    def _get_run(self, run_id):
        row = self.store.one('SELECT * FROM backfill_runs WHERE run_id=?', (run_id,))
        if row is None:
            raise NotFound('Backfill run does not exist', context={'run_id': run_id})
        return dict(row)

    def _ranges(self, run_id):
        rows = self.store.all(
            'SELECT lower,upper,state,detail FROM backfill_ranges WHERE run_id=? ORDER BY lower',
            (run_id,),
        )
        result = []
        for row in rows:
            item = {'lower': row['lower'], 'upper': row['upper'], 'state': row['state']}
            if row['detail']:
                item['detail'] = json.loads(row['detail'])
            result.append(item)
        return result

    def _insert_range(self, run_id, lower, upper, state, detail):
        self.store.execute(
            'INSERT INTO backfill_ranges(run_id,lower,upper,state,detail) VALUES(?,?,?,?,?)',
            (run_id, lower, upper, state, canonical(detail) if detail is not None else None),
        )

    def _update_range(self, run_id, lower, upper, state, detail):
        self.store.execute(
            'UPDATE backfill_ranges SET state=?,detail=? WHERE run_id=? AND lower=? AND upper=?',
            (state, canonical(detail) if detail is not None else None, run_id, lower, upper),
        )

    def _terminal(self, run_id, lower, upper, state, detail):
        self._update_range(run_id, lower, upper, state, detail)

    def _record_bundle(self, run_id, lower, upper, sha256, accepted, duplicate):
        self.store.execute(
            'INSERT OR IGNORE INTO backfill_bundles(run_id,lower,upper,bundle_sha256,'
            'accepted,duplicate,imported_at) VALUES(?,?,?,?,?,?,?)',
            (run_id, lower, upper, sha256, accepted, duplicate, self.store.clock()),
        )

    def _set_run_state(self, run_id, state, reason, *, extra=None):
        with self.store.transaction():
            self.store.execute(
                'UPDATE backfill_runs SET state=?,pause_reason=?,updated_at=? WHERE run_id=?',
                (state, reason, self.store.clock(), run_id),
            )
            if extra is not None:
                audit.append(self.store, 'backfill.paused', 'backfill', {
                    'run_id': run_id, 'reason': reason, **extra,
                })

    def _local_digest(self, issuer, lower, upper):
        rows = self.store.all(
            'SELECT sequence,fingerprint FROM events WHERE issuer=? AND sequence BETWEEN ? AND ? '
            'ORDER BY sequence', (issuer, lower, upper),
        )
        return digest([[row['sequence'], row['fingerprint']] for row in rows])


class BudgetReached(RuntimeError):
    """Internal control-flow signal: the request budget is exhausted."""

    def __init__(self, run_id):
        super().__init__('request budget exhausted')
        self.run_id = run_id
