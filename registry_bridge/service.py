import json

from registry_bridge.errors import InvalidMessage
from registry_bridge.feeds import importer, receiver
from registry_bridge.feeds.message import decode_json, integer, strict_object
from registry_bridge.ledger import audit, issuers, leases
from registry_bridge.query import catalog, consumer, exporting
from registry_bridge.reconcile import cross_registry, rebuild, resolution


class RegistryService:
    def __init__(self, store):
        self.store = store
        from registry_bridge.operations import Operations
        self.operations = Operations(store)
        self.handlers = {
            'issuer.add': self.issuer_add,
            'issuer.list': self.issuer_list,
            'issuer.enable': self.issuer_enable,
            'issuer.progress': self.issuer_progress,
            'event.receive': self.event_receive,
            'event.get': self.event_get,
            'event.pending': self.event_pending,
            'event.drain': self.event_drain,
            'event.skip': self.event_skip,
            'import.feed': self.import_feed,
            'import.history': self.import_history,
            'deadletter.list': self.deadletter_list,
            'deadletter.replay': self.deadletter_replay,
            'asset.get': self.asset_get,
            'asset.list': self.asset_list,
            'asset.timeline': self.asset_timeline,
            'asset.diff': self.asset_diff,
            'asset.authorize': self.asset_authorize,
            'conflict.list': self.conflict_list,
            'conflict.get': self.conflict_get,
            'conflict.close': self.conflict_close,
            'stream.gaps': self.stream_gaps,
            'stream.blockage': self.stream_blockage,
            'dataset.claims': self.dataset_claims,
            'dataset.scan': self.dataset_scan,
            'consumer.checkpoint': self.consumer_checkpoint,
            'consumer.changes': self.consumer_changes,
            'consumer.advance': self.consumer_advance,
            'consumer.export': self.consumer_export,
            'consumer.acknowledge': self.consumer_acknowledge,
            'consumer.lag': self.consumer_lag,
            'lease.acquire': self.lease_acquire,
            'lease.renew': self.lease_renew,
            'lease.release': self.lease_release,
            'lease.list': self.lease_list,
            'audit.page': self.audit_page,
            'audit.verify': self.audit_verify,
            'system.check': self.system_check,
            'system.rebuild': self.system_rebuild,
            'system.metrics': self.system_metrics,
            'system.fingerprint': self.system_fingerprint,
            'snapshot.create': self.snapshot_create,
            'snapshot.verify': self.snapshot_verify,
            'snapshot.compare': self.snapshot_compare,
        }

    def call(self, operation, parameters=None):
        if operation not in self.handlers:
            return self.operations.call(operation, {} if parameters is None else parameters)
        parameters = {} if parameters is None else parameters
        if not isinstance(parameters, dict):
            raise InvalidMessage('Service parameters must be an object')
        return self.handlers[operation](parameters)

    def fields(self, value, required=(), optional=()):
        return strict_object(value, set(required) | set(optional), set(required), 'parameters')

    def issuer_add(self, p):
        self.fields(p, ('issuer', 'display_name'), ('actor',))
        return issuers.add(self.store, **p)

    def issuer_list(self, p):
        self.fields(p)
        return issuers.listing(self.store)

    def issuer_enable(self, p):
        self.fields(p, ('issuer', 'enabled', 'revision'), ('actor',))
        return issuers.set_enabled(self.store, **p)

    def issuer_progress(self, p):
        self.fields(p, ('issuer',))
        return issuers.progress(self.store, **p)

    def event_receive(self, p):
        self.fields(p, ('message',), ('drain', 'limit'))
        if 'drain' in p and not isinstance(p['drain'], bool):
            raise InvalidMessage('drain must be boolean')
        return receiver.receive(self.store, p['message'], drain=p.get('drain', True), limit=p.get('limit', 1000))

    def event_get(self, p):
        self.fields(p, ('event_id',))
        result = receiver.event(self.store, **p)
        if result is None:
            from registry_bridge.errors import NotFound
            raise NotFound('Event does not exist')
        return result

    def event_pending(self, p):
        self.fields(p, ('issuer',), ('limit',))
        return receiver.pending(self.store, **p)

    def event_drain(self, p):
        self.fields(p, ('issuer',), ('limit',))
        return receiver.drain_stream(self.store, **p)

    def event_skip(self, p):
        self.fields(p, ('event_id', 'reason'), ('actor',))
        return resolution.skip_rejected(self.store, **p)

    def import_feed(self, p):
        self.fields(p, ('content', 'source'), ('format', 'atomic'))
        if not isinstance(p['content'], str):
            raise InvalidMessage('Feed content must be text')
        if 'atomic' in p and not isinstance(p['atomic'], bool):
            raise InvalidMessage('atomic must be boolean')
        return importer.import_bytes(
            self.store, p['content'].encode('utf-8'), p['source'],
            format=p.get('format', 'ndjson'), atomic=p.get('atomic', False),
        )

    def import_history(self, p):
        self.fields(p, (), ('limit',))
        return importer.import_history(self.store, **p)

    def deadletter_list(self, p):
        self.fields(p, (), ('unreplayed', 'limit'))
        return importer.list_dead_letters(self.store, **p)

    def deadletter_replay(self, p):
        self.fields(p, ('dead_id', 'corrected'), ('actor',))
        return importer.replay_dead_letter(self.store, **p)

    def asset_get(self, p):
        self.fields(p, ('issuer', 'asset_id'), ('instant', 'revision'))
        return catalog.asset(self.store, **p)

    def asset_list(self, p):
        self.fields(p, (), ('issuer', 'holder', 'state', 'limit', 'cursor', 'instant'))
        return catalog.catalog(self.store, **p)

    def asset_timeline(self, p):
        self.fields(p, ('issuer', 'asset_id'))
        return catalog.timeline(self.store, **p)

    def asset_diff(self, p):
        self.fields(p, ('issuer', 'asset_id', 'before', 'after'))
        return catalog.compare_revisions(self.store, **p)

    def asset_authorize(self, p):
        self.fields(p, ('issuer', 'asset_id', 'purpose', 'region', 'right', 'instant'))
        return catalog.authorize(self.store, **p)

    def conflict_list(self, p):
        self.fields(p, (), ('issuer', 'limit'))
        return resolution.open_conflicts(self.store, **p)

    def conflict_get(self, p):
        self.fields(p, ('conflict_id',))
        return resolution.conflict(self.store, **p)

    def conflict_close(self, p):
        self.fields(p, ('conflict_id', 'revision', 'reason'), ('actor',))
        return resolution.close_conflict(self.store, **p)

    def stream_gaps(self, p):
        self.fields(p, ('issuer',), ('ceiling',))
        return resolution.gaps(self.store, **p)

    def stream_blockage(self, p):
        self.fields(p, ('issuer',))
        return resolution.explain_blockage(self.store, **p)

    def dataset_claims(self, p):
        self.fields(p, ('fingerprint',), ('instant',))
        return cross_registry.dataset_claims(self.store, **p)

    def dataset_scan(self, p):
        self.fields(p, (), ('limit', 'after'))
        return cross_registry.scan(self.store, **p)

    def consumer_checkpoint(self, p):
        self.fields(p, ('consumer',))
        return consumer.checkpoint(self.store, **p)

    def consumer_changes(self, p):
        self.fields(p, (), ('after', 'limit'))
        return consumer.changes(self.store, **p)

    def consumer_advance(self, p):
        self.fields(p, ('consumer', 'position', 'revision'), ('token',))
        return consumer.advance(self.store, **p)

    def consumer_export(self, p):
        self.fields(p, ('consumer',), ('limit',))
        return consumer.create_export(self.store, **p)

    def consumer_acknowledge(self, p):
        self.fields(p, ('export_id', 'sha256', 'revision'), ('token',))
        return consumer.acknowledge_export(self.store, **p)

    def consumer_lag(self, p):
        self.fields(p)
        return consumer.lag(self.store)

    def lease_acquire(self, p):
        self.fields(p, ('name', 'owner'), ('ttl',))
        return leases.acquire(self.store, **p)

    def lease_renew(self, p):
        self.fields(p, ('token',), ('ttl',))
        return leases.renew(self.store, **p)

    def lease_release(self, p):
        self.fields(p, ('token',))
        return leases.release(self.store, **p)

    def lease_list(self, p):
        self.fields(p)
        return leases.list_leases(self.store)

    def audit_page(self, p):
        self.fields(p, (), ('after', 'limit'))
        return audit.page(self.store, **p)

    def audit_verify(self, p):
        self.fields(p)
        return audit.verify(self.store)

    def system_check(self, p):
        self.fields(p)
        return rebuild.check(self.store)

    def system_rebuild(self, p):
        self.fields(p, (), ('actor',))
        return rebuild.rebuild(self.store, **p)

    def system_metrics(self, p):
        self.fields(p)
        return catalog.metrics(self.store)

    def system_fingerprint(self, p):
        self.fields(p)
        return rebuild.fingerprint(self.store)

    def snapshot_create(self, p):
        self.fields(p, (), ('instant',))
        return exporting.snapshot(self.store, **p)

    def snapshot_verify(self, p):
        self.fields(p, ('snapshot',))
        return exporting.verify_snapshot(p['snapshot'])

    def snapshot_compare(self, p):
        self.fields(p, ('left', 'right'))
        return exporting.compare_snapshots(**p)

    def execute_batch(self, requests, *, atomic=True):
        if not isinstance(requests, list) or len(requests) > 1000:
            raise InvalidMessage('Batch must contain at most one thousand requests')
        results = []
        if atomic:
            with self.store.transaction():
                for request in requests:
                    strict_object(request, {'operation', 'parameters'}, {'operation'}, 'request')
                    results.append(self.call(request['operation'], request.get('parameters')))
        else:
            from registry_bridge.errors import RegistryError
            for request in requests:
                try:
                    strict_object(request, {'operation', 'parameters'}, {'operation'}, 'request')
                    result = self.call(request['operation'], request.get('parameters'))
                except RegistryError as exc:
                    results.append({'ok': False, 'error': exc.as_dict()})
                else:
                    results.append({'ok': True, 'result': result})
        return results
