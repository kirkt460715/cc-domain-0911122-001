from registry_bridge.errors import InvalidMessage
from registry_bridge.feeds import bundle
from registry_bridge.feeds.contracts import ContractGate
from registry_bridge.reconcile import inspection, invariants
from registry_bridge.reconcile.worker import RecoveryQueue
from registry_bridge.query import knowledge


class Operations:
    def __init__(self, store):
        self.store = store
        self.recovery = RecoveryQueue(store)
        self.contracts = ContractGate(store)
        from registry_bridge.query.subscriptions import Subscriptions
        self.subscriptions = Subscriptions(store)

    def call(self, operation, p):
        if not isinstance(p, dict):
            raise InvalidMessage('Operation parameters must be an object')
        from registry_bridge.feeds.message import strict_object
        definitions = {
            'subscription.create': (self.subscriptions.create, {'name'}, {'issuers', 'holders', 'states', 'start'}),
            'subscription.configure': (self.subscriptions.configure, {'name', 'revision', 'issuers', 'holders', 'states'}, {'enabled'}),
            'subscription.get': (self.subscriptions.get, {'name'}, set()),
            'subscription.poll': (self.subscriptions.poll, {'name'}, {'limit'}),
            'subscription.acknowledge': (self.subscriptions.acknowledge, {'delivery_id', 'sha256'}, set()),
            'subscription.delivery': (self.subscriptions.delivery, {'delivery_id'}, set()),
            'subscription.list': (self.subscriptions.list, set(), set()),
            'subscription.backlog': (self.subscriptions.backlog, {'name'}, set()),
            'recovery.enqueue': (self.recovery.enqueue, {'job_id', 'issuer'}, {'priority', 'max_attempts'}),
            'recovery.get': (self.recovery.get, {'job_id'}, set()),
            'recovery.claim': (self.recovery.claim, {'owner'}, {'ttl'}),
            'recovery.heartbeat': (self.recovery.heartbeat, {'job_id', 'token'}, {'ttl'}),
            'recovery.execute': (self.recovery.execute, {'job_id', 'token'}, {'limit'}),
            'recovery.fail': (self.recovery.fail, {'job_id', 'token', 'error'}, set()),
            'recovery.recover': (self.recovery.recover_expired, set(), set()),
            'recovery.resume': (self.recovery.resume, {'job_id', 'revision'}, set()),
            'recovery.cancel': (self.recovery.cancel, {'job_id', 'revision', 'reason'}, set()),
            'recovery.list': (self.recovery.list, set(), {'state', 'limit'}),
            'contract.get': (self.contracts.get, {'issuer'}, set()),
            'contract.configure': (self.contracts.configure, {'issuer', 'revision', 'allowed_operations', 'required_evidence', 'allowed_regions'}, {'max_gap'}),
            'contract.receive': (self.contracts.receive, {'value'}, set()),
            'contract.rejections': (self.contracts.rejections, {'issuer'}, {'after', 'limit'}),
        }
        if operation in definitions:
            function, required, optional = definitions[operation]
            strict_object(p, required | optional, required, 'parameters')
            return function(**p)
        stored = {
            'invariants.check': (invariants.inspect_all, set(), set()),
            'invariants.incident': (invariants.incident_bundle, {'event_ids'}, set()),
            'knowledge.view': (knowledge.view_at_knowledge, {'issuer', 'asset_id', 'known_at', 'effective_at'}, set()),
            'knowledge.late': (knowledge.late_arrivals, {'start', 'end'}, {'threshold_seconds', 'limit'}),
            'knowledge.changed': (knowledge.changed_between, {'before', 'after'}, {'issuer'}),
            'knowledge.lifecycle': (knowledge.observed_lifecycle, {'issuer', 'asset_id'}, set()),
            'inspection.health': (inspection.health_report, set(), {'instant'}),
            'inspection.evidence': (inspection.evidence_index, {'sha256'}, set()),
            'inspection.holder': (inspection.holder_inventory, {'holder'}, {'instant'}),
            'inspection.revisions': (inspection.revision_gaps, {'issuer', 'asset_id'}, set()),
            'inspection.trace': (inspection.event_trace, {'event_id'}, set()),
            'inspection.expiry': (inspection.expiry_schedule, {'start', 'end'}, {'limit'}),
            'inspection.impact': (inspection.scope_change_impact, {'issuer', 'asset_id', 'before', 'after'}, set()),
            'bundle.create': (bundle.create_bundle, {'issuer', 'lower', 'upper'}, set()),
            'bundle.import': (bundle.import_bundle, {'value'}, {'allow_register', 'actor'}),
            'bundle.summary': (bundle.stream_summary, {'issuer'}, {'ceiling'}),
            'bundle.compare': (bundle.verify_remote_summary, {'issuer', 'summary'}, set()),
            'bundle.segments': (bundle.segments, {'issuer'}, {'width'}),
            'bundle.differences': (bundle.differing_ranges, {'issuer', 'remote_segments'}, set()),
        }
        if operation not in stored:
            raise InvalidMessage('Unknown operations command', context={'operation': operation})
        function, required, optional = stored[operation]
        strict_object(p, required | optional, required, 'parameters')
        return function(self.store, **p)
