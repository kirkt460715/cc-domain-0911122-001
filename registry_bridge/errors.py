class RegistryError(Exception):
    code = 'registry_error'

    def __init__(self, message, *, context=None):
        super().__init__(message)
        self.message = message
        self.context = dict(context or {})

    def as_dict(self):
        return {
            'code': self.code,
            'message': self.message,
            'context': self.context,
        }


class InvalidMessage(RegistryError):
    code = 'invalid_message'


class Conflict(RegistryError):
    code = 'conflict'


class NotFound(RegistryError):
    code = 'not_found'


class StaleRevision(Conflict):
    code = 'stale_revision'


class StreamBlocked(Conflict):
    code = 'stream_blocked'


class IntegrityFailure(RegistryError):
    code = 'integrity_failure'


class LeaseLost(Conflict):
    code = 'lease_lost'


class InvalidTransition(Conflict):
    code = 'invalid_transition'
