import json
import urllib.error
import urllib.parse
import urllib.request

from registry_bridge.errors import InvalidMessage, RegistryError
from registry_bridge.feeds.message import integer, text


class ViewExpired(RegistryError):
    """The exchange no longer serves the frozen epoch/revision pair."""

    code = 'view_expired'


class ExchangeUnavailable(RegistryError):
    """The exchange could not be reached or answered outside the protocol."""

    code = 'exchange_unavailable'


class ExchangeClient:
    """Read-only HTTP client for the registry exchange protocol.

    Every call is counted so a run can report and enforce a request budget.
    A whole reconciliation run must speak one frozen (epoch, revision) view;
    a 410 answer is surfaced as :class:`ViewExpired` instead of being retried
    against a newer view.
    """

    def __init__(self, base_url, *, timeout=5.0):
        self.base_url = text(base_url, 'base_url', 512).rstrip('/')
        if not (self.base_url.startswith('http://') or self.base_url.startswith('https://')):
            raise InvalidMessage('Exchange base URL must use http or https')
        self.timeout = timeout
        self.requests_used = 0

    def _open(self, path, params, body=None):
        query = urllib.parse.urlencode(params)
        request = urllib.request.Request(
            self.base_url + path + '?' + query,
            data=json.dumps(body).encode() if body is not None else None,
            headers={'Content-Type': 'application/json'},
        )
        self.requests_used += 1
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.loads(response.read().decode('utf-8'))
        except urllib.error.HTTPError as exc:
            if exc.code == 410:
                raise ViewExpired(
                    'Exchange view has expired',
                    context={'status': exc.code, 'path': path, 'params': params},
                ) from exc
            detail = exc.read().decode('utf-8', 'replace')
            raise ExchangeUnavailable(
                'Exchange rejected the request',
                context={'status': exc.code, 'path': path, 'detail': detail[:500]},
            ) from exc
        except urllib.error.URLError as exc:
            raise ExchangeUnavailable(
                'Exchange could not be reached', context={'reason': str(exc.reason)}
            ) from exc
        except (ValueError, UnicodeError) as exc:
            raise ExchangeUnavailable('Exchange returned malformed JSON') from exc

    def view(self):
        return self._open('/view', {})

    def summary(self, epoch, revision, lower, upper):
        integer(lower, 'lower', 1)
        integer(upper, 'upper', lower)
        return self._open('/summary', {
            'epoch': epoch, 'revision': revision, 'lower': lower, 'upper': upper,
        })

    def bundle(self, epoch, revision, lower, upper):
        integer(lower, 'lower', 1)
        integer(upper, 'upper', lower)
        return self._open('/bundle', {
            'epoch': epoch, 'revision': revision, 'lower': lower, 'upper': upper,
        })
