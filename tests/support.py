import copy
import datetime as dt
import tempfile
import unittest
from pathlib import Path

from registry_bridge.ledger.storage import Store
from registry_bridge.ledger import issuers


class Clock:
    def __init__(self):
        self.now = dt.datetime(2026, 11, 11, tzinfo=dt.timezone.utc)

    def __call__(self):
        return self.now.isoformat(timespec='microseconds').replace('+00:00', 'Z')

    def tick(self, seconds=1):
        self.now += dt.timedelta(seconds=seconds)
        return self()


def registration(sequence=1, *, issuer='north', asset='asset1', event=None, holder='holder1'):
    return {
        'version': 1,
        'event_id': event or f'{issuer}:event{sequence}',
        'issuer': issuer,
        'sequence': sequence,
        'asset_id': asset,
        'revision': 1,
        'operation': 'register',
        'effective_at': '2026-11-01T00:00:00Z',
        'payload': {
            'title': 'Synthetic maintenance dataset',
            'holder': holder,
            'dataset_fingerprint': 'a' * 64,
            'scope': {
                'purposes': ['research', 'repair'],
                'regions': ['CN'],
                'rights': ['hold', 'process'],
                'expires_at': '2027-11-01T00:00:00Z',
            },
            'evidence': [{'kind': 'source_agreement', 'sha256': 'b' * 64}],
        },
    }


def transition(sequence, revision, operation, *, issuer='north', asset='asset1', changes=None):
    result = registration(sequence, issuer=issuer, asset=asset)
    result['revision'] = revision
    result['operation'] = operation
    result['effective_at'] = f'2026-11-{min(revision,28):02d}T00:00:00Z'
    result['payload'] = {'reason': 'Synthetic operator action'}
    if operation == 'amend':
        result['payload']['changes'] = changes or {'title': 'Revised synthetic dataset'}
    return result


class LedgerCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'ledger.db'
        self.clock = Clock()
        self.store = Store(self.path, clock=self.clock)
        issuers.add(self.store, 'north', 'Synthetic North')
        issuers.add(self.store, 'south', 'Synthetic South')

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()
