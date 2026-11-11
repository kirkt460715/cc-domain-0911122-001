import argparse
import json
import sqlite3
import sys
from pathlib import Path

from registry_bridge.errors import RegistryError, InvalidMessage
from registry_bridge.feeds import bundle, importer
from registry_bridge.feeds.message import canonical, decode_json
from registry_bridge.ledger.storage import Store
from registry_bridge.query import exporting
from registry_bridge.reconcile import inspection
from registry_bridge.service import RegistryService


def parser():
    result = argparse.ArgumentParser(prog='registry-bridge', description='Offline registry exchange ledger')
    result.add_argument('--db', default='registry.sqlite3')
    commands = result.add_subparsers(dest='command', required=True)
    commands.add_parser('init')
    call = commands.add_parser('call')
    call.add_argument('operation')
    call.add_argument('--json', default='{}')
    call.add_argument('--input')
    batch = commands.add_parser('batch')
    batch.add_argument('path')
    batch.add_argument('--partial', action='store_true')
    ingest = commands.add_parser('import')
    ingest.add_argument('path')
    ingest.add_argument('--source', required=True)
    ingest.add_argument('--format', choices=('csv', 'ndjson'), default='ndjson')
    ingest.add_argument('--atomic', action='store_true')
    export = commands.add_parser('export')
    export.add_argument('path')
    export.add_argument('--format', choices=('json', 'csv'), default='json')
    export.add_argument('--at')
    transfer = commands.add_parser('bundle')
    transfer.add_argument('issuer')
    transfer.add_argument('lower', type=int)
    transfer.add_argument('upper', type=int)
    transfer.add_argument('path')
    accept = commands.add_parser('accept-bundle')
    accept.add_argument('path')
    accept.add_argument('--register', action='store_true')
    report = commands.add_parser('report')
    report.add_argument('--at')
    backup = commands.add_parser('backup')
    backup.add_argument('path')
    commands.add_parser('check')
    commands.add_parser('rebuild')
    demo = commands.add_parser('demo')
    demo.add_argument('--fixture', required=True)
    return result


def read_json(path, maximum=64 * 1024 * 1024):
    file = Path(path)
    if not file.is_file() or file.is_symlink():
        raise InvalidMessage('Input must be a regular file')
    if file.stat().st_size > maximum:
        raise InvalidMessage('Input exceeds size limit')
    try:
        return json.loads(file.read_text(encoding='utf-8'))
    except (ValueError, UnicodeError) as exc:
        raise InvalidMessage('Input must contain UTF-8 JSON') from exc


def dispatch(store, args):
    service = RegistryService(store)
    if args.command == 'init':
        return {'initialized': True, 'integrity': store.integrity()}
    if args.command == 'call':
        parameters = read_json(args.input) if args.input else decode_json(args.json)
        return service.call(args.operation, parameters)
    if args.command == 'batch':
        return service.execute_batch(read_json(args.path), atomic=not args.partial)
    if args.command == 'import':
        return importer.import_file(store, args.path, args.source, format=args.format, atomic=args.atomic)
    if args.command == 'export':
        return exporting.write_snapshot(store, args.path, format=args.format, instant=args.at)
    if args.command == 'bundle':
        value = bundle.create_bundle(store, args.issuer, args.lower, args.upper)
        result = exporting.atomic_write(args.path, canonical(value) + '\n')
        return {**result, 'bundle_sha256': value['sha256']}
    if args.command == 'accept-bundle':
        return bundle.import_bundle(store, read_json(args.path), allow_register=args.register)
    if args.command == 'report':
        return inspection.health_report(store, instant=args.at)
    if args.command == 'backup':
        return store.backup(args.path)
    if args.command == 'check':
        return service.call('system.check')
    if args.command == 'rebuild':
        return service.call('system.rebuild')
    if args.command == 'demo':
        return run_demo(store, read_json(args.fixture))
    raise InvalidMessage('Unknown command')


def run_demo(store, fixture):
    if not isinstance(fixture, dict) or set(fixture) != {'issuers', 'messages', 'expected'}:
        raise InvalidMessage('Demo fixture requires issuers, messages and expected')
    service = RegistryService(store)
    for issuer in fixture['issuers']:
        service.call('issuer.add', issuer)
    outcomes = []
    for message in fixture['messages']:
        outcomes.append(service.call('event.receive', {'message': message}))
    metrics = service.call('system.metrics')
    for key, value in fixture['expected'].items():
        if metrics.get(key) != value:
            raise InvalidMessage('Demo observable result differs from fixture expectation', context={
                'field': key, 'expected': value, 'actual': metrics.get(key),
            })
    check = service.call('system.check')
    if not check['ok']:
        raise InvalidMessage('Demo ledger integrity failure', context=check)
    snapshot = service.call('snapshot.create')
    verification = service.call('snapshot.verify', {'snapshot': snapshot})
    export = service.call('consumer.export', {'consumer': 'demo-reader'})
    acknowledgement = service.call('consumer.acknowledge', {
        'export_id': export['export_id'], 'sha256': export['sha256'],
        'revision': export['checkpoint_revision'],
    })
    return {
        'ok': True,
        'metrics': metrics,
        'outcomes': outcomes,
        'snapshot': verification,
        'consumer': acknowledgement,
        'integrity': check,
    }


def main(argv=None, stdout=None, stderr=None):
    stdout = stdout or sys.stdout
    stderr = stderr or sys.stderr
    args = parser().parse_args(argv)
    try:
        with Store(args.db) as store:
            result = dispatch(store, args)
        stdout.write(canonical({'ok': True, 'result': result}) + '\n')
        if isinstance(result, dict) and result.get('ok') is False:
            return 2
        return 0
    except RegistryError as exc:
        stderr.write(canonical({'ok': False, 'error': exc.as_dict()}) + '\n')
        return 2
    except (OSError, sqlite3.Error) as exc:
        stderr.write(canonical({'ok': False, 'error': {'code': 'storage_error', 'message': str(exc)}}) + '\n')
        return 3
