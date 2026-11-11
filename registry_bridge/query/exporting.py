import csv
import hashlib
import io
import json
import os
import tempfile
from pathlib import Path

from registry_bridge.errors import Conflict, IntegrityFailure, InvalidMessage
from registry_bridge.feeds.message import canonical, digest, integer, strict_object
from registry_bridge.query.catalog import catalog


def atomic_write(destination, content, *, overwrite=False):
    target = Path(destination)
    if target.is_symlink():
        raise InvalidMessage('Output cannot be a symlink')
    if target.exists() and not overwrite:
        raise Conflict('Output already exists')
    if not target.parent.is_dir():
        raise InvalidMessage('Output parent directory does not exist')
    raw = content.encode('utf-8') if isinstance(content, str) else content
    handle, temporary = tempfile.mkstemp(prefix='.' + target.name + '.', dir=target.parent)
    try:
        with os.fdopen(handle, 'wb') as output:
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
        if overwrite:
            os.replace(temporary, target)
        else:
            os.link(temporary, target)
            os.unlink(temporary)
        directory = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return {'path': str(target), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}


def snapshot(store, *, instant=None):
    with store.transaction(immediate=False):
        assets = []
        cursor = None
        while True:
            page = catalog(store, limit=1000, cursor=cursor, instant=instant)
            assets.extend(page['items'])
            cursor = page['next_cursor']
            if cursor is None:
                break
        positions = store.all('SELECT issuer,last_sequence FROM issuers ORDER BY issuer')
        payload = {
            'format': 'registry-snapshot',
            'version': 1,
            'effective_at': instant,
            'cursors': positions,
            'assets': assets,
        }
        return {'manifest': payload, 'sha256': digest(payload), 'count': len(assets)}


def verify_snapshot(value):
    strict_object(value, {'manifest', 'sha256', 'count'}, {'manifest', 'sha256', 'count'}, 'snapshot')
    manifest = value['manifest']
    strict_object(
        manifest,
        {'format', 'version', 'effective_at', 'cursors', 'assets'},
        {'format', 'version', 'effective_at', 'cursors', 'assets'},
        'manifest',
    )
    if manifest['format'] != 'registry-snapshot' or manifest['version'] != 1:
        raise InvalidMessage('Unsupported snapshot format')
    if not isinstance(manifest['assets'], list) or not isinstance(manifest['cursors'], list):
        raise InvalidMessage('Snapshot collections must be lists')
    integer(value['count'], 'count', 0)
    if value['count'] != len(manifest['assets']):
        raise IntegrityFailure('Snapshot count mismatch')
    if digest(manifest) != value['sha256']:
        raise IntegrityFailure('Snapshot digest mismatch')
    seen = set()
    for asset in manifest['assets']:
        if not isinstance(asset, dict) or not {'issuer', 'asset_id', 'revision'} <= set(asset):
            raise InvalidMessage('Malformed snapshot asset')
        key = (asset['issuer'], asset['asset_id'])
        if key in seen:
            raise IntegrityFailure('Duplicate snapshot asset')
        seen.add(key)
    return {'ok': True, 'sha256': value['sha256'], 'count': value['count']}


def to_csv(value):
    verify_snapshot(value)
    output = io.StringIO(newline='')
    fields = ['issuer', 'asset_id', 'revision', 'state', 'title', 'holder', 'dataset_fingerprint', 'effective_at', 'expires_at']
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator='\n')
    writer.writeheader()
    for asset in value['manifest']['assets']:
        row = {field: asset[field] for field in fields if field != 'expires_at'}
        row['expires_at'] = asset['scope']['expires_at']
        for key, item in row.items():
            if isinstance(item, str) and item.startswith(('=', '+', '-', '@', '\t', '\r')):
                row[key] = "'" + item
        writer.writerow(row)
    return output.getvalue()


def write_snapshot(store, path, *, format='json', instant=None):
    value = snapshot(store, instant=instant)
    if format == 'json':
        content = canonical(value) + '\n'
    elif format == 'csv':
        content = to_csv(value)
    else:
        raise InvalidMessage('Unsupported export format')
    result = atomic_write(path, content)
    return {**result, 'count': value['count'], 'snapshot_sha256': value['sha256']}


def read_snapshot(path):
    file = Path(path)
    if not file.is_file() or file.stat().st_size > 128 * 1024 * 1024:
        raise InvalidMessage('Snapshot is missing or too large')
    try:
        value = json.loads(file.read_text(encoding='utf-8'))
    except (ValueError, UnicodeError) as exc:
        raise InvalidMessage('Snapshot is not valid UTF-8 JSON') from exc
    verify_snapshot(value)
    return value


def compare_snapshots(left, right):
    verify_snapshot(left)
    verify_snapshot(right)
    before = {(a['issuer'], a['asset_id']): a for a in left['manifest']['assets']}
    after = {(a['issuer'], a['asset_id']): a for a in right['manifest']['assets']}
    rows = []
    for key in sorted(set(before) | set(after)):
        old = before.get(key)
        new = after.get(key)
        if old == new:
            continue
        kind = 'added' if old is None else 'removed' if new is None else 'changed'
        rows.append({'issuer': key[0], 'asset_id': key[1], 'kind': kind, 'before': old, 'after': new})
    return {'changes': rows, 'count': len(rows), 'left': left['sha256'], 'right': right['sha256']}
