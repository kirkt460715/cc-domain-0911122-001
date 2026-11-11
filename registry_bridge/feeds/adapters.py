import csv
import io

from registry_bridge.errors import InvalidMessage
from registry_bridge.feeds.message import decode_json, integer, parse_message

CSV_FIELDS = (
    'event_id', 'issuer', 'sequence', 'asset_id', 'revision',
    'operation', 'effective_at', 'payload',
)


def adapt_envelope(value, source):
    if not isinstance(value, dict):
        raise InvalidMessage('Feed envelope must be an object')
    if set(value) != {'source', 'schema', 'messages'}:
        raise InvalidMessage('Feed envelope fields are invalid')
    if value['source'] != source:
        raise InvalidMessage('Feed envelope source mismatch')
    integer(value['schema'], 'schema', 1, 1)
    messages = value['messages']
    if not isinstance(messages, list) or len(messages) > 10000:
        raise InvalidMessage('Feed envelope messages must be a bounded list')
    normalized = []
    for index, item in enumerate(messages, 1):
        try:
            normalized.append(parse_message(item))
        except InvalidMessage as exc:
            raise InvalidMessage('Invalid envelope member', context={
                'index': index, 'error': exc.as_dict(),
            }) from exc
    return normalized


def ndjson_records(stream, *, max_bytes=1024 * 1024):
    line_number = 0
    while True:
        raw = stream.readline(max_bytes + 1)
        if not raw:
            break
        line_number += 1
        if len(raw) > max_bytes:
            while raw and not raw.endswith(b'\n'):
                raw = stream.readline(max_bytes + 1)
            yield line_number, None, InvalidMessage('Feed line exceeds size limit'), '[oversized line]'
            continue
        try:
            decoded = raw.decode('utf-8')
        except UnicodeDecodeError:
            yield line_number, None, InvalidMessage('Feed line is not UTF-8'), raw.hex()[:4096]
            continue
        if not decoded.strip():
            continue
        try:
            message = parse_message(decode_json(decoded))
        except InvalidMessage as exc:
            yield line_number, None, exc, decoded
        else:
            yield line_number, message, None, decoded


def csv_records(stream):
    reader = csv.DictReader(stream, strict=True)
    try:
        fields = reader.fieldnames
    except (csv.Error, UnicodeError) as exc:
        raise InvalidMessage('Cannot read CSV header') from exc
    if fields is None or len(fields) != len(set(fields)) or set(fields) != set(CSV_FIELDS):
        raise InvalidMessage('CSV header does not match exchange schema')
    try:
        for row in reader:
            line_number = reader.line_num
            raw = repr(row)
            try:
                if None in row or any(value is None for value in row.values()):
                    raise InvalidMessage('CSV row has incorrect number of fields')
                for field in ('sequence', 'revision'):
                    if not row[field].isascii() or not row[field].isdigit():
                        raise InvalidMessage('CSV integer field must contain decimal digits', context={'field': field})
                    row[field] = int(row[field])
                row['payload'] = decode_json(row['payload'])
                row['version'] = 1
                message = parse_message(row)
            except InvalidMessage as exc:
                yield line_number, None, exc, raw
            else:
                yield line_number, message, None, raw
    except (csv.Error, UnicodeError) as exc:
        raise InvalidMessage('Malformed CSV stream') from exc


def encode_csv(messages):
    output = io.StringIO(newline='')
    writer = csv.DictWriter(output, fieldnames=CSV_FIELDS, lineterminator='\n')
    writer.writeheader()
    for message in messages:
        row = message.as_dict()
        row.pop('version')
        row['payload'] = message.payload_json
        writer.writerow(row)
    return output.getvalue()
