import json

from registry_bridge.errors import Conflict, InvalidMessage, NotFound, StaleRevision
from registry_bridge.feeds.message import canonical, digest, identifier, integer, string_set
from registry_bridge.ledger import audit
from registry_bridge.query import consumer

DDL = '''
CREATE TABLE IF NOT EXISTS subscriptions(
 name TEXT PRIMARY KEY, revision INTEGER NOT NULL, issuers TEXT NOT NULL,
 holders TEXT NOT NULL, states TEXT NOT NULL, position INTEGER NOT NULL,
 enabled INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS deliveries(
 delivery_id TEXT PRIMARY KEY, subscription TEXT NOT NULL REFERENCES subscriptions(name),
 subscription_revision INTEGER NOT NULL, lower_position INTEGER NOT NULL,
 upper_position INTEGER NOT NULL, payload TEXT NOT NULL, digest TEXT NOT NULL,
 state TEXT NOT NULL, attempts INTEGER NOT NULL, created_at TEXT NOT NULL, acknowledged_at TEXT);
CREATE INDEX IF NOT EXISTS deliveries_pending ON deliveries(subscription,state,created_at);
'''


def optional_set(value, field, allowed=None):
    if value == []:
        return []
    return string_set(value, field, allowed=allowed)


class Subscriptions:
    def __init__(self, store):
        self.store = store
        if store._depth:
            raise InvalidMessage('Subscriptions must initialize outside a transaction')
        store.connection.executescript(DDL)

    def get(self, name):
        identifier(name, 'subscription')
        row = self.store.one('SELECT * FROM subscriptions WHERE name=?', (name,))
        if row is None:
            raise NotFound('Subscription does not exist')
        for key in ('issuers', 'holders', 'states'):
            row[key] = json.loads(row[key])
        row['enabled'] = bool(row['enabled'])
        return row

    def create(self, name, *, issuers=None, holders=None, states=None, start=0):
        identifier(name, 'subscription')
        issuer_set = optional_set([] if issuers is None else issuers, 'issuers')
        holder_set = optional_set([] if holders is None else holders, 'holders')
        state_set = optional_set([] if states is None else states, 'states', {'active', 'suspended', 'revoked'})
        integer(start, 'start', 0)
        with self.store.transaction():
            if start > consumer.stable_ceiling(self.store):
                raise Conflict('Subscription starts beyond stable ledger')
            previous = self.store.one('SELECT name FROM subscriptions WHERE name=?', (name,))
            if previous:
                raise Conflict('Subscription already exists')
            now = self.store.clock()
            self.store.execute(
                'INSERT INTO subscriptions(name,revision,issuers,holders,states,position,enabled,created_at,updated_at) '
                'VALUES(?,1,?,?,?,?,1,?,?)',
                (name, canonical(issuer_set), canonical(holder_set), canonical(state_set), start, now, now),
            )
            audit.append(self.store, 'subscription.created', 'operator', {'name': name, 'start': start})
            return self.get(name)

    def configure(self, name, revision, *, issuers, holders, states, enabled=True):
        integer(revision, 'revision')
        issuer_set = optional_set(issuers, 'issuers')
        holder_set = optional_set(holders, 'holders')
        state_set = optional_set(states, 'states', {'active', 'suspended', 'revoked'})
        if not isinstance(enabled, bool):
            raise InvalidMessage('Subscription enabled must be boolean')
        with self.store.transaction():
            previous = self.get(name)
            if previous['revision'] != revision:
                raise StaleRevision('Subscription configuration changed')
            if self.store.scalar("SELECT COUNT(*) FROM deliveries WHERE subscription=? AND state='pending'", (name,)):
                raise Conflict('Pending delivery must be acknowledged before configuration changes')
            self.store.execute(
                'UPDATE subscriptions SET revision=revision+1,issuers=?,holders=?,states=?,enabled=?,updated_at=? WHERE name=?',
                (canonical(issuer_set), canonical(holder_set), canonical(state_set), int(enabled), self.store.clock(), name),
            )
            audit.append(self.store, 'subscription.configured', 'operator', {'name': name, 'revision': revision + 1})
            return self.get(name)

    def matches(self, config, snapshot):
        if snapshot is None:
            return False
        if config['issuers'] and snapshot['issuer'] not in config['issuers']:
            return False
        if config['holders'] and snapshot['holder'] not in config['holders']:
            return False
        if config['states'] and snapshot['state'] not in config['states']:
            return False
        return True

    def transform(self, config, row):
        if row['asset'] is None:
            return None
        new = row['asset']
        old_row = self.store.one(
            'SELECT snapshot FROM history WHERE issuer=? AND asset_id=? AND revision=?',
            (new['issuer'], new['asset_id'], new['revision'] - 1),
        )
        old = json.loads(old_row['snapshot']) if old_row else None
        was_visible = self.matches(config, old)
        is_visible = self.matches(config, new)
        if not was_visible and not is_visible:
            return None
        action = 'upsert' if is_visible else 'remove'
        return {
            'position': row['position'],
            'event_id': row['event_id'],
            'action': action,
            'issuer': new['issuer'],
            'asset_id': new['asset_id'],
            'revision': new['revision'],
            'asset': new if is_visible else None,
        }

    def poll(self, name, *, limit=100):
        integer(limit, 'limit', 1, 10000)
        with self.store.transaction():
            config = self.get(name)
            if not config['enabled']:
                raise Conflict('Subscription is disabled')
            previous = self.store.one(
                "SELECT delivery_id FROM deliveries WHERE subscription=? AND state='pending' ORDER BY created_at LIMIT 1",
                (name,),
            )
            if previous:
                self.store.execute('UPDATE deliveries SET attempts=attempts+1 WHERE delivery_id=?', (previous['delivery_id'],))
                return self.delivery(previous['delivery_id'])
            batch = consumer.changes(self.store, config['position'], limit)
            messages = []
            for row in batch['rows']:
                transformed = self.transform(config, row)
                if transformed is not None:
                    messages.append(transformed)
            payload = {
                'subscription': name,
                'revision': config['revision'],
                'lower': config['position'],
                'upper': batch['next_position'],
                'messages': messages,
            }
            sha = digest(payload)
            delivery_id = digest({'subscription': name, 'sha256': sha})
            existing = self.store.one('SELECT delivery_id FROM deliveries WHERE delivery_id=?', (delivery_id,))
            if existing:
                return self.delivery(delivery_id)
            self.store.execute(
                'INSERT INTO deliveries(delivery_id,subscription,subscription_revision,lower_position,upper_position,'
                "payload,digest,state,attempts,created_at) VALUES(?,?,?,?,?,?,?,'pending',1,?)",
                (delivery_id, name, config['revision'], config['position'], batch['next_position'],
                 canonical(payload), sha, self.store.clock()),
            )
            return self.delivery(delivery_id)

    def delivery(self, delivery_id):
        row = self.store.one('SELECT * FROM deliveries WHERE delivery_id=?', (delivery_id,))
        if row is None:
            raise NotFound('Delivery does not exist')
        row['payload'] = json.loads(row['payload'])
        return row

    def acknowledge(self, delivery_id, sha256):
        with self.store.transaction():
            delivery = self.delivery(delivery_id)
            if delivery['digest'] != sha256:
                raise Conflict('Delivery acknowledgement checksum mismatch')
            if delivery['state'] == 'acknowledged':
                return {'status': 'already_acknowledged', 'subscription': self.get(delivery['subscription'])}
            config = self.get(delivery['subscription'])
            if config['revision'] != delivery['subscription_revision']:
                raise StaleRevision('Subscription configuration changed')
            if config['position'] != delivery['lower_position']:
                raise Conflict('Delivery starts behind subscription cursor')
            self.store.execute(
                'UPDATE subscriptions SET position=?,updated_at=? WHERE name=?',
                (delivery['upper_position'], self.store.clock(), config['name']),
            )
            self.store.execute(
                "UPDATE deliveries SET state='acknowledged',acknowledged_at=? WHERE delivery_id=?",
                (self.store.clock(), delivery_id),
            )
            audit.append(self.store, 'delivery.acknowledged', config['name'], {'delivery_id': delivery_id, 'position': delivery['upper_position']})
            return {'status': 'acknowledged', 'subscription': self.get(config['name'])}

    def list(self):
        rows = self.store.all('SELECT name FROM subscriptions ORDER BY name')
        return [self.get(row['name']) for row in rows]

    def backlog(self, name):
        config = self.get(name)
        pending = self.store.all(
            "SELECT delivery_id,lower_position,upper_position,attempts,created_at FROM deliveries "
            "WHERE subscription=? AND state='pending' ORDER BY created_at,delivery_id", (name,),
        )
        ceiling = consumer.stable_ceiling(self.store)
        return {
            'subscription': name,
            'position': config['position'],
            'stable_ceiling': ceiling,
            'unscanned_positions': ceiling - config['position'],
            'pending': pending,
        }
