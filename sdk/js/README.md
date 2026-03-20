# CobaltDB Node.js SDK

Connect to CobaltDB server using the MySQL protocol.

## Installation

```bash
npm install mysql2
```

## Usage

```javascript
const cobaltdb = require('./sdk/js');

async function main() {
  // Connect to CobaltDB server
  const conn = await cobaltdb.connect({
    host: '127.0.0.1',
    port: 3307,
    user: 'admin',
  });

  // Create table
  await conn.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');

  // Insert data
  await conn.execute('INSERT INTO users VALUES (?, ?)', [1, 'Alice']);
  await conn.execute('INSERT INTO users VALUES (?, ?)', [2, 'Bob']);

  // Query data
  const [rows] = await conn.execute('SELECT * FROM users ORDER BY id');
  console.log(rows);

  await conn.end();
}

main().catch(console.error);
```

## Connection Pool

```javascript
const cobaltdb = require('./sdk/js');

const pool = cobaltdb.createPool({
  host: '127.0.0.1',
  port: 3307,
  user: 'admin',
  connectionLimit: 10,
});

// Execute queries directly on the pool
const [rows] = await pool.execute('SELECT * FROM users');

// Or get a dedicated connection
const conn = await pool.getConnection();
await conn.beginTransaction();
await conn.execute('INSERT INTO users VALUES (?, ?)', [3, 'Charlie']);
await conn.commit();
conn.release();

await pool.end();
```

## API

### `cobaltdb.connect(options)` → `Promise<Connection>`

| Option | Default | Description |
|--------|---------|-------------|
| `host` | `127.0.0.1` | Server hostname |
| `port` | `3307` | MySQL protocol port |
| `user` | `admin` | Username |
| `password` | `""` | Password |
| `database` | `""` | Database name |

### `cobaltdb.createPool(options)` → `Pool`

Same options as `connect`, plus `connectionLimit` (default: 10).
