/**
 * CobaltDB Node.js SDK
 *
 * Connect to CobaltDB server using the MySQL protocol.
 * Uses mysql2 package under the hood for best performance.
 *
 * Usage:
 *   const cobaltdb = require('cobaltdb-sdk');
 *   const conn = await cobaltdb.connect({ host: '127.0.0.1', port: 3307 });
 *   const [rows] = await conn.execute('SELECT * FROM users');
 *   console.log(rows);
 *   await conn.end();
 *
 * Requirements:
 *   npm install mysql2
 */

'use strict';

const VERSION = '0.3.0';

class CobaltDBError extends Error {
  constructor(message) {
    super(message);
    this.name = 'CobaltDBError';
  }
}

/**
 * Connect to a CobaltDB server.
 *
 * @param {Object} options - Connection options
 * @param {string} [options.host='127.0.0.1'] - Server hostname
 * @param {number} [options.port=3307] - MySQL protocol port
 * @param {string} [options.user='admin'] - Username
 * @param {string} [options.password=''] - Password
 * @param {string} [options.database=''] - Database name
 * @returns {Promise<Connection>} Connection object
 */
async function connect(options = {}) {
  const config = {
    host: options.host || '127.0.0.1',
    port: options.port || 3307,
    user: options.user || 'admin',
    password: options.password || '',
    database: options.database || '',
    ...options,
  };

  let mysql2;
  try {
    mysql2 = require('mysql2/promise');
  } catch (e) {
    throw new CobaltDBError(
      'mysql2 package not found. Install it with:\n  npm install mysql2'
    );
  }

  const conn = await mysql2.createConnection(config);
  return new Connection(conn);
}

/**
 * Create a connection pool.
 *
 * @param {Object} options - Pool options
 * @param {number} [options.connectionLimit=10] - Max connections
 * @returns {Pool} Connection pool
 */
function createPool(options = {}) {
  const config = {
    host: options.host || '127.0.0.1',
    port: options.port || 3307,
    user: options.user || 'admin',
    password: options.password || '',
    database: options.database || '',
    connectionLimit: options.connectionLimit || 10,
    waitForConnections: true,
    ...options,
  };

  let mysql2;
  try {
    mysql2 = require('mysql2/promise');
  } catch (e) {
    throw new CobaltDBError(
      'mysql2 package not found. Install it with:\n  npm install mysql2'
    );
  }

  const pool = mysql2.createPool(config);
  return new Pool(pool);
}

class Connection {
  constructor(conn) {
    this._conn = conn;
  }

  /**
   * Execute a SQL query with optional parameters.
   * @param {string} sql - SQL query
   * @param {Array} [params] - Query parameters
   * @returns {Promise<[Array, Array]>} [rows, fields]
   */
  async execute(sql, params) {
    return this._conn.execute(sql, params);
  }

  /**
   * Execute a SQL query (alias for execute).
   */
  async query(sql, params) {
    return this._conn.query(sql, params);
  }

  /**
   * Begin a transaction.
   */
  async beginTransaction() {
    return this._conn.beginTransaction();
  }

  /**
   * Commit the current transaction.
   */
  async commit() {
    return this._conn.commit();
  }

  /**
   * Rollback the current transaction.
   */
  async rollback() {
    return this._conn.rollback();
  }

  /**
   * Close the connection.
   */
  async end() {
    return this._conn.end();
  }

  /**
   * Destroy the connection immediately.
   */
  destroy() {
    this._conn.destroy();
  }
}

class Pool {
  constructor(pool) {
    this._pool = pool;
  }

  /**
   * Execute a query using a pool connection.
   */
  async execute(sql, params) {
    return this._pool.execute(sql, params);
  }

  /**
   * Get a connection from the pool.
   */
  async getConnection() {
    const conn = await this._pool.getConnection();
    return new Connection(conn);
  }

  /**
   * Close all connections in the pool.
   */
  async end() {
    return this._pool.end();
  }
}

module.exports = {
  connect,
  createPool,
  Connection,
  Pool,
  CobaltDBError,
  VERSION,
};
