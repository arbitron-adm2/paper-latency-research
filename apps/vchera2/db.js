'use strict';
// Only the strategy's own schema is created. No original journal.db or .env is opened.
const path = require('node:path');
const { DatabaseSync } = require('node:sqlite');
const { privateRuntime } = require('./runtime.cjs');
const db = new DatabaseSync(path.join(privateRuntime(), 'vchera2.sqlite'));
db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;');
module.exports = { db };
