const tape = require('tape')
const _test = require('tape-promise').default;
const test = _test(tape)
const fs = require('fs');
const pg = require('pg');




async function initDb() {
    const conf = {
      user: 'admin',
      password: 'admin',
      host: 'localhost',
      database: 'eventstoretest',
      port: 5432
    }
    const db = new pg.Client(conf)
    db.connect()
    await loadDdl(db);
    return db;
}

async function shutdownDb(db) {
    await db.end();
  }


const db = await initDb();

const text = 'INSERT INTO users(name, email) VALUES($1, $2) RETURNING *'
const values = ['brianc', 'brian.m.carlson@gmail.com']


try {
    const res = await client.query(text, values)
    console.log(res.rows[0])
    // { name: 'brianc', email: 'brian.m.carlson@gmail.com' }
  } catch (err) {
    console.log(err.stack)
  }


await shutdownDb(db);