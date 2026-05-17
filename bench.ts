import sql from 'mssql'
import {rmSync} from "node:fs"
import {DatabaseSync} from "node:sqlite"
import { Pool as PgPool } from 'pg'
import { readFileSync } from 'fs'
import { randomUUID } from 'crypto'

// Connection defaults (adjust if needed)
const SQL_SERVER_CONFIG = {
  server: 'localhost',
  port: 1433,
  database: 'eventstore',
  user: 'sa',
  password: 'EventStore!2025',
  options: {
    encrypt: false,
    trustServerCertificate: true,
    enableArithAbort: true
  }
}

const POSTGRES_CONFIG = {
  host: 'localhost',
  port: 5432,
  database: 'eventstore',
  user: 'postgres',
  password: 'EventStore!pg'
}

const SQLITE_PATH = 'sqlite-store.db'

interface BenchmarkResult {
  backend: string
  inserted: number
  readCount: number
  insertMs: number
  readMs: number
}

function newEntityKey(): string {
  return `bench-${randomUUID().replace(/-/g, '')}`
}

function buildPayload(): string {
  const s1 = 'A'.repeat(180)
  const s2 = 'B'.repeat(160)
  const s3 = 'C'.repeat(140)
  const price = 12345.67
  const quantity = 42
  return JSON.stringify({ s1, s2, s3, price, qty: quantity })
}

const PAYLOAD = buildPayload()

async function runSqlServer(count: number): Promise<BenchmarkResult> {
  const entity = 'bench'
  const entityKey = newEntityKey()

  const pool = await sql.connect(SQL_SERVER_CONFIG)

  const swInsert = performance.now()
  let previousId: string | null = null

  for (let i = 0; i < count; i++) {
    const request = pool.request()
    request.input('entity', sql.NVarChar, entity)
    request.input('entity_key', sql.NVarChar, entityKey)
    request.input('event', sql.NVarChar, 'bench-event')
    request.input('data', sql.NVarChar, PAYLOAD)
    request.input('append_key', sql.NVarChar, randomUUID())
    request.input('previous_id', sql.UniqueIdentifier, previousId)
    request.output('event_id', sql.UniqueIdentifier)

    const result = await request.execute('append_event')
    const eventId = result.output.event_id
    previousId = eventId ? eventId.toString() : null
  }
  const insertMs = Math.round(performance.now() - swInsert)

  const swRead = performance.now()
  const readResult = await pool.request()
    .input('entity', sql.NVarChar, entity)
    .input('entity_key', sql.NVarChar, entityKey)
    .query('SELECT COUNT(*) as count FROM replay_events WHERE entity = @entity AND entity_key = @entity_key')

  const readCount = readResult.recordset[0].count
  const readMs = Math.round(performance.now() - swRead)

  await pool.close()

  return {
    backend: 'sql-server',
    inserted: count,
    readCount,
    insertMs,
    readMs
  }
}

async function runPostgres(count: number): Promise<BenchmarkResult> {
  const entity = 'bench'
  const entityKey = newEntityKey()

  const pool = new PgPool(POSTGRES_CONFIG)

  const swInsert = performance.now()
  let previousId: string | null = null

  for (let i = 0; i < count; i++) {
    const appendKey = randomUUID()
    const result = await pool.query(
      'SELECT append_event($1, $2, $3, $4::jsonb, $5, $6) as event_id',
      [entity, entityKey, 'bench-event', PAYLOAD, appendKey, previousId]
    )
    previousId = result.rows[0].event_id
  }
  const insertMs = Math.round(performance.now() - swInsert)

  const swRead = performance.now()
  const readResult = await pool.query(
    'SELECT COUNT(*) as count FROM replay_events WHERE entity = $1 AND entity_key = $2',
    [entity, entityKey]
  )
  const readCount = parseInt(readResult.rows[0].count)
  const readMs = Math.round(performance.now() - swRead)

  await pool.end()

  return {
    backend: 'postgresql',
    inserted: count,
    readCount,
    insertMs,
    readMs
  }
}

function runSqlite(totalCount: number): BenchmarkResult {
  const entity = 'bench'
  const entityKey = newEntityKey()

  // Load SQLite database
  rmSync(SQLITE_PATH)
  const db = new DatabaseSync(SQLITE_PATH)
  const ddl = readFileSync('sqlite-event-store.ddl', 'utf-8')
  db.exec(ddl)

  const swInsert = performance.now()
  let previousId: string | null = null
    const appendStmt = db.prepare(`
      INSERT INTO append_event (entity, entity_key, event, data, append_key, previous_id)
      VALUES (?1, ?2, ?3, ?4, ?5, ?6) 
      RETURNING (SELECT event_id FROM ledger WHERE append_key = ?5) AS event_id`)
  const countStmt = db.prepare(`SELECT COUNT(*) as count FROM replay_events WHERE entity = ? AND entity_key = ?`)

  for (let i = 0; i < totalCount; i++) {
    const appendKey = randomUUID()
    const {event_id} = appendStmt.get(entity, entityKey, 'bench-event', PAYLOAD, appendKey, previousId)
    previousId = event_id
  }
  const insertMs = Math.round(performance.now() - swInsert)

  const swRead = performance.now()
  const {count} = countStmt.get(entity, entityKey)
  const readMs = Math.round(performance.now() - swRead)

  // Save database
  db.close()

  return {
    backend: 'sqlite',
    inserted: totalCount,
    readCount: count,
    insertMs,
    readMs
  }
}

function parseArgs(args: string[]): { backend?: string; count?: number } {
  let backend: string | undefined
  let count: number | undefined

  for (let i = 0; i < args.length; i++) {
    const arg = args[i]
    if (arg === '--backend' && i + 1 < args.length) {
      const backendArg = args[++i]
      if (backendArg === 'sql-server' || backendArg === 'postgresql' || backendArg === 'sqlite') {
        backend = backendArg
      } else {
        console.error(`Unknown backend: ${backendArg}. Use sql-server, postgresql, or sqlite.`)
        process.exit(1)
      }
    } else if (arg === '--count' && i + 1 < args.length) {
      const countArg = parseInt(args[++i])
      if (!isNaN(countArg) && countArg > 0) {
        count = countArg
      }
    }
  }

  if (!backend && !count && args.length > 0) {
    console.warn('Unknown args ignored. Use --backend (sql-server|postgresql|sqlite) and --count N')
  }

  if (backend === undefined && args.some(a => a === '--backend')) {
    // --backend was provided but with invalid value, error already shown above
    process.exit(1)
  }

  return { backend, count }
}

function printTable(result: BenchmarkResult): void {
  const maxLen = (str: string) => str.length
  const pad = (str: string, len: number) => str.padEnd(len)

  const headers = ['Backend', 'Inserted', 'Read Count', 'Insert ms', 'Read ms']
  const values = [
    result.backend,
    result.inserted.toString(),
    result.readCount.toString(),
    result.insertMs.toString(),
    result.readMs.toString()
  ]

  const colWidths = headers.map((h, i) => Math.max(h.length, values[i].length) + 2)

  // Print header
  console.log('┌' + colWidths.map(w => '─'.repeat(w)).join('┬') + '┐')
  console.log('│' + headers.map((h, i) => pad(h, colWidths[i])).join('│') + '│')
  console.log('├' + colWidths.map(w => '─'.repeat(w)).join('┼') + '┤')
  console.log('│' + values.map((v, i) => pad(v, colWidths[i])).join('│') + '│')
  console.log('└' + colWidths.map(w => '─'.repeat(w)).join('┴') + '┘')
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2))

  console.log('Event Store Benchmark\n')

  let target: string
  if (args.backend) {
    target = args.backend
  } else {
    // Simple prompt (no interactive selection like Spectre.Console)
    const readline = await import('readline/promises')
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout
    })

    const answer = await rl.question('Choose backend (sql-server/postgresql/sqlite): ')
    const backendInput = answer.trim()
    if (backendInput === 'sql-server' || backendInput === 'postgresql' || backendInput === 'sqlite') {
      target = backendInput
    } else {
      console.error(`Unknown backend: ${backendInput}. Use sql-server, postgresql, or sqlite.`)
      rl.close()
      process.exit(1)
    }
    rl.close()
  }

  let eventCount: number
  if (args.count) {
    eventCount = args.count
  } else {
    const readline = await import('readline/promises')
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout
    })

    const answer = await rl.question('How many events? ')
    const count = parseInt(answer.trim())
    if (isNaN(count) || count <= 0) {
      console.error('Must be > 0')
      process.exit(1)
    }
    eventCount = count
    rl.close()
  }

  console.log(`Running ${eventCount} events on ${target}...\n`)

  let result: BenchmarkResult
  switch (target) {
    case 'sql-server':
      result = await runSqlServer(eventCount)
      break
    case 'postgresql':
      result = await runPostgres(eventCount)
      break
    case 'sqlite':
      result = await runSqlite(eventCount)
      break
    default:
      console.error(`Unknown backend: ${target}`)
      process.exit(1)
  }

  printTable(result)
}

main().catch(err => {
  console.error('Error:', err)
  process.exit(1)
})

