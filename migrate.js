import fs from 'fs';
import { Pool } from 'pg';
import 'dotenv/config';

const sql = fs.readFileSync('./schema.sql','utf8');
const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl:{rejectUnauthorized:false} });

try {
  const c = await pool.connect();
  await c.query(sql);
  c.release();
  console.log('✅ schema applied');
} catch(e) {
  console.error('❌', e.message);
  process.exit(1);
} finally { process.exit(0); }
