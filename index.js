import express from "express";
import { Pool } from "pg";
import cors from "cors";
import bodyParser from "body-parser";
import { parse as parseCsv } from "csv-parse/sync";
import 'dotenv/config';
import fs from "fs";
import path from "path";
import { parse as parseCsv } from "csv-parse/sync";


const meterMap = JSON.parse(
  fs.readFileSync(path.resolve("./meters.json"), "utf8")
);

const app = express();
app.use(cors({ origin: "*" }));
app.use(bodyParser.text({ type: ['text/*','text/csv','application/csv','application/octet-stream'], limit:'5mb' }));
app.use(express.json({ limit:'5mb' }));

const pool = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });

app.get("/health", async (_req,res) => {
  try { await pool.query('select 1'); res.json({ ok:true }); }
  catch(e){ res.status(500).json({ ok:false, error:'db_unavailable' }); }
});

app.post("/ingest", async (req, res) => {
  try {
    const ct = (req.header("content-type") || "").toLowerCase();
    const site_id = req.query.site || "LNT-GreatYarmouth"; // or pass ?site= in URL
    const config = meterMap[site_id];
    if (!config) return res.status(400).json({ error: "unknown_site" });

    const text = typeof req.body === "string" ? req.body : "";
    const parsed = parseCsv(text, { columns: true, skip_empty_lines: true });

    const rows = [];
    for (const r of parsed) {
      const ts = r.timestamp;
      for (const [col, meta] of Object.entries(config)) {
        if (r[col] === undefined || r[col] === "") continue;
        rows.push({
          site_id,
          meter_id: meta.meter_id,
          type: meta.type,
          unit: meta.unit,
          ts,
          value: Number(r[col])
        });
      }
    }

    if (!rows.length) return res.status(400).json({ error: "empty_payload" });

    const client = await pool.connect();
    try {
      await client.query("begin");

      // Ensure site exists
      await client.query(
        `insert into sites (site_code,name) values ($1,$1) on conflict do nothing`,
        [site_id]
      );

      // Upsert meters
      for (const [col, meta] of Object.entries(config)) {
        await client.query(
          `insert into meters (site_code,meter_id,type,unit)
           values ($1,$2,$3,$4)
           on conflict (site_code,meter_id) do update set type=excluded.type,unit=excluded.unit`,
          [site_id, meta.meter_id, meta.type, meta.unit]
        );
      }

      // Insert readings
      const ins = `insert into readings(site_code,meter_id,ts,value)
                   values ($1,$2,$3,$4) on conflict do nothing`;
      for (const r of rows) {
        await client.query(ins, [r.site_id, r.meter_id, r.ts, r.value]);
      }

      await client.query("commit");
      res.json({ ok: true, rows: rows.length });
    } catch (e) {
      await client.query("rollback");
      throw e;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "server_error" });
  }
});

app.get("/sites", async (_req,res)=>{
  const { rows } = await pool.query(`select site_code as code, site_code as name from sites order by site_code`);
  res.json(rows);
});

app.get("/meters", async (req,res)=>{
  const { site_code } = req.query;
  const { rows } = await pool.query(`select meter_id, type, unit from meters where site_code=$1 order by meter_id`, [site_code]);
  res.json(rows);
});

app.get("/series", async (req,res)=>{
  const { meter_id, site_code, hours=24 } = req.query;
  const { rows } = await pool.query(
    `select ts, value from readings
     where site_code=$1 and meter_id=$2 and ts>=now() - ($3||' hours')::interval
     order by ts`, [site_code, meter_id, hours]
  );
  res.json(rows);
});

// Helper: turn "Great Yarmouth Boundary Elec" → "GREAT_YARMOUTH_BOUNDARY_ELEC"
function toMeterId(h) {
  return String(h || "")
    .trim()
    .replace(/[^\p{L}\p{N}]+/gu, "_")
    .replace(/^_+|_+$/g, "")
    .toUpperCase();
}

// Helper: parse "14-Oct-25" into a UTC Date
function parseTimestampMaybe(str) {
  if (!str) return null;
  const s = String(str).trim();

  // dd-MMM-yy or dd-MMM-yyyy
  const m = s.match(/^(\d{1,2})-([A-Za-z]{3})-(\d{2,4})$/);
  if (m) {
    const day = parseInt(m[1], 10);
    const monStr = m[2].toLowerCase();
    const yearRaw = parseInt(m[3], 10);
    const monMap = { jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11 };
    const month = monMap[monStr];
    if (month === undefined) return null;
    const year = yearRaw < 100 ? 2000 + yearRaw : yearRaw; // 25 -> 2025
    return new Date(Date.UTC(year, month, day));
  }

  // ISO or anything Date can parse
  const d = new Date(s);
  if (!isNaN(d)) return d;

  return null;
}

/**
 * WIDE CSV INGEST
 * CSV format:
 *   timestamp,<meter1>,<meter2>,...
 *   14-Oct-25,165299.9,1683.1
 *
 * Query params:
 *   ?site=LNT-GreatYarmouth
 *   &unit=kWh (default if missing)
 *   &type=electric (default if missing)
 */
app.post("/ingest/wide", async (req, res) => {
  try {
    const siteCode = String(req.query.site || "").trim();
    if (!siteCode) return res.status(400).json({ error: "site_required" });

    const defUnit = (req.query.unit || "kWh").toString();
    const defType = (req.query.type || "electric").toString();

    const text = typeof req.body === "string" ? req.body : "";
    if (!text.trim()) return res.status(400).json({ error: "empty_body" });

    // Parse CSV with header row
    const rows = parseCsv(text, { columns: true, skip_empty_lines: true });
    if (!rows.length) return res.status(400).json({ error: "empty_csv" });

    const headers = Object.keys(rows[0]);
    if (!headers.length || headers[0].toLowerCase() !== "timestamp") {
      return res.status(400).json({ error: "first_column_must_be_timestamp" });
    }
    const meterHeaders = headers.slice(1);
    if (!meterHeaders.length) {
      return res.status(400).json({ error: "no_meter_columns" });
    }

    // Resolve site
    const client = await pool.connect();
    let ingested = 0;

    try {
      await client.query("begin");

      const sRes = await client.query("select id from sites where site_code=$1 limit 1", [siteCode]);
      if (!sRes.rows.length) {
        await client.query("rollback");
        client.release();
        return res.status(400).json({ error: "site_not_found", site: siteCode });
      }
      const siteId = sRes.rows[0].id;

      // Upsert meters derived from headers
      const meterIds = meterHeaders.map(toMeterId);

      for (let i = 0; i < meterHeaders.length; i++) {
        const mId = meterIds[i];
        await client.query(
          `insert into meters (site_id, meter_id, type, unit)
           values ($1,$2,$3,$4)
           on conflict (site_id, meter_id)
           do update set type = excluded.type, unit = excluded.unit`,
          [siteId, mId, defType, defUnit]
        );
      }

      // Map meter_id -> UUID
      const { rows: mRows } = await client.query(
        `select id, meter_id from meters where site_id=$1 and meter_id = any($2::text[])`,
        [siteId, meterIds]
      );
      const idMap = Object.fromEntries(mRows.map(r => [r.meter_id, r.id]));

      // Insert readings
      const insertText = `insert into readings (meter_id, ts, value)
                          values ($1,$2,$3)
                          on conflict (meter_id, ts) do nothing`;

      for (const r of rows) {
        const ts = parseTimestampMaybe(r[headers[0]]);
        if (!ts) continue;

        for (let i = 0; i < meterHeaders.length; i++) {
          const header = meterHeaders[i];
          const mId = meterIds[i];
          const mv = r[header];
          if (mv === undefined || mv === null || mv === "") continue;

          const val = Number(mv);
          if (!isFinite(val)) continue;

          const meterUuid = idMap[mId];
          if (!meterUuid) continue;

          await client.query(insertText, [meterUuid, ts.toISOString(), val]);
          ingested++;
        }
      }

      await client.query("commit");
      client.release();

      res.json({ ok: true, site: siteCode, meters: meterIds, ingested });
    } catch (e) {
      try { await client.query("rollback"); } catch {}
      client.release();
      console.error("WIDE ingest error:", e);
      res.status(500).json({ error: "server_error" });
    }
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "server_error" });
  }
});


const port = process.env.PORT || 8081;
app.listen(port, ()=> console.log("API on "+port));
