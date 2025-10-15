import express from "express";
import { Pool } from "pg";
import cors from "cors";
import bodyParser from "body-parser";
import { parse as parseCsv } from "csv-parse/sync";   // <-- keep this ONCE
import "dotenv/config";
import fs from "fs";
import path from "path";

// ---- optional map for /ingest (column-per-meter uploads by name)
const meterMap = fs.existsSync(path.resolve("./meters.json"))
  ? JSON.parse(fs.readFileSync(path.resolve("./meters.json"), "utf8"))
  : {};

const app = express();
app.use(cors({ origin: "*" }));
app.use(bodyParser.text({
  type: ["text/*", "text/csv", "application/csv", "application/octet-stream"],
  limit: "5mb"
}));
app.use(express.json({ limit: "5mb" }));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

app.get("/health", async (_req, res) => {
  try {
    await pool.query("select 1");
    res.json({ ok: true });
  } catch {
    res.status(500).json({ ok: false, error: "db_unavailable" });
  }
});

/* -------------------- NARROW INGEST (rows = readings) -------------------- */
/* expects CSV columns: site_id,meter_id,type,unit,timestamp,value */
app.post("/ingest", async (req, res) => {
  try {
    const site_id = req.query.site || "LNT-GreatYarmouth"; // fallback if not in CSV
    const config = meterMap[site_id] || null;

    const text = typeof req.body === "string" ? req.body : "";
    const parsed = parseCsv(text, { columns: true, skip_empty_lines: true });

    const rows = [];
    if (parsed.length && "timestamp" in parsed[0] && !("site_id" in parsed[0]) && config) {
      // WIDE->NARROW via meters.json (timestamp, <friendly cols>...)
      for (const r of parsed) {
        const ts = r.timestamp;
        for (const [col, meta] of Object.entries(config)) {
          if (r[col] === undefined || r[col] === "") continue;
          rows.push({
            site_code: site_id,
            meter_id: meta.meter_id,
            type: meta.type,
            unit: meta.unit,
            ts,
            value: Number(r[col])
          });
        }
      }
    } else {
      // Narrow shape already
      for (const r of parsed) {
        if (!r.timestamp) continue;
        rows.push({
          site_code: r.site_id || site_id,
          meter_id: r.meter_id,
          type: r.type,
          unit: r.unit,
          ts: r.timestamp,
          value: Number(r.value)
        });
      }
    }

    if (!rows.length) return res.status(400).json({ error: "empty_payload" });

    const client = await pool.connect();
    try {
      await client.query("begin");

      // Ensure sites exist
      const uniqueSites = [...new Set(rows.map(r => r.site_code))];
      for (const sc of uniqueSites) {
        await client.query(
          `insert into sites (site_code, name) values ($1, $1)
           on conflict do nothing`,
          [sc]
        );
      }

      // Upsert meters
      const seenMeters = new Set();
      for (const r of rows) {
        const key = `${r.site_code}::${r.meter_id}`;
        if (seenMeters.has(key)) continue;
        seenMeters.add(key);
        await client.query(
          `insert into meters (site_code, meter_id, type, unit)
           values ($1,$2,$3,$4)
           on conflict (site_code, meter_id)
           do update set type = excluded.type, unit = excluded.unit`,
          [r.site_code, r.meter_id, r.type, r.unit]
        );
      }

      // Insert readings
      const ins = `insert into readings(site_code, meter_id, ts, value)
                   values ($1,$2,$3,$4)
                   on conflict do nothing`;
      for (const r of rows) {
        await client.query(ins, [r.site_code, r.meter_id, r.ts, r.value]);
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

/* -------------------- READ APIs -------------------- */
app.get("/sites", async (_req, res) => {
  const { rows } = await pool.query(
    `select site_code as code, site_code as name
     from sites
     order by site_code`
  );
  res.json(rows);
});

app.get("/meters", async (req, res) => {
  const { site_code } = req.query;
  const { rows } = await pool.query(
    `select meter_id, type, unit
       from meters
      where site_code = $1
      order by meter_id`,
    [site_code]
  );
  res.json(rows);
});

app.get("/series", async (req, res) => {
  const { meter_id, site_code, hours = 24 } = req.query;
  const { rows } = await pool.query(
    `select ts, value
       from readings
      where site_code = $1
        and meter_id  = $2
        and ts >= now() - ($3 || ' hours')::interval
      order by ts`,
    [site_code, meter_id, hours]
  );
  res.json(rows);
});

/* -------------------- WIDE INGEST (timestamp + meter columns) -------------------- */
/* CSV:
 *   timestamp,<meter1>,<meter2>,...
 *   14-Oct-25,165299.9,1683.1
 * Query:
 *   ?site=LNT-GreatYarmouth
 *   &unit=kWh (default)
 *   &type=electric (default)
 */
function toMeterId(h) {
  return String(h || "")
    .trim()
    .replace(/[^\p{L}\p{N}]+/gu, "_")
    .replace(/^_+|_+$/g, "")
    .toUpperCase();
}

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

  const d = new Date(s);
  if (!isNaN(d)) return d;

  return null;
}

app.post("/ingest/wide", async (req, res) => {
  try {
    const siteCode = String(req.query.site || "").trim();
    if (!siteCode) return res.status(400).json({ error: "site_required" });

    const defUnit = (req.query.unit || "kWh").toString();
    const defType = (req.query.type || "electric").toString();

    const text = typeof req.body === "string" ? req.body : "";
    if (!text.trim()) return res.status(400).json({ error: "empty_body" });

    const rowsCsv = parseCsv(text, { columns: true, skip_empty_lines: true });
    if (!rowsCsv.length) return res.status(400).json({ error: "empty_csv" });

    const headers = Object.keys(rowsCsv[0]);
    if (!headers.length || headers[0].toLowerCase() !== "timestamp") {
      return res.status(400).json({ error: "first_column_must_be_timestamp" });
    }
    const meterHeaders = headers.slice(1);
    if (!meterHeaders.length) return res.status(400).json({ error: "no_meter_columns" });

    // Ensure site & meters exist (simple schema keyed by site_code)
    const client = await pool.connect();
    let ingested = 0;
    try {
      await client.query("begin");

      await client.query(
        `insert into sites (site_code, name) values ($1, $1)
         on conflict do nothing`,
        [siteCode]
      );

      const meterIds = meterHeaders.map(toMeterId);
      for (let i = 0; i < meterHeaders.length; i++) {
        const mId = meterIds[i];
        await client.query(
          `insert into meters (site_code, meter_id, type, unit)
           values ($1,$2,$3,$4)
           on conflict (site_code, meter_id)
           do update set type = excluded.type, unit = excluded.unit`,
          [siteCode, mId, defType, defUnit]
        );
      }

      const insertText = `insert into readings (site_code, meter_id, ts, value)
                          values ($1,$2,$3,$4)
                          on conflict do nothing`;

      for (const r of rowsCsv) {
        const ts = parseTimestampMaybe(r[headers[0]]);
        if (!ts) continue;

        for (let i = 0; i < meterHeaders.length; i++) {
          const header = meterHeaders[i];
          const mId = meterIds[i];
          const mv = r[header];
          if (mv === undefined || mv === null || mv === "") continue;

          const val = Number(mv);
          if (!isFinite(val)) continue;

          await client.query(insertText, [siteCode, mId, ts.toISOString(), val]);
          ingested++;
        }
      }

      await client.query("commit");
      client.release();
      res.json({ ok: true, site: siteCode, meters: meterHeaders.map(toMeterId), ingested });
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
app.listen(port, () => console.log("API on " + port));

