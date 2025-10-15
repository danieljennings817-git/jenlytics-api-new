import express from "express";
import { Pool } from "pg";
import cors from "cors";
import bodyParser from "body-parser";
import { parse as parseCsv } from "csv-parse/sync";   // keep once
import "dotenv/config";
import fs from "fs";
import path from "path";

// Optional: header→meta mapping per site
// {
//   "LNT-GreatYarmouth": {
//     "Great Yarmouth Boundary Elec - Meter ID (kW hr)": { "meter_id":"ELC_BOUND","type":"electric","unit":"kWh" },
//     "Great Yarmouth Mains water - Value (m³)":         { "meter_id":"WTR_MAIN","type":"water","unit":"m3" }
//   }
// }
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
  try { await pool.query("select 1"); res.json({ ok: true }); }
  catch { res.status(500).json({ ok: false, error: "db_unavailable" }); }
});

/* -------------------- helpers -------------------- */

// Clean fallback meter_id from header
function toMeterId(h) {
  return String(h || "")
    .normalize("NFKD")
    .replace(/[^\p{L}\p{N}]+/gu, "_")
    .replace(/^_+|_+$/g, "")
    .toUpperCase();
}

// Parse "14-Oct-25", "15/10/2025 00:00[:ss]" or ISO
function parseTimestampMaybe(str) {
  if (!str) return null;
  const s = String(str).trim();

  // dd-MMM-yy[ HH:mm[:ss]]
  let m = s.match(/^(\d{1,2})-([A-Za-z]{3})-(\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
  if (m) {
    const day = +m[1];
    const monMap = { jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11 };
    const month = monMap[m[2].toLowerCase()];
    const yr = +m[3]; const year = yr < 100 ? 2000 + yr : yr;
    const hh = +(m[4] ?? 0), mm = +(m[5] ?? 0), ss = +(m[6] ?? 0);
    return new Date(Date.UTC(year, month, day, hh, mm, ss));
  }

  // dd/MM/yyyy[ HH:mm[:ss]]
  m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
  if (m) {
    const day = +m[1], month = +m[2]-1, yr = +m[3]; const year = yr < 100 ? 2000 + yr : yr;
    const hh = +(m[4] ?? 0), mm = +(m[5] ?? 0), ss = +(m[6] ?? 0);
    return new Date(Date.UTC(year, month, day, hh, mm, ss));
  }

  const d = new Date(s);
  if (!isNaN(d)) return d;
  return null;
}

// Coerce "165,299.9" or "1 234,56" → number
function parseNumber(x) {
  if (x === undefined || x === null || x === "") return null;
  let s = String(x).trim();

  // If looks like comma-decimal "1234,56" (maybe with dot thousands)
  if (/^-?\d{1,3}(\.\d{3})*,\d+$/.test(s) || /^-?\d+,\d+$/.test(s)) {
    s = s.replace(/\./g, "").replace(",", ".");
  } else {
    // Remove thousands separators (commas/spaces) like "165,299.9" or "1 234.56"
    s = s.replace(/(?<=\d)[ ,](?=\d{3}\b)/g, "");
  }
  const n = Number(s);
  return isFinite(n) ? n : null;
}

/* -------------------- narrow ingest -------------------- */
/* CSV columns: site_id,meter_id,type,unit,timestamp,value
   (or if meters.json matches, can accept timestamp + named columns via ?site=... as before) */
app.post("/ingest", async (req, res) => {
  try {
    const site_id = req.query.site || "LNT-GreatYarmouth";
    const config = meterMap[site_id] || null;

    const text = typeof req.body === "string" ? req.body : "";
    // delimiter auto-detect on first line
    const first = (text.split(/\r?\n/)[0] || "");
    const delim = (first.split(";").length - 1) > (first.split(",").length - 1) ? ";" : ",";

    const parsed = parseCsv(text, { columns: true, skip_empty_lines: true, delimiter: delim });

    const rows = [];
    if (parsed.length && "timestamp" in parsed[0] && !("site_id" in parsed[0]) && config) {
      // WIDE->NARROW via meters.json
      for (const r of parsed) {
        const ts = r.timestamp;
        for (const [col, meta] of Object.entries(config)) {
          const v = parseNumber(r[col]);
          if (v === null) continue;
          rows.push({
            site_code: site_id,
            meter_id: meta.meter_id,
            type: meta.type,
            unit: meta.unit,
            ts,
            value: v
          });
        }
      }
    } else {
      // Narrow as-is (also normalise numbers)
      for (const r of parsed) {
        if (!r.timestamp) continue;
        const v = parseNumber(r.value);
        if (v === null) continue;
        rows.push({
          site_code: r.site_id || site_id,
          meter_id: r.meter_id,
          type: r.type,
          unit: r.unit,
          ts: r.timestamp,
          value: v
        });
      }
    }

    if (!rows.length) return res.status(400).json({ error: "empty_payload" });

    const client = await pool.connect();
    try {
      await client.query("begin");

      // sites
      const uniqueSites = [...new Set(rows.map(r => r.site_code))];
      for (const sc of uniqueSites) {
        await client.query(
          `insert into sites (site_code, name) values ($1, $1)
           on conflict do nothing`,
          [sc]
        );
      }

      // meters
      const seen = new Set();
      for (const r of rows) {
        const k = `${r.site_code}::${r.meter_id}`;
        if (seen.has(k)) continue;
        seen.add(k);
        await client.query(
          `insert into meters (site_code, meter_id, type, unit)
           values ($1,$2,$3,$4)
           on conflict (site_code, meter_id)
           do update set type=excluded.type, unit=excluded.unit`,
          [r.site_code, r.meter_id, r.type, r.unit]
        );
      }

      // readings
      const ins = `insert into readings (site_code, meter_id, ts, value)
                   values ($1,$2,$3,$4)
                   on conflict do nothing`;
      for (const r of rows) {
        await client.query(ins, [r.site_code, r.meter_id, r.ts, r.value]);
      }

      await client.query("commit");
      res.json({ ok: true, rows: rows.length });
    } catch (e) {
      await client.query("rollback"); throw e;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "server_error" });
  }
});

/* -------------------- read APIs -------------------- */
app.get("/sites", async (_req, res) => {
  const { rows } = await pool.query(
    `select site_code as code, site_code as name from sites order by site_code`
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

/* -------------------- wide ingest -------------------- */
/* CSV:
 *   timestamp,<meter1>,<meter2>,...
 * Supports: comma or semicolon; UK/ISO dates; thousands/comma decimals.
 * Query:
 *   ?site=LNT-GreatYarmouth
 *   &unit=kWh (default if no meters.json)
 *   &type=electric (default if no meters.json)
 */
app.post("/ingest/wide", async (req, res) => {
  try {
    const siteCode = String(req.query.site || "").trim();
    if (!siteCode) return res.status(400).json({ error: "site_required" });

    const defUnit = (req.query.unit || "kWh").toString();
    const defType = (req.query.type || "electric").toString();

    const text = typeof req.body === "string" ? req.body : "";
    if (!text.trim()) return res.status(400).json({ error: "empty_body" });

    // delimiter auto-detect
    const first = (text.split(/\r?\n/)[0] || "");
    const delim = (first.split(";").length - 1) > (first.split(",").length - 1) ? ";" : ",";

    const rowsCsv = parseCsv(text, { columns: true, skip_empty_lines: true, delimiter: delim });
    if (!rowsCsv.length) return res.status(400).json({ error: "empty_csv" });

    const headers = Object.keys(rowsCsv[0]);
    if (!headers.length || headers[0].toLowerCase() !== "timestamp") {
      return res.status(400).json({ error: "first_column_must_be_timestamp" });
    }
    const valueHeaders = headers.slice(1);

    // Build header→meta mapping (prefer meters.json, else fallback)
    const siteMap = meterMap[siteCode] || null;
    const headerToMeta = {};
    for (const h of valueHeaders) {
      if (siteMap && siteMap[h]) {
        headerToMeta[h] = siteMap[h];
      } else {
        headerToMeta[h] = { meter_id: toMeterId(h), type: defType, unit: defUnit };
      }
    }

    const client = await pool.connect();
    let ingested = 0;
    try {
      await client.query("begin");

      await client.query(
        `insert into sites (site_code, name) values ($1, $1)
         on conflict do nothing`,
        [siteCode]
      );

      // upsert meters
      for (const h of valueHeaders) {
        const meta = headerToMeta[h];
        await client.query(
          `insert into meters (site_code, meter_id, type, unit)
           values ($1,$2,$3,$4)
           on conflict (site_code, meter_id)
           do update set type=excluded.type, unit=excluded.unit`,
          [siteCode, meta.meter_id, meta.type, meta.unit]
        );
      }

      const insertText = `insert into readings (site_code, meter_id, ts, value)
                          values ($1,$2,$3,$4)
                          on conflict do nothing`;

      for (const row of rowsCsv) {
        const ts = parseTimestampMaybe(row[headers[0]]);
        if (!ts) continue;

        for (const h of valueHeaders) {
          const meta = headerToMeta[h];
          const val = parseNumber(row[h]);
          if (val === null) continue;

          await client.query(insertText, [siteCode, meta.meter_id, ts.toISOString(), val]);
          ingested++;
        }
      }

      await client.query("commit");
      res.json({
        ok: true,
        site: siteCode,
        meters: valueHeaders.map(h => headerToMeta[h].meter_id),
        ingested
      });
    } catch (e) {
      try { await client.query("rollback"); } catch {}
      console.error("WIDE ingest error:", e);
      res.status(500).json({ error: "server_error" });
    } finally {
      client.release();
    }
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "server_error" });
  }
});

const port = process.env.PORT || 8081;
app.listen(port, () => console.log("API on " + port));


