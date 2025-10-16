import express from "express";
import { Pool } from "pg";
import cors from "cors";
import bodyParser from "body-parser";
import { parse as parseCsv } from "csv-parse/sync";   // keep once
import "dotenv/config";
import fs from "fs";
import path from "path";

/* -------------------- meters.json (optional) -------------------- */
// Example structure:
// {
//   "LNT-GreatYarmouth": {
//     "Great Yarmouth Boundary Elec - Meter ID (kW hr)": { "meter_id":"ELC_BOUND","type":"electric","unit":"kWh" },
//     "Great Yarmouth Mains water - Value (m³)":         { "meter_id":"WTR_MAIN","type":"water","unit":"m3" }
//   }
// }
const meterMap = fs.existsSync(path.resolve("./meters.json"))
  ? JSON.parse(fs.readFileSync(path.resolve("./meters.json"), "utf8"))
  : {};

/* -------------------- app & db -------------------- */
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

function normHeader(h) {
  return String(h ?? "")
    .replace(/\uFEFF/g, "")          // BOM
    .replace(/\u00A0/g, " ")         // NBSP → space
    .replace(/[“”]/g, '"')           // smart quotes → "
    .replace(/[’]/g, "'")            // smart apostrophe → '
    .replace(/m³/gi, "m3")           // proper superscript ³
    .replace(/m�/gi, "m3")           // garbled ³ from bad encoding
    .replace(/\s+/g, " ")
    .trim();
}


// Ensure headers are unique so csv-parse doesn't drop/overwrite duplicates
function makeUniqueHeaders(headers) {
  const seen = new Map();
  return headers.map((h, idx) => {
    const base = h || `col_${idx}`;
    let name = base;
    const n = seen.get(base) ?? 0;
    if (n > 0) name = `${base}__${n + 1}`;
    seen.set(base, n + 1);
    return name;
  });
}

function parseTimestampMaybe(str) {
  if (str === undefined || str === null) return null;
  let s = String(str).trim();

  // Strip trailing timezone words/offsets/acronyms (e.g. "BST", "GMT", "+01:00", "UTC")
  // Keep AM/PM intact.
  s = s.replace(/\s*(?:GMT|UTC|BST|CEST|CET|IST|EET|EEST|PST|PDT|EST|EDT|[A-Z]{2,4}|[+-]\d{2}:\d{2})$/i, "").trim();

  // Excel serial (days since 1899-12-30)
  if (/^\d{5}(\.\d+)?$/.test(s)) {
    const base = new Date(Date.UTC(1899, 11, 30));
    const ms = Math.round(parseFloat(s) * 86400000);
    return new Date(base.getTime() + ms);
  }

  // dd-MMM-yy[ HH:mm[:ss][ AM/PM]]
  let m = s.match(/^(\d{1,2})-([A-Za-z]{3})-(\d{2,4})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?(?:\s?(AM|PM))?)?$/i);
  if (m) {
    const day = +m[1];
    const monMap = { jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,oct:9,nov:10,dec:11 };
    const month = monMap[m[2].toLowerCase()];
    const yr = +m[3]; const year = yr < 100 ? 2000 + yr : yr;
    let hh = +(m[4] ?? 0), mm = +(m[5] ?? 0), ss = +(m[6] ?? 0);
    const ap = (m[7] || "").toUpperCase();
    if (ap) { if (ap === "PM" && hh < 12) hh += 12; if (ap === "AM" && hh === 12) hh = 0; }
    return new Date(Date.UTC(year, month, day, hh, mm, ss));
  }

  // dd/MM/yyyy[ HH:mm[:ss][ AM/PM]]
  m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?(?:\s?(AM|PM))?)?$/i);
  if (m) {
    const day = +m[1], month = +m[2]-1, yr = +m[3]; const year = yr < 100 ? 2000 + yr : yr;
    let hh = +(m[4] ?? 0), mm = +(m[5] ?? 0), ss = +(m[6] ?? 0);
    const ap = (m[7] || "").toUpperCase();
    if (ap) { if (ap === "PM" && hh < 12) hh += 12; if (ap === "AM" && hh === 12) hh = 0; }
    return new Date(Date.UTC(year, month, day, hh, mm, ss));
  }

  // dd-MM-yyyy[ HH:mm[:ss][ AM/PM]]
  m = s.match(/^(\d{1,2})-(\d{1,2})-(\d{4})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?(?:\s?(AM|PM))?)?$/i);
  if (m) {
    const day = +m[1], month = +m[2]-1, year = +m[3];
    let hh = +(m[4] ?? 0), mm = +(m[5] ?? 0), ss = +(m[6] ?? 0);
    const ap = (m[7] || "").toUpperCase();
    if (ap) { if (ap === "PM" && hh < 12) hh += 12; if (ap === "AM" && hh === 12) hh = 0; }
    return new Date(Date.UTC(year, month, day, hh, mm, ss));
  }

  // ISO-ish: yyyy-MM-dd[ T]HH:mm[:ss]
  m = s.match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
  if (m) {
    const year = +m[1], month = +m[2]-1, day = +m[3];
    const hh = +(m[4] ?? 0), mm = +(m[5] ?? 0), ss = +(m[6] ?? 0);
    return new Date(Date.UTC(year, month, day, hh, mm, ss));
  }

  const d = new Date(s);
  if (!isNaN(d)) return d;
  return null;
}


// Number parser (handles units, thousand/decimal variants, bracket negatives)
function parseNumber(x) {
  if (x === undefined || x === null) return null;
  let s = String(x).trim();

  // Empty / placeholders → null
  if (s === "" || /^(-|—|–|N\/A|null|nil)$/i.test(s)) return null;

  // Bracketed negatives: (123.45) → -123.45
  let neg = false;
  const mNeg = s.match(/^\((.*)\)$/);
  if (mNeg) { neg = true; s = mNeg[1].trim(); }

  // Extract first number-like token (drop units like "kWh", "m³")
  const mNum = s.match(/[-+]?[\d\s.,]+(?:\.\d+|,\d+)?/);
  if (!mNum) return null;
  s = mNum[0].trim();

  // Detect comma-decimal style: "1.234,56" or "1234,56"
  if (/^-?\d{1,3}(\.\d{3})*,\d+$/.test(s) || /^-?\d+,\d+$/.test(s)) {
    s = s.replace(/\./g, "").replace(",", ".");
  } else {
    // Remove thousands separators (commas/spaces)
    s = s.replace(/(?<=\d)[ ,](?=\d{3}\b)/g, "");
  }

  const n = Number(s);
  if (!isFinite(n)) return null;
  return neg ? -n : n;
}

// Try parsing with both delimiters and pick the "better" one (more columns)
function parseCsvBest(text) {
  const tryDelims = [",", ";"];
  let best = null;

  for (const delim of tryDelims) {
    const rowsRaw = parseCsv(text, {
      columns: (header) => {
        const cleaned = header.map(normHeader);
        const unique = makeUniqueHeaders(cleaned);
        return unique;
      },
      bom: true,
      skip_empty_lines: true,
      relax_column_count: true,
      delimiter: delim,
      trim: false
    });

    if (!rowsRaw.length) continue;
    const headers = Object.keys(rowsRaw[0]);
    const score = headers.length;

    if (!best || score > best.headers.length) {
      best = { rows: rowsRaw, delimiter: delim, headers };
    }
  }

  if (!best) return { rows: [], delimiter: ",", headers: [] };
  return best;
}

/* -------------------- narrow ingest (unchanged) -------------------- */
app.post("/ingest", async (req, res) => {
  try {
    const site_id = req.query.site || "LNT-GreatYarmouth";
    const config = meterMap[site_id] || null;

    const text = typeof req.body === "string" ? req.body : "";
    const parsedBest = parseCsvBest(text);
    const parsed = parsedBest.rows;

    const rows = [];

    if (parsed.length && "timestamp" in parsed[0] && !("site_id" in parsed[0]) && config) {
      const normConfig = {};
      Object.keys(config).forEach(k => { normConfig[normHeader(k)] = config[k]; });

      for (const r of parsed) {
        const ts = r.timestamp;
        const keys = Object.keys(r).filter(k => k !== "timestamp");
        for (const col of keys) {
          const rawMeta = config[col];
          const meta = rawMeta ?? normConfig[normHeader(col)];
          if (!meta) continue;

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

      const uniqueSites = [...new Set(rows.map(r => r.site_code))];
      for (const sc of uniqueSites) {
        await client.query(
          `insert into sites (site_code, name) values ($1, $1)
           on conflict do nothing`,
          [sc]
        );
      }

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

      const ins = `insert into readings (site_code, meter_id, ts, value)
                   values ($1,$2,$3,$4)
                   on conflict do nothing`;
      for (const r of rows) {
        const ts = r.ts instanceof Date ? r.ts.toISOString() : r.ts;
        await client.query(ins, [r.site_code, r.meter_id, ts, r.value]);
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

/* -------------------- read APIs (unchanged) -------------------- */
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

/* -------------------- WIDE ingest with DEBUG -------------------- */
/*
 * CSV:
 *   timestamp,<meter1>,<meter2>,...
 * Supports: comma or semicolon; UK/ISO dates; thousands/comma decimals.
 * Query:
 *   ?site=LNT-GreatYarmouth
 *   &unit=kWh (default if no meters.json)
 *   &type=electric (default if no meters.json)
 *   &debug=1  (return detailed parsing diagnostics)
 */
app.post("/ingest/wide", async (req, res) => {
  try {
    const siteCode = String(req.query.site || "").trim();
    if (!siteCode) return res.status(400).json({ error: "site_required" });

    const defUnit = (req.query.unit || "kWh").toString();
    const defType = (req.query.type || "electric").toString();
    const wantDebug = String(req.query.debug || "0") === "1";

    const text = typeof req.body === "string" ? req.body : "";
    if (!text.trim()) return res.status(400).json({ error: "empty_body" });

    const parsedBest = parseCsvBest(text);
    const rowsCsv   = parsedBest.rows;
    const headers   = parsedBest.headers;
    const delimiter = parsedBest.delimiter;

    if (!rowsCsv.length) return res.status(400).json({ error: "empty_csv" });

    if (!headers.length || normHeader(headers[0]).toLowerCase() !== "timestamp") {
      return res.status(400).json({ error: "first_column_must_be_timestamp", headers, delimiter });
    }
    const valueHeaders = headers.slice(1);

    // Build header→meta mapping (prefer meters.json exact, then normalised)
    const siteMap = meterMap[siteCode] || null;
    const normSiteMap = {};
    if (siteMap) {
      for (const k of Object.keys(siteMap)) normSiteMap[normHeader(k)] = siteMap[k];
    }

    const headerToMeta = {};
    for (const h of valueHeaders) {
      const exact = siteMap && siteMap[h];
      const normal = normSiteMap[normHeader(h)];
      const meta = exact ?? normal ?? { meter_id: toMeterId(h), type: defType, unit: defUnit };
      headerToMeta[h] = meta;
    }

    // --- DEBUG counters
    const totalRows = rowsCsv.length;
    let scannedRows = 0;
    let badTimestamps = 0;
    let badTimestampExamples = [];

    const tsSamples = [];
    for (let i = 0; i < Math.min(5, rowsCsv.length); i++) {
      tsSamples.push(String(rowsCsv[i][headers[0]]));
    }

    const per = {}; // per header stats
    for (const h of valueHeaders) {
      per[h] = { parsed: 0, nulls: 0, bad_examples: [] };
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
        if (!ts) {
          if (badTimestampExamples.length < 3) {
            const raw = row[headers[0]];
            badTimestampExamples.push(raw === undefined ? "<undefined>" : String(raw));
          }
          badTimestamps++;
          continue;
        }
        scannedRows++;

        for (const h of valueHeaders) {
          const meta = headerToMeta[h];
          const raw = row[h];
          const val = parseNumber(raw);
          if (val === null) {
            per[h].nulls++;
            if (per[h].bad_examples.length < 3 && raw !== undefined && raw !== null && String(raw).trim() !== "") {
              per[h].bad_examples.push(String(raw));
            }
            continue;
          }
          per[h].parsed++;
          await client.query(insertText, [siteCode, meta.meter_id, ts.toISOString(), val]);
          ingested++;
        }
      }

      await client.query("commit");

      const basePayload = {
        ok: true,
        site: siteCode,
        meters: valueHeaders.map(h => headerToMeta[h].meter_id),
        ingested
      };

      if (wantDebug) {
        basePayload.debug = {
          delimiter,
          headers,
          mapped_headers: Object.fromEntries(valueHeaders.map(h => [h, headerToMeta[h].meter_id])),
          total_rows: totalRows,
          scanned_rows: scannedRows,
          bad_timestamps: badTimestamps,
          bad_timestamp_examples: badTimestampExamples,
          timestamp_samples: tsSamples,
          per_column: per
        };
      }

      res.json(basePayload);
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






