import express from "express";
import { Pool } from "pg";
import cors from "cors";
import bodyParser from "body-parser";
import { parse as parseCsv } from "csv-parse/sync";
import "dotenv/config";
import fs from "fs";
import path from "path";
import crypto from "crypto";

/* -------------------- meters.json (optional) -------------------- */
// Example:
// {
//   "LNT-GreatYarmouth": {
//     "Great Yarmouth Boundary Elec - Meter ID (kW hr)": { "meter_id":"ELC_BOUND","type":"electric","unit":"kWh" },
//     "Great Yarmouth Mains water - Value (m3)":         { "meter_id":"WTR_MAIN","type":"water","unit":"m3" }
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

// --- tiny request logger
app.use((req, _res, next) => {
  console.log(new Date().toISOString(), req.method, req.path);
  next();
});

// --- process-level traps so crashes print reasons
process.on("unhandledRejection", (reason) => {
  console.error("unhandledRejection:", reason);
});
process.on("uncaughtException", (err) => {
  console.error("uncaughtException:", err);
});

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

app.get("/health", async (_req, res) => {
  try { await pool.query("select 1"); res.json({ ok: true }); }
  catch (e) {
    console.error("health error:", e);
    res.status(500).json({ ok: false, error: "db_unavailable", detail: String(e?.message || e) });
  }
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

// Normalise header text (trim, collapse spaces, remove NBSP, unify quotes, fix m3 garble)
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

// Keys used for alias/canonical matching
function normKey(s) {
  return normHeader(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, " ");
}

// Token-overlap similarity (Jaccard)
function tokenSim(a, b) {
  const A = new Set(normKey(a).split(" ").filter(Boolean));
  const B = new Set(normKey(b).split(" ").filter(Boolean));
  if (!A.size || !B.size) return 0;
  let inter = 0;
  for (const t of A) if (B.has(t)) inter++;
  return inter / (A.size + B.size - inter);
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

// Timestamp parser (BST/GMT acronyms, ISO, UK/EU/US variants, Excel serial)
function parseTimestampMaybe(str) {
  if (str === undefined || str === null) return null;
  let s = String(str).trim();

  // strip trailing tz words/offsets
  s = s.replace(/\s*(?:GMT|UTC|BST|CEST|CET|IST|EET|EEST|PST|PDT|EST|EDT|[A-Z]{2,4}|[+-]\d{2}:\d{2})$/i, "").trim();

  // Excel serial (days since 1899-12-30)
  if (/^\d{5}(\.\d+)?$/.test(s)) {
    const base = new Date(Date.UTC(1899, 11, 30));
    const ms = Math.round(parseFloat(s) * 86400000);
    return new Date(base.getTime() + ms);
  }

  // dd-MMM-yy[ HH:mm[:ss] [AM/PM]]
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

  // dd/MM/yyyy[ ...]
  m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?(?:\s?(AM|PM))?)?$/i);
  if (m) {
    const day = +m[1], month = +m[2]-1, yr = +m[3]; const year = yr < 100 ? 2000 + yr : yr;
    let hh = +(m[4] ?? 0), mm = +(m[5] ?? 0), ss = +(m[6] ?? 0);
    const ap = (m[7] || "").toUpperCase();
    if (ap) { if (ap === "PM" && hh < 12) hh += 12; if (ap === "AM" && hh === 12) hh = 0; }
    return new Date(Date.UTC(year, month, day, hh, mm, ss));
  }

  // dd-MM-yyyy[ ...]
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

// Number parser
function parseNumber(x) {
  if (x === undefined || x === null) return null;
  let s = String(x).trim();

  if (s === "" || /^(-|—|–|N\/A|null|nil|nan)$/i.test(s)) return null;

  let neg = false;
  const mNeg = s.match(/^\((.*)\)$/);
  if (mNeg) { neg = true; s = mNeg[1].trim(); }

  const mNum = s.match(/[-+]?[\d\s.,]+(?:\.\d+|,\d+)?/);
  if (!mNum) return null;
  s = mNum[0].trim();

  if (/^-?\d{1,3}(\.\d{3})*,\d+$/.test(s) || /^-?\d+,\d+$/.test(s)) {
    s = s.replace(/\./g, "").replace(",", ".");
  } else {
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

/* -------------------- narrow ingest -------------------- */
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
      try { await client.query("rollback"); } catch {}
      console.error("INGEST error:", e);
      const out = { error: "server_error" };
      if (String(process.env.DEBUG_ERRORS || "") === "1") {
        out.detail = e?.message || String(e);
      }
      res.status(500).json(out);
    } finally {
      client.release();
    }
  } catch (e) {
    console.error("INGEST outer error:", e);
    const out = { error: "server_error" };
    if (String(process.env.DEBUG_ERRORS || "") === "1") out.detail = e?.message || String(e);
    res.status(500).json(out);
  }
});

/* -------------------- read APIs -------------------- */
app.get("/sites", async (_req, res) => {
  // <<< changed: hide UNROUTED in UI
  const { rows } = await pool.query(
    `select site_code as code, site_code as name
       from sites
      where site_code <> 'UNROUTED'
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

/* -------------------- WIDE ingest with DEBUG + forward-fill -------------------- */
app.post("/ingest/wide", async (req, res) => {
  try {
    const siteCode = String(req.query.site || "").trim();
    if (!siteCode) return res.status(400).json({ error: "site_required" });

    const defUnit = (req.query.unit || "KWh").toString();
    const defType = (req.query.type || "electric").toString();
    const wantDebug = String(req.query.debug || "0") === "1";
    const fill = String(req.query.fill || "drop"); // drop | ffill | zero

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

    // Forward-fill support: remember last values per header
    const lastVals = Object.fromEntries(valueHeaders.map(h => [h, null]));

    // --- DEBUG counters
    const totalRows = rowsCsv.length;
    let scannedRows = 0;
    let badTimestamps = 0;
    let badTimestampExamples = [];

    const tsSamples = [];
    for (let i = 0; i < Math.min(5, rowsCsv.length); i++) {
      tsSamples.push(String(rowsCsv[i][headers[0]]));
    }

    const per = {};
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
          let val = parseNumber(raw);

          if (val === null) {
            if (fill === "ffill" && lastVals[h] !== null) {
              val = lastVals[h];
              per[h].parsed++;
            } else if (fill === "zero") {
              val = 0;
              per[h].parsed++;
            } else {
              per[h].nulls++;
              if (per[h].bad_examples.length < 3 && raw !== undefined && raw !== null && String(raw).trim() !== "") {
                per[h].bad_examples.push(String(raw));
              }
              continue;
            }
          } else {
            per[h].parsed++;
          }

          lastVals[h] = val;
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
          per_column: per,
          fill_policy: fill
        };
      }

      res.json(basePayload);
    } catch (e) {
      try { await client.query("rollback"); } catch {}
      console.error("WIDE ingest error:", e);
      const out = { error: "server_error" };
      if (String(process.env.DEBUG_ERRORS || "") === "1") out.detail = e?.message || String(e);
      res.status(500).json(out);
    } finally {
      client.release();
    }
  } catch (e) {
    console.error("WIDE outer error:", e);
    const out = { error: "server_error" };
    if (String(process.env.DEBUG_ERRORS || "") === "1") out.detail = e?.message || String(e);
    res.status(500).json(out);
  }
});

/**
 * Gmail → Apps Script posts JSON here:
 * {
 *   token, message_id, filename, csv,
 *   from?, subject?, sent_at?
 * }
 *
 * Routing: rules → sticky sender fallback. Meter IDs: alias/exact/normalized/fuzzy → learn alias.
 */
app.post("/ingest/email", async (req, res) => {
  try {
    const { token, message_id, filename, csv, from, subject } = req.body || {};

    // --- robust token check (supports either env var name)
    const provided = String(token || "").trim();
    const expected = String(process.env.INGEST_TOKEN || process.env.SHARED_TOKEN || "").trim();
    if (!expected || provided !== expected) {
      return res.status(401).json({ error: "unauthorized" });
    }
    if (!csv || !message_id || !filename) {
      return res.status(400).json({ error: "missing fields (csv, message_id, filename)" });
    }

    const hash = crypto.createHash("sha256").update(csv).digest("hex");

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      // Deduplicate by Gmail message_id + filename
      await client.query(
        `INSERT INTO email_ingest_log (message_id, filename, hash)
         VALUES ($1,$2,$3)
         ON CONFLICT (message_id, filename) DO NOTHING`,
        [message_id, filename, hash]
      );
      const seen = await client.query(
        `SELECT 1 FROM email_ingest_log WHERE message_id=$1 AND filename=$2`,
        [message_id, filename]
      );
      if (seen.rowCount === 0) {
        await client.query("ROLLBACK");
        return res.json({ ingested: 0, reason: "duplicate" });
      }

      // ROUTING: rules first
      const ruleRes = await client.query(
        `SELECT site_code, default_type, default_unit
           FROM ingest_rules
          WHERE enabled = true
            AND ( $1::text IS NULL OR from_regex    IS NULL OR $1::text ~* from_regex )
            AND ( $2::text IS NULL OR subject_regex IS NULL OR $2::text ~* subject_regex )
            AND ( $3::text IS NULL OR filename_regex IS NULL OR $3::text ~* filename_regex )
          ORDER BY priority ASC
          LIMIT 1`,
        [from || null, subject || null, filename || null]
      );

      let siteCode = "UNROUTED";
      let defType  = "unknown";
      let defUnit  = "";

      if (ruleRes.rowCount > 0) {
        siteCode = ruleRes.rows[0].site_code;
        defType  = ruleRes.rows[0].default_type || defType;
        defUnit  = ruleRes.rows[0].default_unit || defUnit;
      }

      // --- STICKY FALLBACK (sender → site)
      const fromKey = String((from || "").toLowerCase());
      if (siteCode === "UNROUTED" && fromKey) {
        const stick = await client.query(
          `SELECT site_code FROM sticky_sources WHERE source_key = $1`,
          [fromKey]
        );
        if (stick.rowCount) siteCode = stick.rows[0].site_code;
      }

      // Ensure site exists — but NEVER create UNROUTED    <<< changed
      if (siteCode !== "UNROUTED") {
        await client.query(
          `INSERT INTO sites (site_code, name) VALUES ($1,$1)
           ON CONFLICT DO NOTHING`,
          [siteCode]
        );
      }

      // Load canonical meters for this site + aliases
      const metersRows = await client.query(
        `SELECT meter_id, type, unit FROM meters WHERE site_code = $1`,
        [siteCode]
      );
      const canonical = metersRows.rows.map(r => ({
        meter_id: r.meter_id,
        type: r.type || "",
        unit: r.unit || "",
        key: normKey(r.meter_id)
      }));

      const aliasRows = await client.query(
        `SELECT alias, meter_id, type, unit
           FROM meter_aliases
          WHERE site_code = $1`,
        [siteCode]
      );
      const aliasMap = new Map();
      for (const r of aliasRows.rows) {
        aliasMap.set(normKey(r.alias), {
          meter_id: r.meter_id,
          type: r.type || "",
          unit: r.unit || ""
        });
      }

      // smart resolver + auto-learn alias (only if not UNROUTED)
      async function resolveMeterMeta(name) {
        const key = normKey(name);

        // alias hit
        const byAlias = aliasMap.get(key);
        if (byAlias) return byAlias;

        // direct meter_id exact
        const direct = canonical.find(c => c.meter_id === name);
        if (direct) {
          if (siteCode !== "UNROUTED") {
            await client.query(
              `INSERT INTO meter_aliases (site_code, alias, meter_id, type, unit)
               VALUES ($1,$2,$3,$4,$5)
               ON CONFLICT (site_code, alias)
               DO UPDATE SET meter_id=EXCLUDED.meter_id, type=EXCLUDED.type, unit=EXCLUDED.unit`,
              [siteCode, name, direct.meter_id, direct.type, direct.unit]
            );
            aliasMap.set(key, { meter_id: direct.meter_id, type: direct.type, unit: direct.unit });
          }
          return { meter_id: direct.meter_id, type: direct.type, unit: direct.unit };
        }

        // normalized equality to meter_id
        const normEq = canonical.find(c => c.key === key);
        if (normEq) {
          if (siteCode !== "UNROUTED") {
            await client.query(
              `INSERT INTO meter_aliases (site_code, alias, meter_id, type, unit)
               VALUES ($1,$2,$3,$4,$5)
               ON CONFLICT (site_code, alias)
               DO UPDATE SET meter_id=EXCLUDED.meter_id, type=EXCLUDED.type, unit=EXCLUDED.unit`,
              [siteCode, name, normEq.meter_id, normEq.type, normEq.unit]
            );
            aliasMap.set(key, { meter_id: normEq.meter_id, type: normEq.type, unit: normEq.unit });
          }
          return { meter_id: normEq.meter_id, type: normEq.type, unit: normEq.unit };
        }

        // fuzzy by tokens
        let best = null, bestScore = 0;
        for (const c of canonical) {
          const s = tokenSim(name, c.meter_id);
          if (s > bestScore) { best = c; bestScore = s; }
        }
        if (best && bestScore >= 0.6) {
          if (siteCode !== "UNROUTED") {
            await client.query(
              `INSERT INTO meter_aliases (site_code, alias, meter_id, type, unit)
               VALUES ($1,$2,$3,$4,$5)
               ON CONFLICT (site_code, alias)
               DO UPDATE SET meter_id=EXCLUDED.meter_id, type=EXCLUDED.type, unit=EXCLUDED.unit`,
              [siteCode, name, best.meter_id, best.type, best.unit]
            );
            aliasMap.set(key, { meter_id: best.meter_id, type: best.type, unit: best.unit });
          }
          return { meter_id: best.meter_id, type: best.type, unit: best.unit };
        }

        // fallback to generated id with routing defaults
        return { meter_id: toMeterId(name), type: defType, unit: defUnit };
        }

      // Parse CSV (auto-detect , or ;)
      const parsedBest = parseCsvBest(csv);
      const rowsCsv   = parsedBest.rows;
      const headers   = parsedBest.headers;

      if (!rowsCsv.length) {
        await client.query("ROLLBACK");
        return res.status(400).json({ error: "empty_csv" });
      }

      // Decide shape
      const firstColIsTimestamp = !!headers.length && normHeader(headers[0]).toLowerCase() === "timestamp";
      const hasNarrowFields = (() => {
        const H = headers.map(h => normHeader(h).toLowerCase());
        const hasTs = H.includes("timestamp");
        const hasMeterLike = H.includes("meter_id") || H.includes("point") || H.includes("pointname") || H.includes("name") || H.includes("dis");
        const hasValue = H.includes("value") || H.includes("reading") || H.includes("kwh") || H.includes("kw") || H.includes("flow") || H.includes("temperature");
        return hasTs && hasMeterLike && hasValue;
      })();

      let ingested = 0;

      if (firstColIsTimestamp && headers.length > 1) {
        // ------------ WIDE ------------
        const valueHeaders = headers.slice(1);

        // ensure meters exist (resolve + upsert)
        for (const h of valueHeaders) {
          const meta = await resolveMeterMeta(h);
          await client.query(
            `INSERT INTO meters (site_code, meter_id, type, unit)
             VALUES ($1,$2,$3,$4)
             ON CONFLICT (site_code, meter_id)
             DO UPDATE SET type=EXCLUDED.type, unit=EXCLUDED.unit`,
            [siteCode, meta.meter_id, meta.type || defType, meta.unit || defUnit]
          );
        }

        const insertReading = `INSERT INTO readings (site_code, meter_id, ts, value)
                               VALUES ($1,$2,$3,$4)
                               ON CONFLICT DO NOTHING`;

        for (const row of rowsCsv) {
          const ts = parseTimestampMaybe(row[headers[0]]);
          if (!ts) continue;

          for (const h of valueHeaders) {
            const v = parseNumber(row[h]);
            if (v === null) continue;
            const meta = await resolveMeterMeta(h);
            await client.query(insertReading, [siteCode, meta.meter_id, ts.toISOString(), v]);
            ingested++;
          }
        }
      } else if (hasNarrowFields) {
        // ------------ NARROW ------------
        const insertReading = `INSERT INTO readings (site_code, meter_id, ts, value)
                               VALUES ($1,$2,$3,$4)
                               ON CONFLICT DO NOTHING`;

        for (const row of rowsCsv) {
          const keys = Object.fromEntries(Object.entries(row).map(([k, v]) => [normHeader(k).toLowerCase(), v]));
          const ts = parseTimestampMaybe(keys["timestamp"]);
          const meterLike = keys["meter_id"] || keys["point"] || keys["pointname"] || keys["name"] || keys["dis"];
          const valueLike = keys["value"] ?? keys["reading"] ?? keys["kwh"] ?? keys["kw"] ?? keys["flow"] ?? keys["temperature"];

          if (!ts || !meterLike) continue;
          const meterMeta = await resolveMeterMeta(meterLike);
          const val = parseNumber(valueLike);
          if (val === null) continue;

          await client.query(
            `INSERT INTO meters (site_code, meter_id, type, unit)
             VALUES ($1,$2,$3,$4)
             ON CONFLICT (site_code, meter_id)
             DO UPDATE SET type=EXCLUDED.type, unit=EXCLUDED.unit`,
            [siteCode, meterMeta.meter_id, meterMeta.type || defType, meterMeta.unit || defUnit]
          );

          await client.query(insertReading, [siteCode, meterMeta.meter_id, ts.toISOString(), val]);
          ingested++;
        }
      } else {
        await client.query("ROLLBACK");
        return res.status(400).json({ error: "unrecognised_csv_shape", headers });
      }

      // remember sticky sender → site for next time (avoid UNROUTED)
      if (siteCode !== "UNROUTED" && fromKey) {
        await client.query(
          `INSERT INTO sticky_sources (source_key, site_code, last_seen)
           VALUES ($1,$2,now())
           ON CONFLICT (source_key) DO UPDATE
             SET site_code = EXCLUDED.site_code, last_seen = now()`,
          [fromKey, siteCode]
        );
      }

      await client.query("COMMIT");
      return res.json({ ok: true, site: siteCode, filename, ingested });
    } catch (e) {
      try { await client.query("ROLLBACK"); } catch {}
      console.error("email ingest error:", e);
      const out = { error: "server_error" };
      if (String(process.env.DEBUG_ERRORS || "") === "1") {
        out.detail = e?.message || String(e);
        if (e && typeof e === "object") {
          out.pg = {
            code: e.code, detail: e.detail, constraint: e.constraint,
            table: e.table, column: e.column, routine: e.routine
          };
        }
      }
      return res.status(500).json(out);
    } finally {
      client.release();
    }
  } catch (e) {
    console.error("email ingest outer error:", e);
    const out = { error: "server_error" };
    if (String(process.env.DEBUG_ERRORS || "") === "1") out.detail = e?.message || String(e);
    return res.status(500).json(out);
  }
});

// --- global error handler
app.use((err, req, res, _next) => {
  console.error("Unhandled error:", err);
  const out = { error: "server_error" };
  if (String(process.env.DEBUG_ERRORS || "") === "1") out.detail = err?.message || String(err);
  res.status(500).json(out);
});

const port = process.env.PORT || 8081;
app.listen(port, () => {
  console.log("API on " + port);
});






















