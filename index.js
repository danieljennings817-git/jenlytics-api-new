import express from "express";
import { Pool } from "pg";
import cors from "cors";
import bodyParser from "body-parser";
import { parse as parseCsv } from "csv-parse/sync";
import 'dotenv/config';
import fs from "fs";
import path from "path";

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

const port = process.env.PORT || 8081;
app.listen(port, ()=> console.log("API on "+port));
