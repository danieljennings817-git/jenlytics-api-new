create extension if not exists pgcrypto;

create table if not exists sites (
  site_code text primary key,
  name      text not null
);

create table if not exists meters (
  site_code text not null references sites(site_code) on delete cascade,
  meter_id  text not null,
  type      text not null,   -- electric|water|heat
  unit      text not null,   -- kWh|m3|kW etc
  primary key(site_code, meter_id)
);

create table if not exists readings (
  site_code text not null,
  meter_id  text not null,
  ts        timestamptz not null,
  value     numeric not null,
  primary key (site_code, meter_id, ts),
  foreign key (site_code, meter_id) references meters(site_code, meter_id) on delete cascade
);
create index if not exists ix_readings_by_meter on readings(site_code, meter_id, ts desc);
