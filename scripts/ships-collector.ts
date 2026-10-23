// scripts/ships-collector.ts
//
// Fartygen runt Sundsvall nästan kontinuerligt (2026-10-23): en körning lyssnar på AISStream i 50 minuter och postar
// det som ändrats var 30:e sekund till proxyn (Lovable, /api/public/ships/ingest), som slår ihop rapporterna. När
// tiden är ute stänger den anslutningen, postar en sista gång och startar nästa körning av samma workflow: en
// "stafett", eftersom AISStreams gratiskonto bara tillåter en anslutning åt gången (workflowets concurrency ser till
// att två körningar aldrig överlappar). Bryts anslutningen mitt i återansluter den efter 5 s.
//
// Miljövariabler:
//   AISSTREAM_API_KEY    AISStreams nyckel (krävs)
//   SHIPS_INGEST_SECRET  proxyns ingest-nyckel (Bearer)
//   SHIPS_APP_URL        proxyns adress (standard sundsvall-transit-pulse.lovable.app)
//   AIS_SECONDS          körningens längd i sekunder (standard 3000 = 50 min)
//   AIS_POST_SECONDS     sekunder mellan postningar (standard 30)
//   AIS_BBOX             test: en annan ruta, JSON [[[lat,lon],[lat,lon]]]
//   AIS_DRY_RUN=1        test: bara räkna och skriva ut, skicka inget och starta ingen ny körning
//   GITHUB_TOKEN, GITHUB_REPOSITORY, GITHUB_REF_NAME   för att starta nästa körning (sätts av workflowet)

import { appendFileSync } from "node:fs";

const apiKey = process.env["AISSTREAM_API_KEY"] || process.env["AISSTREAM_KEY"];
const ingestSecret = process.env["SHIPS_INGEST_SECRET"] || apiKey;
const baseUrl = process.env["SHIPS_APP_URL"] || "https://sundsvall-transit-pulse.lovable.app";

if (!apiKey) {
  console.error("Saknar AISSTREAM_API_KEY");
  process.exit(1);
}

const ingestUrl = new URL("/api/public/ships/ingest", baseUrl).toString();
const RUN_MS = (Number(process.env["AIS_SECONDS"]) || 3000) * 1000;
const POST_MS = (Number(process.env["AIS_POST_SECONDS"]) || 30) * 1000;
const RECONNECT_MS = 5000;
const BBOX = process.env["AIS_BBOX"] ? JSON.parse(process.env["AIS_BBOX"]) : [[[62.15, 17.0], [62.75, 18.2]]];
const DRY = process.env["AIS_DRY_RUN"] === "1";

interface ShipReport {
  mmsi: string;
  name?: string | null;
  lat?: number;
  lon?: number;
  sog?: number | null;
  cog?: number | null;
  heading?: number | null;
  ship_type?: number | null;
  destination?: string | null;
  eta?: string | null;
  updated_at?: string;
}

const t0 = Date.now();
const reports = new Map<string, ShipReport>();
const changed = new Set<string>();   // MMSI som ändrats sedan senaste postningen
let total = 0, posts = 0, done = false, fatal = false;
let ws: WebSocket | null = null;

const stamp = () => `[${new Date().toISOString().slice(11, 19)} +${Math.round((Date.now() - t0) / 1000)}s]`;
const log = (...a: unknown[]) => console.log(stamp(), ...a);
const err = (...a: unknown[]) => console.error(stamp(), ...a);

function handle(raw: unknown) {
  let data: any;
  try { data = JSON.parse(String(raw)); } catch { return; }   // ignorera felaktiga paket
  total++;
  // AISStream svarar {"error": "..."} på fel nyckel eller prenumeration, och stänger sedan
  if (data.error || data.Error) {
    const msg = String(data.error || data.Error);
    err("AISStream svarade med fel:", JSON.stringify(data));
    if (/api ?key|apikey|unauthori[sz]ed|invalid key/i.test(msg)) {
      err("Nyckeln (AISSTREAM_API_KEY) verkar fel. Avbryter i stället för att återansluta i 50 minuter.");
      fatal = true; finish();
    }
    return;
  }
  if (total <= 3) log("Meddelande", total, data.MessageType, data.MetaData?.MMSI, data.MetaData?.ShipName);
  const mmsi = String(data.MetaData?.MMSI || "");
  if (!mmsi) return;
  const current = reports.get(mmsi) || { mmsi };
  if (data.MetaData?.ShipName) current.name = String(data.MetaData.ShipName).trim() || null;
  if (data.MessageType === "PositionReport") {
    const msg = data.Message?.PositionReport;
    if (msg) {
      current.lat = msg.Latitude;
      current.lon = msg.Longitude;
      current.sog = typeof msg.Sog === "number" ? msg.Sog : null;
      current.cog = typeof msg.Cog === "number" ? msg.Cog : null;
      current.heading = msg.TrueHeading === 511 ? null : (msg.TrueHeading ?? null);
      current.updated_at = new Date().toISOString();
    }
  } else if (data.MessageType === "ShipStaticData") {
    const msg = data.Message?.ShipStaticData;
    if (msg) {
      if (msg.Type !== undefined) current.ship_type = msg.Type;
      if (msg.Destination) current.destination = String(msg.Destination).trim() || null;
    }
  }
  reports.set(mmsi, current);
  changed.add(mmsi);
}

function connect() {
  if (done) return;
  log("Ansluter till AISStream WebSocket...");
  const sock = new WebSocket("wss://stream.aisstream.io/v0/stream");
  ws = sock;
  sock.onopen = () => {
    log("Ansluten. Skickar prenumeration...");
    sock.send(JSON.stringify({ APIKey: apiKey, BoundingBoxes: BBOX, FilterMessageTypes: ["PositionReport", "ShipStaticData"] }));
  };
  sock.onmessage = (ev) => handle(ev.data);
  sock.onerror = (e) => err("WebSocket-fel:", (e as any)?.message || e);
  sock.onclose = (ev) => {
    if (ws === sock) ws = null;
    log(`Anslutningen stängd (kod ${ev.code}${ev.reason ? ", " + ev.reason : ""}), ${total} meddelanden hittills.`);
    if (!done) { log(`Återansluter om ${RECONNECT_MS / 1000} s...`); setTimeout(connect, RECONNECT_MS); }
  };
}

// Postar det som ändrats sedan förra gången (all = true: hela kartan). 401 = fel nyckel: avbryt. Annat fel: logga,
// behåll ändringarna till nästa postning och fortsätt lyssna.
async function post(all = false): Promise<void> {
  const ids = all ? [...reports.keys()] : [...changed];
  const batch = ids.map((m) => reports.get(m)!).filter(Boolean);
  if (!batch.length) { log("Postning: inget nytt sedan förra."); return; }
  if (DRY) { log(`(torrkörning) skulle posta ${batch.length} fartyg, ${reports.size} kända`); changed.clear(); return; }
  for (const m of ids) changed.delete(m);
  try {
    const res = await fetch(ingestUrl, {
      method: "POST",
      headers: { Authorization: `Bearer ${ingestSecret}`, "Content-Type": "application/json" },
      body: JSON.stringify({ reports: batch }),
    });
    if (res.status === 401) {
      err("Inläsningen nekades (401): fel SHIPS_INGEST_SECRET. Avbryter.", await res.text().catch(() => ""));
      fatal = true; finish(); return;
    }
    if (!res.ok) {
      err(`Inläsningen misslyckades (${res.status}), försöker igen nästa gång:`, (await res.text().catch(() => "")).slice(0, 300));
      for (const m of ids) changed.add(m);
      return;
    }
    posts++;
    const result = await res.json().catch(() => ({}));
    log(`Postade ${batch.length} fartyg (${reports.size} kända, postning ${posts}).`, JSON.stringify(result).slice(0, 200));
  } catch (e) {
    err("Inläsningen misslyckades (nätverk), försöker igen nästa gång:", String(e));
    for (const m of ids) changed.add(m);
  }
}

// Startar nästa körning av samma workflow (stafetten). Sätter dispatched=true som steg-utdata, så att workflowets
// reservsteg vet att det redan är gjort.
async function relay(): Promise<void> {
  const token = process.env["GITHUB_TOKEN"], repo = process.env["GITHUB_REPOSITORY"];
  if (DRY || fatal) {
    log(DRY ? "Torrkörning: startar ingen ny körning." : "Avbröts på grund av fel: startar ingen ny körning (cron tar vid om en timme).");
    const out = process.env["GITHUB_OUTPUT"];
    if (out && fatal) appendFileSync(out, "stop=true\n");   // reservsteget ska inte heller starta en: fel nyckel loopar annars
    return;
  }
  if (!token || !repo) { log("Ingen GITHUB_TOKEN/GITHUB_REPOSITORY: startar ingen ny körning härifrån."); return; }
  const ref = process.env["GITHUB_REF_NAME"] || "main";
  try {
    const res = await fetch(`https://api.github.com/repos/${repo}/actions/workflows/ships-collector.yml/dispatches`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28" },
      body: JSON.stringify({ ref }),
    });
    if (res.status === 204) {
      log("Nästa körning startad (workflow_dispatch).");
      const out = process.env["GITHUB_OUTPUT"];
      if (out) appendFileSync(out, "dispatched=true\n");
    } else err(`Kunde inte starta nästa körning (${res.status}):`, (await res.text().catch(() => "")).slice(0, 300));
  } catch (e) { err("Kunde inte starta nästa körning:", String(e)); }
}

let finishing: Promise<void> | null = null;
function finish(): Promise<void> {
  if (finishing) return finishing;
  done = true;
  finishing = (async () => {
    clearInterval(postTimer); clearTimeout(endTimer);
    try { ws?.close(); } catch {}
    log(`Avslutar: ${reports.size} unika fartyg, ${total} meddelanden, ${posts} postningar.`);
    if (!fatal) await post(true);   // en sista gång, hela kartan
    if (DRY) for (const r of [...reports.values()].slice(0, 40)) console.log(r.mmsi, r.name ?? "", r.lat?.toFixed(3), r.lon?.toFixed(3), r.sog ?? "");
    await relay();
    process.exit(fatal ? 1 : 0);
  })();
  return finishing;
}

log(`Kör ${RUN_MS / 60000} minuter, postar var ${POST_MS / 1000}:e sekund till ${ingestUrl}${DRY ? " (torrkörning)" : ""}.`);
const postTimer = setInterval(() => { if (!done) void post(); }, POST_MS);
const endTimer = setTimeout(() => void finish(), RUN_MS);
for (const sig of ["SIGTERM", "SIGINT"] as const) process.on(sig, () => { log(`Fick ${sig}.`); void finish(); });
connect();
