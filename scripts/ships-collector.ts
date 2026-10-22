// scripts/ships-collector.ts
const apiKey = process.env["AISSTREAM_API_KEY"] || process.env["AISSTREAM_KEY"];
const ingestSecret = process.env["SHIPS_INGEST_SECRET"] || apiKey;
const baseUrl = process.env["SHIPS_APP_URL"] || "https://sundsvall-transit-pulse.lovable.app";

if (!apiKey) {
  console.error("Saknar AISSTREAM_API_KEY");
  process.exit(1);
}

const ingestUrl = new URL("/api/public/ships/ingest", baseUrl).toString();
// 4 minutes: a moored or anchored ship reports its position only every 3 minutes, so 45 s missed most of them
const COLLECT_MS = (Number(process.env["AIS_SECONDS"]) || 240) * 1000;
// Koordinater för Sundsvall / Alnösundet
const BBOX = process.env["AIS_BBOX"] ? JSON.parse(process.env["AIS_BBOX"]) : [[[62.25, 17.15], [62.50, 17.80]]];
// AIS_DRY_RUN=1: only count and print (a coverage test), send nothing
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

async function collectShips(): Promise<ShipReport[]> {
  const reports = new Map<string, ShipReport>();
  let total = 0;

  return new Promise((resolve) => {
    console.log("Ansluter till AISStream WebSocket...");
    const ws = new WebSocket("wss://stream.aisstream.io/v0/stream");

    const timer = setTimeout(() => {
      console.log(`${COLLECT_MS / 1000} sekunder har gått. Stänger anslutningen. Mottog ${reports.size} unika fartyg.`);
      ws.close();
      resolve(Array.from(reports.values()));
    }, COLLECT_MS);

    ws.onopen = () => {
      console.log("Ansluten. Skickar prenumeration...");
      ws.send(JSON.stringify({
        APIKey: apiKey,
        BoundingBoxes: BBOX,
        FilterMessageTypes: ["PositionReport", "ShipStaticData"]
      }));
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(String(event.data));
        total++;
        // AISStream answers a bad key or subscription with {"error": "..."} and closes: say so
        if (data.error || data.Error) { console.error("AISStream svarade med fel:", JSON.stringify(data)); return; }
        if (total <= 3) console.log("Meddelande", total, data.MessageType, data.MetaData?.MMSI, data.MetaData?.ShipName);
        const mmsi = String(data.MetaData?.MMSI || "");
        if (!mmsi) return;

        const current = reports.get(mmsi) || { mmsi };

        if (data.MetaData?.ShipName) {
          current.name = String(data.MetaData.ShipName).trim() || null;
        }

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
      } catch {
        // Ignorera felaktiga paket
      }
    };

    ws.onclose = (ev) => {
      console.log(`Anslutningen stängd (kod ${ev.code}${ev.reason ? ", " + ev.reason : ""}), ${total} meddelanden totalt.`);
    };

    ws.onerror = (err) => {
      console.error("WebSocket-fel:", err);
    };
  });
}

async function run() {
  const reports = await collectShips();
  if (reports.length === 0) {
    console.log("Inga fartygspositioner fångades upp under denna period.");
    return;
  }

  if (DRY) {
    for (const r of reports.slice(0, 40)) console.log(r.mmsi, r.name ?? "", r.lat?.toFixed(3), r.lon?.toFixed(3), r.sog ?? "");
    return;
  }
  console.log(`Skickar ${reports.length} fartyg till proxyn: ${ingestUrl}`);
  const res = await fetch(ingestUrl, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${ingestSecret}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ reports })
  });

  if (!res.ok) {
    const text = await res.text();
    console.error(`Inläsningen misslyckades (${res.status}):`, text);
    process.exit(1);
  }

  const result = await res.json();
  console.log("Lyckades spara fartygsrapporter!", result);
}

run();
