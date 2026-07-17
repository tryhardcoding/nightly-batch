// Nightly aggregation job. Reads a work list from the database, fetches public
// replay data for each item, sums paid amounts with a static FX table, and
// writes per-item totals back. Paced to be a polite client.

import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}
const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";

// Static conversion rates to JPY. Rough magnitudes are enough for this use.
const FX_TO_YEN = {
  JPY: 1, USD: 155, EUR: 168, GBP: 197, KRW: 0.11, TWD: 4.8,
  HKD: 20, CAD: 114, AUD: 103, PHP: 2.7, BRL: 28, INR: 1.85,
  SGD: 115, THB: 4.3, MXN: 8.5, IDR: 0.0096, MYR: 33, VND: 0.0061,
};
const CURRENCY_SYMBOL = {
  "¥": "JPY", "￥": "JPY", "$": "USD", "€": "EUR", "£": "GBP",
  "₩": "KRW", "NT$": "TWD", "HK$": "HKD", "CA$": "CAD", "A$": "AUD",
  "₱": "PHP", "R$": "BRL", "₹": "INR", "₫": "VND", "RM": "MYR",
  "Rp": "IDR", "฿": "THB",
};

function parseAmount(text) {
  if (!text) return null;
  const t = text.trim().replace(/ /g, " ");
  const symbols = Object.keys(CURRENCY_SYMBOL).sort((a, b) => b.length - a.length);
  let currency = null;
  for (const s of symbols) {
    if (t.includes(s)) { currency = CURRENCY_SYMBOL[s]; break; }
  }
  if (!currency) {
    const code = t.match(/\b([A-Z]{3})\b/);
    if (code && FX_TO_YEN[code[1]] !== undefined) currency = code[1];
  }
  if (!currency) return null;
  const num = t.replace(/[^\d.,]/g, "").replace(/,/g, "");
  const amount = Number.parseFloat(num);
  if (!Number.isFinite(amount)) return null;
  return { currency, amount };
}

const CLIENT = { clientName: "WEB", clientVersion: "2.20250701.01.00", hl: "ja" };
const MAX_PAGES = 400;
const PAGE_PAUSE_MS = 120;

async function processItem(videoId) {
  const watch = await fetch(`https://www.youtube.com/watch?v=${videoId}`, {
    headers: { "user-agent": UA, "accept-language": "ja" },
  });
  if (!watch.ok) return null;
  const html = await watch.text();
  const key = (html.match(/"INNERTUBE_API_KEY":"([^"]+)"/) || [])[1];
  const first = html.match(
    /"liveChatRenderer":\{"continuations":\[\{"reloadContinuationData":\{"continuation":"([^"]+)"/,
  );
  if (!key || !first) return null;
  let continuation = first[1];

  const breakdown = {};
  let count = 0;
  for (let i = 0; i < MAX_PAGES && continuation; i++) {
    const res = await fetch(
      `https://www.youtube.com/youtubei/v1/live_chat/get_live_chat_replay?key=${key}&prettyPrint=false`,
      {
        method: "POST",
        headers: { "content-type": "application/json", "user-agent": UA },
        body: JSON.stringify({ context: { client: CLIENT }, continuation }),
      },
    );
    if (!res.ok) break;
    const data = await res.json();
    const cont = data.continuationContents?.liveChatContinuation;
    if (!cont) break;
    for (const a of cont.actions ?? []) {
      const item =
        a.replayChatItemAction?.actions?.[0]?.addChatItemAction?.item;
      const paid =
        item?.liveChatPaidMessageRenderer || item?.liveChatPaidStickerRenderer;
      if (!paid) continue;
      const parsed = parseAmount(paid.purchaseAmountText?.simpleText);
      if (!parsed) continue;
      breakdown[parsed.currency] = (breakdown[parsed.currency] ?? 0) + parsed.amount;
      count++;
    }
    continuation =
      cont.continuations?.[0]?.liveChatReplayContinuationData?.continuation ??
      null;
    await new Promise((r) => setTimeout(r, PAGE_PAUSE_MS));
  }

  let totalYen = 0;
  for (const [cur, amt] of Object.entries(breakdown)) {
    totalYen += amt * (FX_TO_YEN[cur] ?? 0);
  }
  return { totalYen: Math.round(totalYen), count, breakdown };
}

async function mapPool(items, concurrency, fn) {
  const results = [];
  let idx = 0;
  const workers = Array.from({ length: concurrency }, async () => {
    while (idx < items.length) {
      const my = idx++;
      results[my] = await fn(items[my], my).catch(() => null);
    }
  });
  await Promise.all(workers);
  return results;
}

async function main() {
  const sinceDays = Number(process.env.WINDOW_DAYS ?? 30);
  const since = new Date(Date.now() - sinceDays * 864e5).toISOString();

  const { data: archives, error } = await sb
    .from("tracked_videos")
    .select("video_id, channel_id, peak_concurrent, published_at")
    .eq("live_status", "archive")
    .gte("published_at", since)
    .order("peak_concurrent", { ascending: false, nullsFirst: false })
    .limit(2000);
  if (error) throw error;

  const { data: done } = await sb
    .from("video_superchats")
    .select("video_id, harvested_at");
  const freshCut = Date.now() - 20 * 3600_000;
  const skip = new Set(
    (done ?? [])
      .filter((r) => new Date(r.harvested_at).getTime() > freshCut)
      .map((r) => r.video_id),
  );
  const targets = (archives ?? []).filter((r) => !skip.has(r.video_id));
  console.log(`targets: ${targets.length} / fresh-skipped: ${skip.size}`);

  let written = 0;
  const t0 = Date.now();
  await mapPool(targets, 8, async (row) => {
    const result = await processItem(row.video_id);
    if (!result) return;
    const { error: upErr } = await sb.from("video_superchats").upsert(
      {
        video_id: row.video_id,
        channel_id: row.channel_id,
        total_yen: result.totalYen,
        superchat_count: result.count,
        currency_breakdown: result.breakdown,
        harvested_at: new Date().toISOString(),
      },
      { onConflict: "video_id" },
    );
    if (!upErr) written++;
  });
  console.log(
    `done: ${written} rows in ${((Date.now() - t0) / 60000).toFixed(1)} min`,
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
