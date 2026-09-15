const { Redis } = require("@upstash/redis");

const redis = new Redis({
  url: process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN,
});

const PFX = "swl:"; // secondarypart worklog namespace, in case the Redis DB is shared
const KEYS = {
  MEMBERS: PFX + "members",
  ENTRIES: PFX + "entries",
  REQUESTS: PFX + "requests",
  NOTICES: PFX + "notices",
  SCHEDULE: PFX + "schedule",
  PROJECTS: PFX + "projects",
};

const DEFAULT_MEMBERS = { m1: "임유미", m2: "양선영", m3: "이한나", m4: "조윤서" };

async function getJson(key, fallback) {
  const v = await redis.get(key);
  if (v === null || v === undefined) return fallback;
  // @upstash/redis auto-deserializes JSON-looking strings, but guard anyway
  if (typeof v === "string") {
    try { return JSON.parse(v); } catch (e) { return fallback; }
  }
  return v;
}

async function setJson(key, value) {
  await redis.set(key, JSON.stringify(value));
}

async function ensureMembers() {
  let members = await getJson(KEYS.MEMBERS, null);
  if (!members) {
    members = DEFAULT_MEMBERS;
    await setJson(KEYS.MEMBERS, members);
  }
  return members;
}

function nowIso() {
  return new Date().toISOString();
}

// ---------- API handlers (mirror the earlier Apps Script backend 1:1) ----------

async function api_getAll() {
  const members = await ensureMembers();
  const entries = await getJson(KEYS.ENTRIES, []);
  const requests = await getJson(KEYS.REQUESTS, []);
  const notices = await getJson(KEYS.NOTICES, []);
  const schedule = await getJson(KEYS.SCHEDULE, []);
  const projects = await getJson(KEYS.PROJECTS, []);

  const sortedEntries = [...entries].sort((a, b) => (a.date < b.date ? 1 : a.date > b.date ? -1 : 0)).slice(0, 500);

  return { members, entries: sortedEntries, requests, notices, schedule, projects };
}

async function api_saveEntry(payload) {
  const entries = await getJson(KEYS.ENTRIES, []);
  const requests = await getJson(KEYS.REQUESTS, []);
  const id = payload.memberId + "__" + payload.date;
  const ts = nowIso();

  const entryObj = {
    id,
    memberId: payload.memberId,
    date: payload.date,
    tasks: payload.tasks || [],
    issues: payload.issues || "",
    tomorrowPlan: payload.tomorrowPlan || "",
    request: payload.request || "",
    requestTo: payload.requestTo || "",
    updatedAt: ts,
  };

  const idx = entries.findIndex((e) => e.id === id);
  if (idx >= 0) entries[idx] = entryObj;
  else entries.push(entryObj);
  await setJson(KEYS.ENTRIES, entries);

  // Requests accumulate independently of the day's entry — saving the same
  // entry again (e.g. later that day, or a different request afterward) must
  // never erase a request that was already sent to someone. We only add a
  // NEW request row when the current text/target isn't already an open
  // request linked to this entry; re-saving the exact same open request is a
  // no-op so we don't spam duplicates on every keystroke-save. Clearing the
  // request field on a later save does NOT retract requests already sent —
  // those stay visible until the recipient marks them done. A cancelled
  // request must NOT match here, or re-sending the same text after
  // cancelling it would silently resurrect the cancelled row instead of
  // creating a fresh request.
  let savedRequest = null;
  if (payload.requestTo && payload.request) {
    const existingOpenSame = requests.find(
      (r) => r.entryId === id && r.to === payload.requestTo && r.text === payload.request && !r.done && !r.cancelled
    );
    if (existingOpenSame) {
      savedRequest = existingOpenSame;
    } else {
      savedRequest = {
        id: id + "__" + ts + "__" + Math.random().toString(36).slice(2, 8),
        entryId: id,
        from: payload.memberId,
        to: payload.requestTo,
        text: payload.request,
        date: payload.date,
        createdAt: ts,
        done: false,
      };
      requests.push(savedRequest);
      await setJson(KEYS.REQUESTS, requests);
    }
  }

  return {
    entry: {
      memberId: entryObj.memberId, date: entryObj.date, tasks: entryObj.tasks,
      issues: entryObj.issues, tomorrowPlan: entryObj.tomorrowPlan,
      request: entryObj.request, requestTo: entryObj.requestTo, updatedAt: entryObj.updatedAt,
    },
    request: savedRequest,
  };
}

async function api_renameMember(id, name) {
  const members = await ensureMembers();
  members[id] = name;
  await setJson(KEYS.MEMBERS, members);
  return { id, name };
}

// The recipient can leave a short comment when marking a request done (what
// they actually did), so the sender gets more than a silent status flip.
// The request isn't cleared from the sender's list yet at this point — it
// stays visible with the comment until the sender acknowledges it (see
// api_acknowledgeRequest), so the sender doesn't miss the confirmation.
async function api_markRequestDone(requestId, comment) {
  const requests = await getJson(KEYS.REQUESTS, []);
  const row = requests.find((r) => r.id === requestId);
  if (!row) return null;
  row.done = true;
  row.doneComment = (comment || "").trim();
  row.doneAt = nowIso();
  row.acknowledged = false;
  await setJson(KEYS.REQUESTS, requests);
  return { id: requestId, done: true, doneComment: row.doneComment, doneAt: row.doneAt };
}

// The sender reads the recipient's done comment, then acknowledges it —
// only then does the request disappear from the sender's list too.
async function api_acknowledgeRequest(requestId) {
  const requests = await getJson(KEYS.REQUESTS, []);
  const row = requests.find((r) => r.id === requestId);
  if (!row) return null;
  row.acknowledged = true;
  await setJson(KEYS.REQUESTS, requests);
  return { id: requestId, acknowledged: true };
}

// The sender can withdraw a request they no longer need (sent by mistake,
// duplicate, or the need went away). This is distinct from "done" — done
// means the recipient handled it, cancelled means the sender pulled it back
// — so a cancelled request stops showing up for the recipient but the
// sender still sees a brief "취소됨" record of it (trimmed client-side same
// as done ones, so it doesn't accumulate forever).
async function api_cancelRequest(requestId) {
  const requests = await getJson(KEYS.REQUESTS, []);
  const row = requests.find((r) => r.id === requestId);
  if (!row) return null;
  if (row.done) return { id: requestId, done: true, cancelled: false }; // already handled, nothing to cancel
  row.cancelled = true;
  await setJson(KEYS.REQUESTS, requests);
  return { id: requestId, cancelled: true };
}

async function api_saveNotice(weekStart, author, text) {
  const notices = await getJson(KEYS.NOTICES, []);
  const ts = nowIso();
  const obj = { id: weekStart, weekStart, author, text, updatedAt: ts };
  const idx = notices.findIndex((n) => n.weekStart === weekStart);
  if (idx >= 0) notices[idx] = obj;
  else notices.push(obj);
  await setJson(KEYS.NOTICES, notices);
  return obj;
}

async function api_saveSchedule(date, text, author) {
  const schedule = await getJson(KEYS.SCHEDULE, []);
  const idx = schedule.findIndex((s) => s.date === date);
  if (text) {
    const ts = nowIso();
    const obj = { id: date, date, text, author, updatedAt: ts };
    if (idx >= 0) schedule[idx] = obj;
    else schedule.push(obj);
    await setJson(KEYS.SCHEDULE, schedule);
    return obj;
  }
  if (idx >= 0) {
    schedule.splice(idx, 1);
    await setJson(KEYS.SCHEDULE, schedule);
  }
  return null;
}

// Team-wide project/event periods (pop-up stores, flea markets, etc.) shown
// as a highlighted band across their date range on the monthly calendar.
// Unlike the per-date schedule note above, this spans multiple days.
async function api_saveProject(payload) {
  const projects = await getJson(KEYS.PROJECTS, []);
  const title = ((payload && payload.title) || "").trim();
  let startDate = payload && payload.startDate;
  let endDate = payload && payload.endDate;
  if (!title || !startDate || !endDate) return null;
  if (startDate > endDate) {
    const t = startDate; startDate = endDate; endDate = t;
  }
  const ts = nowIso();
  const id = (payload && payload.id) || ("proj_" + Date.now() + "_" + Math.random().toString(36).slice(2, 8));
  const obj = {
    id,
    title,
    startDate,
    endDate,
    color: (payload && payload.color) || "yellow",
    author: (payload && payload.author) || "",
    createdAt: ts,
  };
  const idx = projects.findIndex((p) => p.id === id);
  if (idx >= 0) projects[idx] = obj;
  else projects.push(obj);
  await setJson(KEYS.PROJECTS, projects);
  return obj;
}

async function api_deleteProject(id) {
  const projects = await getJson(KEYS.PROJECTS, []);
  const next = projects.filter((p) => p.id !== id);
  await setJson(KEYS.PROJECTS, next);
  return { removed: projects.length - next.length };
}

const HANDLERS = {
  api_getAll,
  api_saveEntry,
  api_renameMember,
  api_markRequestDone,
  api_acknowledgeRequest,
  api_cancelRequest,
  api_saveNotice,
  api_saveSchedule,
  api_saveProject,
  api_deleteProject,
};

module.exports = async (req, res) => {
  if (req.method !== "POST") {
    res.status(405).json({ error: "POST only" });
    return;
  }
  try {
    const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : (req.body || {});
    const { fn, args } = body;
    const handler = HANDLERS[fn];
    if (!handler) {
      res.status(400).json({ error: "Unknown function: " + fn });
      return;
    }
    const result = await handler(...(args || []));
    res.status(200).json(result);
  } catch (err) {
    res.status(500).json({ error: String((err && err.message) || err) });
  }
};
