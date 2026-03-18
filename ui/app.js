/* ── State ──────────────────────────────────────────────────────────────── */
const state = {
  data: {},
  activeTab: "dashboard",
  chartInstance: null,
  horizon: "weekly",
};

/* ── Data loading ───────────────────────────────────────────────────────── */
async function loadJson(path) {
  const r = await fetch(path, { cache: "no-store" });
  if (!r.ok) throw new Error(`${path} → ${r.status}`);
  return r.json();
}

async function loadAll() {
  const files = {
    status:       "/json/ingestion_status_latest.json",
    teamScores:   "/json/team_scores.json",
    teamClaps:    "/json/team_CLAPS.json",
    leagueClap:   "/json/ldbCLAP.json",
    xmatchups:    "/json/ldb_xmatchups.json",
    weekMatchups: "/json/week_matchups.json",
    weeklyScores: "/json/weeklyScores.json",
    transactions: "/json/transactions_latest.json",
    schedule:     "/json/schedule.json",
    viewLeagueDaily:  "/json/view_league_daily_latest.json",
    viewLeagueWeekly: "/json/view_league_weekly_latest.json",
    viewGmDaily:      "/json/view_gm_daily_latest.json",
    viewGmWeekly:     "/json/view_gm_weekly_latest.json",
    freeAgents:       "/json/free_agent_candidates_latest.json",
    weeklyDigest:     "/json/weekly_digest_latest.json",
    teamWeeklyTotalsLatest: "/json/team_weekly_totals_latest.json",
    clapPlayerHistory:      "/json/clap_player_history_latest.json",
    matchupExpectations:    "/json/matchup_expectations_latest.json",
    clapCalibration:        "/json/clap_calibration_latest.json",
  };

  const results = await Promise.allSettled(
    Object.entries(files).map(async ([k, p]) => [k, await loadJson(p)])
  );

  results.forEach(r => {
    if (r.status === "fulfilled") {
      const [key, val] = r.value;
      state.data[key] = val;
    }
  });
}

/* ── Utilities ──────────────────────────────────────────────────────────── */
function fmt(v, decimals = 2) {
  const n = Number(v);
  return isNaN(n) ? "—" : n.toFixed(decimals);
}

function fmtPct(v) {
  const n = Number(v);
  return isNaN(n) ? "—" : (n * 100).toFixed(1) + "%";
}

function el(tag, attrs = {}, children = []) {
  const e = document.createElement(tag);
  Object.entries(attrs).forEach(([k, v]) => {
    if (k === "class") e.className = v;
    else if (k === "text") e.textContent = v;
    else e.setAttribute(k, v);
  });
  children.forEach(c => e.appendChild(typeof c === "string" ? document.createTextNode(c) : c));
  return e;
}

function td(text, cls = "") {
  const e = document.createElement("td");
  e.textContent = text ?? "—";
  if (cls) e.className = cls;
  return e;
}

function th(text, cls = "") {
  const e = document.createElement("th");
  e.textContent = text;
  if (cls) e.className = cls;
  return e;
}

function clearEl(id) {
  const e = document.getElementById(id);
  if (e) e.innerHTML = "";
  return e;
}

function setText(id, text) {
  const e = document.getElementById(id);
  if (e) e.textContent = text;
}

function ageHours(isoUtc) {
  if (!isoUtc) return null;
  const ts = Date.parse(isoUtc);
  if (Number.isNaN(ts)) return null;
  return (Date.now() - ts) / 3600000;
}

/* ── Tab navigation ─────────────────────────────────────────────────────── */
function switchTab(tab) {
  state.activeTab = tab;
  document.querySelectorAll(".tab").forEach(b => b.classList.toggle("active", b.dataset.tab === tab));
  document.querySelectorAll(".view").forEach(v => v.classList.toggle("active", v.id === `view-${tab}`));
  if (tab === "trends") renderTrends();
}

document.querySelectorAll(".tab").forEach(b => {
  b.addEventListener("click", () => switchTab(b.dataset.tab));
});

/* ── Header ─────────────────────────────────────────────────────────────── */
function renderHeader() {
  const status = state.data.status || {};
  const badge  = document.getElementById("statusBadge");
  const s      = status.status || "unknown";
  badge.textContent = s;
  badge.className   = `badge badge-${s === "ok" ? "ok" : s === "warn" ? "warn" : s === "error" ? "error" : "unknown"}`;

  const ts = status.last_success_utc || status.generated_at_utc;
  if (ts) {
    const d = new Date(ts);
    setText("asOf", `Updated ${d.toLocaleDateString()} ${d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`);
    setText(
      "lastNightlyRun",
      `Last nightly run: ${d.toLocaleDateString()} ${d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`
    );
  } else {
    setText("lastNightlyRun", "Last nightly run: unavailable");
  }
}

/* ── Dashboard view ─────────────────────────────────────────────────────── */
function renderDashboard() {
  const ws     = state.data.weeklyScores || {};
  const clap   = state.data.leagueClap  || {};
  const ts     = state.data.teamScores  || {};
  const status = state.data.status      || {};
  const tx     = state.data.transactions || {};

  // Week metadata
  const weeks = Object.keys(ws).sort((a, b) => +a.replace("week","") - +b.replace("week",""));
  const latestWeek = weeks[weeks.length - 1] || "week1";
  const weekNum = latestWeek.replace("week", "");
  setText("dash-weeks", weekNum);

  const allTeams = Object.keys(ts);
  setText("dash-teams", `${allTeams.length} teams`);
  setText("dash-week-label", `Week ${weekNum}`);

  // Average wins per week across all teams
  let totalWins = 0, totalMatches = 0;
  allTeams.forEach(t => {
    const rec = ts[t].season_record || [0, 0];
    totalWins    += rec[0];
    totalMatches += rec[0] + rec[1];
  });
  const avgWins = totalMatches > 0 ? (totalWins / (totalMatches / 12)).toFixed(1) : "—";
  setText("dash-winrate", avgWins);

  // Ingestion detail
  const resources = (status.resources || []).map(r => `${r.name}: ${r.status}`).join(" · ");
  setText("dash-ingest-detail", resources || "—");

  // Latest week scorers table — sort by R descending
  const body = clearEl("dash-weekly-body");
  const weekData = ws[latestWeek] || {};
  const sortedTeams = Object.entries(weekData)
    .sort(([, a], [, b]) => Number(b.R) - Number(a.R));

  sortedTeams.forEach(([team, scores]) => {
    const tr = document.createElement("tr");
    tr.appendChild(td(team, "team-name"));
    tr.appendChild(td(fmt(scores.R, 0), "num"));
    tr.appendChild(td(fmt(scores.HR, 0), "num"));
    tr.appendChild(td(fmt(scores.OPS, 3), "num"));
    tr.appendChild(td(fmt(scores.K, 0), "num"));
    tr.appendChild(td(fmt(scores.ERA, 2), "num"));
    tr.appendChild(td(fmt(scores.VIJAY, 2), "num"));
    body.appendChild(tr);
  });

  // CLAP league averages grid
  const clapGrid = clearEl("dash-clap-grid");
  const clapDisplay = [
    ["R",      "Runs",    fmt(clap.R?.[0], 1)],
    ["HR",     "HR",      fmt(clap.HR?.[0], 1)],
    ["OPS",    "OPS",     fmt(clap.OPS?.[0], 3)],
    ["K",      "K",       fmt(clap.K?.[0], 1)],
    ["ERA",    "ERA",     fmt(clap.ERA?.[0], 2)],
    ["aWHIP",  "WHIP",    fmt(clap.aWHIP?.[0], 3)],
    ["aSB",    "aSB",     fmt(clap.aSB?.[0], 1)],
    ["aRBI",   "aRBI",    fmt(clap.aRBI?.[0], 1)],
    ["VIJAY",  "VIJAY",   fmt(clap.VIJAY?.[0], 2)],
    ["NQW",    "NQW",     fmt(clap.NQW?.[0], 1)],
    ["HRA",    "HRA",     fmt(clap.HRA?.[0], 1)],
    ["OBP",    "OBP",     fmt(clap.OBP?.[0], 3)],
  ];
  clapDisplay.forEach(([, label, val]) => {
    const cell = el("div", { class: "stat-cell" });
    cell.appendChild(el("div", { class: "stat-cell-label", text: label }));
    cell.appendChild(el("div", { class: "stat-cell-value", text: val }));
    clapGrid.appendChild(cell);
  });

  // Transactions
  const events = tx.events || [];
  setText("dash-tx-count", `${events.length} total events`);
  const txBody = clearEl("dash-tx-body");
  events.slice(-15).reverse().forEach(ev => {
    const tr = document.createElement("tr");
    tr.appendChild(td(ev.type || ev.transaction_type || "—", "chip chip-gray"));
    tr.appendChild(td(ev.player_name || ev.player || "—"));
    tr.appendChild(td(ev.team_name || ev.team || "—", "dim"));
    const dateStr = ev.date || ev.timestamp_utc || "—";
    tr.appendChild(td(dateStr.slice(0, 10), "dim"));
    txBody.appendChild(tr);
  });
  if (events.length === 0) {
    const tr = document.createElement("tr");
    const c = document.createElement("td");
    c.colSpan = 4;
    c.textContent = "No transactions found";
    c.className = "dim";
    c.style.padding = "16px 10px";
    tr.appendChild(c);
    txBody.appendChild(tr);
  }

  renderModelOpsPanel();
  renderClapProvenancePanel();
  renderClapCalibrationPanel();
  renderPlayerSeasonTotalsPanel();
}

function renderClapProvenancePanel() {
  const body = clearEl("dash-clap-provenance-body");
  const payload = state.data.matchupExpectations || {};
  const firstMatchup = (payload.matchups || [])[0] || {};
  const selected = firstMatchup.selected || {};
  const categories = selected.categories || {};
  if (!Object.keys(categories).length) {
    setText("dash-clap-provenance-source", "Source: unavailable");
    const tr = document.createElement("tr");
    const c = document.createElement("td");
    c.colSpan = 2;
    c.className = "dim";
    c.style.padding = "16px 10px";
    c.textContent = "No CLAP provenance data available";
    tr.appendChild(c);
    body.appendChild(tr);
    return;
  }
  setText("dash-clap-provenance-source", "Source: matchup_expectations_latest.json");
  const ordered = ["R", "HR", "OPS", "OBP", "aRBI", "aSB", "K", "ERA", "aWHIP", "NQW", "VIJAY", "HRA"];
  const categoryList = ordered.filter((cat) => Object.prototype.hasOwnProperty.call(categories, cat));
  Object.keys(categories).sort().forEach((cat) => {
    if (!categoryList.includes(cat)) categoryList.push(cat);
  });
  categoryList.forEach((category) => {
    const tr = document.createElement("tr");
    tr.appendChild(td(category, category === "VIJAY" ? "team-name" : ""));
    const src = String(categories[category]?.category_source || "unknown");
    const cell = document.createElement("td");
    const cls =
      category === "VIJAY"
        ? (src === "appearance_summed" ? "chip chip-green" : "chip chip-red")
        : (src === "component_derived" ? "chip chip-gray" : src === "appearance_summed" ? "chip chip-green" : "chip chip-red");
    cell.appendChild(el("span", { class: cls, text: src }));
    tr.appendChild(cell);
    body.appendChild(tr);
  });
}

function renderClapCalibrationPanel() {
  const body = clearEl("dash-clap-calib-body");
  const payload = state.data.clapCalibration || {};
  const metrics = payload.metrics || {};
  const analytic = metrics.analytic_normal || {};
  const monteCarlo = metrics.monte_carlo || {};
  if (!Object.keys(metrics).length) {
    setText("dash-clap-calib-source", "Source: unavailable");
    const tr = document.createElement("tr");
    const c = document.createElement("td");
    c.colSpan = 4;
    c.className = "dim";
    c.style.padding = "16px 10px";
    c.textContent = "No CLAP calibration diagnostics available";
    tr.appendChild(c);
    body.appendChild(tr);
    return;
  }
  setText("dash-clap-calib-source", "Source: clap_calibration_latest.json");
  const rows = [
    {
      label: "overall",
      analyticBrier: analytic.brier_score,
      monteBrier: monteCarlo.brier_score,
      samples: Math.max(Number(analytic.samples || 0), Number(monteCarlo.samples || 0)),
    },
  ];
  const roleSegments = metrics.role_segments || {};
  ["batters", "rp", "sp"].forEach((role) => {
    const rs = roleSegments[role] || {};
    rows.push({
      label: `role:${role}`,
      analyticBrier: rs.analytic_normal?.brier_score,
      monteBrier: rs.monte_carlo?.brier_score,
      samples: Math.max(Number(rs.analytic_normal?.samples || 0), Number(rs.monte_carlo?.samples || 0)),
    });
  });
  const sourceSeg = metrics.category_source_diagnostics?.source_segments || {};
  ["component_derived", "appearance_summed"].forEach((sourceName) => {
    const ss = sourceSeg[sourceName] || {};
    rows.push({
      label: `source:${sourceName}`,
      analyticBrier: ss.analytic_normal?.brier_score,
      monteBrier: ss.monte_carlo?.brier_score,
      samples: Math.max(Number(ss.analytic_normal?.samples || 0), Number(ss.monte_carlo?.samples || 0)),
    });
  });
  const spBuckets = metrics.sp_start_buckets || {};
  Object.entries(spBuckets).forEach(([bucketName, bucketData]) => {
    const b = bucketData || {};
    rows.push({
      label: `sp:${bucketName}`,
      analyticBrier: b.analytic_normal?.brier_score,
      monteBrier: b.monte_carlo?.brier_score,
      samples: Math.max(Number(b.analytic_normal?.samples || 0), Number(b.monte_carlo?.samples || 0)),
    });
  });
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    tr.appendChild(td(row.label));
    tr.appendChild(td(fmt(row.analyticBrier, 4), "num"));
    tr.appendChild(td(fmt(row.monteBrier, 4), "num"));
    tr.appendChild(td(String(row.samples || 0), "num"));
    body.appendChild(tr);
  });
}

function renderPlayerSeasonTotalsPanel() {
  const body = clearEl("dash-season-players-body");
  const payload = state.data.teamWeeklyTotalsLatest || {};
  const rawTeams = payload.season_roto?.teams;
  const seasonTeams = Array.isArray(rawTeams) ? rawTeams : (rawTeams && typeof rawTeams === "object" ? Object.values(rawTeams) : []);
  if (!seasonTeams.length) {
    setText("dash-season-players-source", "Source: unavailable");
    const tr = document.createElement("tr");
    const c = document.createElement("td");
    c.colSpan = 12;
    c.className = "dim";
    c.style.padding = "16px 10px";
    c.textContent = "No season player totals available";
    tr.appendChild(c);
    body.appendChild(tr);
    return;
  }
  setText("dash-season-players-source", "Source: team_weekly_totals_latest.json season_roto.players");
  const rows = [];
  seasonTeams.forEach((team) => {
    const teamLabel = team.team_abbr || team.team_name || team.team_id || "—";
    const rawPlayers = team.players;
    const players = Array.isArray(rawPlayers) ? rawPlayers : (rawPlayers && typeof rawPlayers === "object" ? Object.values(rawPlayers) : []);
    players.forEach((player) => {
      rows.push({ teamLabel, player });
    });
  });
  rows.sort((a, b) => {
    const byTeam = String(a.teamLabel).localeCompare(String(b.teamLabel));
    if (byTeam !== 0) return byTeam;
    return String(a.player?.player_name || a.player?.player_id || "").localeCompare(String(b.player?.player_name || b.player?.player_id || ""));
  });
  rows.forEach(({ teamLabel, player }) => {
    const c = player?.category_totals || {};
    const tr = document.createElement("tr");
    tr.appendChild(td(teamLabel));
    tr.appendChild(td(player?.player_name || player?.player_id || "—", "team-name"));
    tr.appendChild(td(player?.status || "—", "dim"));
    tr.appendChild(td(fmt(c.HR, 0), "num"));
    tr.appendChild(td(fmt(c.R, 0), "num"));
    tr.appendChild(td(fmt(c.OBP, 3), "num"));
    tr.appendChild(td(fmt(c.OPS, 3), "num"));
    tr.appendChild(td(fmt(c.K, 0), "num"));
    tr.appendChild(td(fmt(c.ERA, 3), "num"));
    tr.appendChild(td(fmt(c.aWHIP, 3), "num"));
    tr.appendChild(td(fmt(c.MGS, 2), "num"));
    tr.appendChild(td(fmt(c.VIJAY, 2), "num"));
    body.appendChild(tr);
  });
}

function renderModelOpsPanel() {
  const horizonLabel = state.horizon === "daily" ? "Daily" : "Weekly";
  setText("dash-horizon-label-left", horizonLabel);
  setText("dash-horizon-label-right", horizonLabel);
  const weeklyBtn = document.getElementById("horizon-weekly");
  const dailyBtn = document.getElementById("horizon-daily");
  if (weeklyBtn && dailyBtn) {
    weeklyBtn.classList.toggle("active", state.horizon === "weekly");
    dailyBtn.classList.toggle("active", state.horizon === "daily");
  }

  const league = state.horizon === "daily" ? state.data.viewLeagueDaily || {} : state.data.viewLeagueWeekly || {};
  const gm = state.horizon === "daily" ? state.data.viewGmDaily || {} : state.data.viewGmWeekly || {};

  // Freshness table
  const freshnessBody = clearEl("dash-freshness-body");
  const freshnessRows = [
    ["ingestion_status", state.data.status?.generated_at_utc || state.data.status?.last_success_utc],
    ["view_league_" + state.horizon, league.generated_at_utc],
    ["view_gm_" + state.horizon, gm.generated_at_utc],
    ["free_agent_candidates", state.data.freeAgents?.generated_at_utc],
    ["weekly_digest", state.data.weeklyDigest?.generated_at_utc],
  ];
  freshnessRows.forEach(([name, iso]) => {
    const tr = document.createElement("tr");
    const hours = ageHours(iso);
    let status = "unknown";
    let cls = "chip chip-gray";
    if (hours !== null) {
      if (hours <= 24) { status = "fresh"; cls = "chip chip-green"; }
      else if (hours <= 48) { status = "aging"; cls = "chip chip-gray"; }
      else { status = "stale"; cls = "chip chip-red"; }
    }
    tr.appendChild(td(name));
    tr.appendChild(td(iso ? iso.replace("T", " ").replace("Z", " UTC") : "—", "dim"));
    tr.appendChild(td(hours === null ? "—" : hours.toFixed(1), "num"));
    const statusTd = document.createElement("td");
    statusTd.appendChild(el("span", { class: cls, text: status }));
    tr.appendChild(statusTd);
    freshnessBody.appendChild(tr);
  });

  // Overperformers table
  const overBody = clearEl("dash-over-body");
  const overs = (league.leaders?.overperformers || []).slice(0, 10);
  overs.forEach(rowData => {
    const tr = document.createElement("tr");
    tr.appendChild(td(rowData.player_name || "—"));
    tr.appendChild(td(rowData.player_role || "—", "dim"));
    tr.appendChild(td(fmt(rowData.performance_delta, 2), "num"));
    tr.appendChild(td(rowData.performance_flag || "—"));
    overBody.appendChild(tr);
  });
  if (overs.length === 0) {
    const tr = document.createElement("tr");
    const c = document.createElement("td");
    c.colSpan = 4;
    c.textContent = "No overperformer data found for selected horizon";
    c.className = "dim";
    c.style.padding = "16px 10px";
    tr.appendChild(c);
    overBody.appendChild(tr);
  }

  // GM projection table
  const gmBody = clearEl("dash-gm-horizon-body");
  const gmRows = (gm.players || []).slice(0, 10);
  gmRows.forEach(rowData => {
    const tr = document.createElement("tr");
    tr.appendChild(td(rowData.player_name || "—"));
    tr.appendChild(td(rowData.player_role || "—", "dim"));
    tr.appendChild(td(fmt(rowData.projected_points_window, 2), "num"));
    tr.appendChild(td(rowData.performance_flag || "—"));
    gmBody.appendChild(tr);
  });
  if (gmRows.length === 0) {
    const tr = document.createElement("tr");
    const c = document.createElement("td");
    c.colSpan = 4;
    c.textContent = "No GM projection data found for selected horizon";
    c.className = "dim";
    c.style.padding = "16px 10px";
    tr.appendChild(c);
    gmBody.appendChild(tr);
  }
}

/* ── Standings view ─────────────────────────────────────────────────────── */
function renderStandings() {
  const ts    = state.data.teamScores || {};
  const claps = state.data.teamClaps  || {};

  const teams = Object.entries(ts).map(([team, data]) => {
    const rec = data.season_record || [0, 0];
    const w   = rec[0], l = rec[1];
    const pct = (w + l) > 0 ? w / (w + l) : 0;
    const c   = claps[team] || {};
    return { team, w, l, pct, c };
  });

  teams.sort((a, b) => b.pct - a.pct);

  const body = clearEl("standings-body");

  teams.forEach(({ team, w, l, pct, c }, idx) => {
    const rank = idx + 1;
    const tr   = document.createElement("tr");

    const rankTd = document.createElement("td");
    const rb = el("span", { class: `rank-badge rank-${rank <= 3 ? rank : ""}`, text: String(rank) });
    rankTd.appendChild(rb);
    tr.appendChild(rankTd);

    tr.appendChild(td(team, "team-name"));
    tr.appendChild(td(String(w), "num"));
    tr.appendChild(td(String(l), "num"));

    const pctTd = document.createElement("td");
    pctTd.className = "num win-pct";
    pctTd.style.color = pct >= 0.55 ? "var(--win)" : pct < 0.45 ? "var(--loss)" : "var(--text)";
    pctTd.textContent = (pct * 100).toFixed(1) + "%";
    tr.appendChild(pctTd);

    tr.appendChild(td(fmt(c.R_mean, 1), "num"));
    tr.appendChild(td(fmt(c.HR_mean, 1), "num"));
    tr.appendChild(td(fmt(c.OPS_mean, 3), "num"));
    tr.appendChild(td(fmt(c.K_mean, 1), "num"));
    tr.appendChild(td(fmt(c.ERA_mean, 2), "num"));
    tr.appendChild(td(fmt(c.aWHIP_mean, 3), "num"));
    tr.appendChild(td(fmt(c.VIJAY_mean, 2), "num"));
    tr.appendChild(td(fmt(c.aSB_mean, 1), "num"));

    body.appendChild(tr);
  });
}

/* ── Matchups view ──────────────────────────────────────────────────────── */
// Categories where LOWER is better (prob reflects likelihood of lower value = win)
const LOWER_IS_BETTER = new Set(["ERA", "aWHIP", "HRA"]);

// Display-friendly names
const CAT_LABELS = {
  R:     "R",
  HR:    "HR",
  OPS:   "OPS",
  OBP:   "OBP",
  aRBI:  "aRBI",
  aSB:   "aSB",
  K:     "K",
  ERA:   "ERA",
  aWHIP: "WHIP",
  NQW:   "NQW",
  VIJAY: "VIJAY",
  HRA:   "HRA",
};

function renderMatchups() {
  const pairs   = state.data.weekMatchups || [];
  const xm      = state.data.xmatchups   || {};
  const ts      = state.data.teamScores  || {};
  const grid    = clearEl("matchups-grid");
  const cats    = Object.keys(CAT_LABELS);

  pairs.forEach(([away, home]) => {
    const awayTeam = away.team;
    const homeTeam = home.team;

    // Win probabilities for away team against home team
    const probs = xm[awayTeam]?.[homeTeam] || {};

    // Count projected category wins for each side
    let awayWins = 0, homeWins = 0;
    cats.forEach(cat => {
      const p = Number(probs[cat] ?? 0.5);
      if (p > 0.5) awayWins++;
      else if (p < 0.5) homeWins++;
    });

    const awayRec = ts[awayTeam]?.season_record || [0, 0];
    const homeRec = ts[homeTeam]?.season_record || [0, 0];

    // Build card
    const card = el("div", { class: "matchup-card" });

    // Header
    const header = el("div", { class: "matchup-header" });

    const awayDiv = el("div", { class: "matchup-team away" });
    awayDiv.appendChild(el("div", { class: "matchup-team-name", text: awayTeam }));
    awayDiv.appendChild(el("div", { class: "matchup-team-record", text: `${awayRec[0]}-${awayRec[1]} · Away` }));

    const vsDiv = el("div", { class: "matchup-vs", text: "@" });

    const homeDiv = el("div", { class: "matchup-team" });
    homeDiv.appendChild(el("div", { class: "matchup-team-name", text: homeTeam }));
    homeDiv.appendChild(el("div", { class: "matchup-team-record", text: `${homeRec[0]}-${homeRec[1]} · Home` }));

    header.appendChild(awayDiv);
    header.appendChild(vsDiv);
    header.appendChild(homeDiv);
    card.appendChild(header);

    // Projected cat wins bar
    const winsBar = el("div", { class: "matchup-win-counts" });
    const awayWinsEl = el("div", { class: "matchup-proj-wins left", text: String(awayWins) });
    const label = el("div", { class: "matchup-proj-label", text: "proj cat wins" });
    const homeWinsEl = el("div", { class: "matchup-proj-wins right", text: String(homeWins) });
    winsBar.appendChild(awayWinsEl);
    winsBar.appendChild(label);
    winsBar.appendChild(homeWinsEl);
    card.appendChild(winsBar);

    // Category rows
    const catRows = el("div", { class: "cat-rows" });

    cats.forEach(cat => {
      // prob is away team's probability of winning this category
      const rawProb = Number(probs[cat] ?? 0.5);
      const awayFav = rawProb > 0.5;
      const awayPct = Math.round(rawProb * 100);
      const homePct = 100 - awayPct;

      const row = el("div", { class: "cat-row" });

      // Away prob label
      const awayLabel = el("div", {
        class: `cat-prob-left${awayFav ? " fav" : ""}`,
        text:  `${awayPct}%`
      });

      // Bar
      const barWrap = el("div", { class: "cat-bar-wrap" });
      const barLeft = el("div", { class: `cat-bar-left${awayFav ? " fav" : ""}` });
      // Width = awayPct% of 50% of the bar (center-out)
      barLeft.style.width = `${Math.max(2, awayPct)}%`;
      const catLabel = el("div", { class: "cat-name", text: CAT_LABELS[cat] || cat });
      const barRight = el("div", { class: "cat-bar-right" });

      barWrap.appendChild(barLeft);
      barWrap.appendChild(catLabel);
      barWrap.appendChild(barRight);

      // Home prob label
      const homeLabel = el("div", {
        class: `cat-prob-right${!awayFav ? " fav" : ""}`,
        text:  `${homePct}%`
      });

      row.appendChild(awayLabel);
      row.appendChild(barWrap);
      row.appendChild(homeLabel);
      catRows.appendChild(row);
    });

    card.appendChild(catRows);
    grid.appendChild(card);
  });

  if (pairs.length === 0) {
    grid.innerHTML = `<div class="loading">No matchup data available</div>`;
  }
}

/* ── Trends view ────────────────────────────────────────────────────────── */
function buildTrendsSelectors() {
  const ws        = state.data.weeklyScores || {};
  const teamSel   = document.getElementById("trends-team");
  const teams     = [...new Set(Object.values(ws).flatMap(w => Object.keys(w)))].sort();

  teams.forEach(t => {
    const opt = document.createElement("option");
    opt.value = t;
    opt.textContent = t;
    teamSel.appendChild(opt);
  });

  teamSel.addEventListener("change", renderTrends);
  document.getElementById("trends-cat").addEventListener("change", renderTrends);
}

function renderTrends() {
  const ws      = state.data.weeklyScores || {};
  const clap    = state.data.leagueClap  || {};
  const team    = document.getElementById("trends-team")?.value;
  const cat     = document.getElementById("trends-cat")?.value || "R";
  if (!team) return;

  const weeks   = Object.keys(ws).sort((a, b) => +a.replace("week","") - +b.replace("week",""));
  const labels  = weeks.map(w => `Wk ${w.replace("week","")}`);
  const values  = weeks.map(w => {
    const v = ws[w]?.[team]?.[cat];
    return v !== undefined ? Number(v) : null;
  });

  // League average line
  const leagueAvg = clap[cat]?.[0];
  const avgLine   = leagueAvg !== undefined ? weeks.map(() => Number(leagueAvg)) : null;

  // Destroy old chart
  if (state.chartInstance) {
    state.chartInstance.destroy();
    state.chartInstance = null;
  }

  const ctx = document.getElementById("trends-chart").getContext("2d");

  const teamColor   = "#38bdf8";
  const avgColor    = "#64748b";
  const lowerBetter = LOWER_IS_BETTER.has(cat);

  const datasets = [
    {
      label: `${team} — ${cat}`,
      data: values,
      borderColor: teamColor,
      backgroundColor: teamColor + "22",
      borderWidth: 2,
      pointBackgroundColor: teamColor,
      pointRadius: 4,
      pointHoverRadius: 6,
      fill: true,
      tension: 0.3,
      spanGaps: true,
    }
  ];

  if (avgLine) {
    datasets.push({
      label: "League avg",
      data: avgLine,
      borderColor: avgColor,
      borderWidth: 1,
      borderDash: [5, 4],
      pointRadius: 0,
      fill: false,
      tension: 0,
    });
  }

  state.chartInstance = new Chart(ctx, {
    type: "line",
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: {
          labels: {
            color: "#94a3b8",
            font: { size: 12 },
            boxWidth: 16,
          }
        },
        tooltip: {
          backgroundColor: "#0f172a",
          borderColor: "#1e2d45",
          borderWidth: 1,
          titleColor: "#e2e8f0",
          bodyColor: "#94a3b8",
          padding: 10,
          callbacks: {
            label: ctx => {
              const v = ctx.raw;
              if (v === null) return ` ${ctx.dataset.label}: —`;
              return ` ${ctx.dataset.label}: ${Number(v).toFixed(3)}`;
            }
          }
        },
      },
      scales: {
        x: {
          ticks: { color: "#64748b", font: { size: 11 } },
          grid:  { color: "#1e2d45" },
        },
        y: {
          reverse: lowerBetter,
          ticks: { color: "#64748b", font: { size: 11 } },
          grid:  { color: "#1e2d45" },
          title: {
            display: true,
            text: lowerBetter ? `${cat} (lower = better)` : cat,
            color: "#64748b",
            font: { size: 11 },
          }
        },
      },
    },
  });

  // Weekly breakdown table
  const thead = clearEl("trends-table-head");
  thead.appendChild(th("Week"));
  weeks.forEach(w => thead.appendChild(th(w.replace("week", "Wk "), "num")));

  const tbody = clearEl("trends-table-body");
  const tr    = document.createElement("tr");
  tr.appendChild(td(team, "team-name"));
  values.forEach(v => {
    const c = document.createElement("td");
    c.className = "num";
    c.textContent = v !== null ? Number(v).toFixed(cat === "OPS" || cat === "OBP" || cat === "aWHIP" ? 3 : cat === "ERA" ? 2 : 1) : "—";
    tr.appendChild(c);
  });
  tbody.appendChild(tr);

  // League avg row
  if (avgLine) {
    const avgRow = document.createElement("tr");
    avgRow.appendChild(td("League avg", "dim"));
    avgLine.forEach(v => {
      const c = document.createElement("td");
      c.className = "num dim";
      c.textContent = Number(v).toFixed(cat === "OPS" || cat === "OBP" || cat === "aWHIP" ? 3 : cat === "ERA" ? 2 : 1);
      avgRow.appendChild(c);
    });
    tbody.appendChild(avgRow);
  }
}

/* ── Init ───────────────────────────────────────────────────────────────── */
async function init() {
  try {
    await loadAll();

    renderHeader();
    renderDashboard();
    renderStandings();
    renderMatchups();
    buildTrendsSelectors();

    const weeklyBtn = document.getElementById("horizon-weekly");
    const dailyBtn = document.getElementById("horizon-daily");
    if (weeklyBtn && dailyBtn) {
      weeklyBtn.addEventListener("click", () => {
        state.horizon = "weekly";
        renderModelOpsPanel();
      });
      dailyBtn.addEventListener("click", () => {
        state.horizon = "daily";
        renderModelOpsPanel();
      });
    }

  } catch (err) {
    setText("asOf", `Load error: ${err.message}`);
    console.error(err);
  }
}

init();
