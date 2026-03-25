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
    scheduleStrength:       "/json/schedule_strength_latest.json",
    vijayValuation:         "/json/vijay_valuation_latest.json",
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
  if (tab === "free-agents") renderFreeAgents();
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
    setText("lastNightlyRun", status.target_date ? `Data for ${status.target_date}` : "Data date: —");
  } else {
    setText("lastNightlyRun", "Last run: unavailable");
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

  // Ingestion status badge + resource chips
  const ingestBadge = document.getElementById("dash-ingest-badge");
  if (ingestBadge) {
    const s = status.status || "unknown";
    ingestBadge.textContent = s;
    ingestBadge.className = `badge badge-${s === "ok" ? "ok" : s === "warn" ? "warn" : s === "error" ? "error" : "unknown"}`;
  }
  const ingestDetail = document.getElementById("dash-ingest-detail");
  if (ingestDetail) {
    ingestDetail.innerHTML = "";
    (status.resources || []).forEach(r => {
      const chipClass = r.status === "ok" ? "chip chip-green" : r.status === "skipped" ? "chip chip-gray" : "chip chip-red";
      const chip = el("span", { class: chipClass, text: r.name });
      chip.title = r.error_short ? `${r.status}: ${r.error_short}` : r.status;
      ingestDetail.appendChild(chip);
    });
    if (!status.resources?.length) {
      ingestDetail.textContent = "—";
      ingestDetail.style.fontSize = "12px";
      ingestDetail.style.color = "var(--text-dim)";
    }
  }

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
  const leaderW = teams[0]?.w ?? 0;

  const body = clearEl("standings-body");

  teams.forEach(({ team, w, l, pct, c }, idx) => {
    const rank = idx + 1;
    const tr   = document.createElement("tr");

    if (rank === 1) tr.classList.add("rank-first");

    const rankTd = document.createElement("td");
    const rb = el("span", { class: `rank-badge rank-${rank <= 3 ? rank : ""}`, text: String(rank) });
    rankTd.appendChild(rb);
    tr.appendChild(rankTd);

    tr.appendChild(td(team, "team-name"));
    tr.appendChild(td(String(w), "num"));
    tr.appendChild(td(String(l), "num"));

    const pctTd = document.createElement("td");
    pctTd.className = "num win-pct";
    const pctColor = pct >= 0.55 ? "var(--win)" : pct < 0.45 ? "var(--loss)" : "var(--text)";
    const barBg    = pct >= 0.55 ? "var(--win)" : pct < 0.45 ? "var(--loss)" : "var(--accent)";
    pctTd.style.color = pctColor;
    pctTd.innerHTML =
      `<span style="display:inline-flex;align-items:center;gap:6px;justify-content:flex-end">` +
      `<span class="pct-bar-wrap"><span class="pct-bar" style="width:${(pct*100).toFixed(0)}%;background:${barBg}"></span></span>` +
      `${(pct * 100).toFixed(1)}%</span>`;
    tr.appendChild(pctTd);

    const gb = idx === 0 ? "—" : ((leaderW - w) / 2).toFixed(1);
    tr.appendChild(td(gb, "num dim"));

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
    const awayLeading = awayWins > homeWins;
    const homeLeading = homeWins > awayWins;
    const winsBar = el("div", { class: "matchup-win-counts" });
    const awayWinsEl = el("div", { class: `matchup-proj-wins${awayLeading ? " leading" : ""}`, text: String(awayWins) });
    const label = el("div", { class: "matchup-proj-label", text: "proj cat wins" });
    const homeWinsEl = el("div", { class: `matchup-proj-wins right${homeLeading ? " leading" : ""}`, text: String(homeWins) });
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
      const barTrack = el("div", { class: "cat-bar-track" });
      const barLeft = el("div", { class: `cat-bar-left${awayFav ? " fav" : ""}` });
      barLeft.style.width = `${Math.max(2, awayPct)}%`;
      const barRight = el("div", { class: "cat-bar-right" });
      barTrack.appendChild(barLeft);
      barTrack.appendChild(barRight);
      const catLabel = el("div", { class: "cat-name", text: CAT_LABELS[cat] || cat });
      barWrap.appendChild(barTrack);
      barWrap.appendChild(catLabel);

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

  const teamColor   = "#C2390A";   /* accent red-orange */
  const avgColor    = "rgba(26,20,16,0.20)";
  const lowerBetter = LOWER_IS_BETTER.has(cat);

  const datasets = [
    {
      label: `${team} — ${cat}`,
      data: values,
      borderColor: teamColor,
      backgroundColor: teamColor + "28",
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
      borderWidth: 1.5,
      borderDash: [5, 4],
      pointRadius: 0,
      fill: false,
      tension: 0.3,
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
            color: "rgba(26,20,16,0.52)",
            font: { family: "'IBM Plex Mono', monospace", size: 11 },
            boxWidth: 14,
            padding: 16,
          }
        },
        tooltip: {
          backgroundColor: "#1A1410",
          borderColor: "#35291F",
          borderWidth: 1,
          titleColor: "#EDE6D8",
          bodyColor: "rgba(237,230,216,0.70)",
          padding: 12,
          cornerRadius: 6,
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
          ticks: { color: "rgba(26,20,16,0.40)", font: { family: "'IBM Plex Mono', monospace", size: 10 } },
          grid:  { color: "#D5CFC3" },
          border: { color: "#D5CFC3" },
        },
        y: {
          reverse: lowerBetter,
          ticks: { color: "rgba(26,20,16,0.40)", font: { family: "'IBM Plex Mono', monospace", size: 10 } },
          grid:  { color: "#D5CFC3" },
          border: { color: "#D5CFC3" },
          title: {
            display: true,
            text: lowerBetter ? `${cat} (lower = better)` : cat,
            color: "rgba(26,20,16,0.40)",
            font: { family: "'IBM Plex Mono', monospace", size: 10 },
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

/* ── Season Outlook ─────────────────────────────────────────────────────── */

function renderSeasonOutlook() {
  const d = state.data.scheduleStrength;
  if (!d || !d.teams) {
    const container = document.getElementById("outlook-divisions");
    if (container) container.innerHTML = '<div class="card"><div class="card-sub">Schedule strength data not yet available. Run the pipeline to generate schedule_strength_latest.json.</div></div>';
    return;
  }

  // Header summary cards
  setText("outlook-periods-remaining", d.remaining_periods ?? "—");
  setText("outlook-periods-total", `of ${d.total_periods ?? "—"} total`);
  setText("outlook-prob-source", (d.win_probability_source || "—").replace(/_/g, " "));
  setText("outlook-target-date", d.target_date ?? "—");
  const age = d.generated_at_utc ? ageHours(d.generated_at_utc) : null;
  setText("outlook-generated-at", age !== null ? `${age.toFixed(1)}h ago` : "—");

  // Division cards
  const divsContainer = document.getElementById("outlook-divisions");
  if (divsContainer) {
    divsContainer.innerHTML = "";
    const divisions = d.division_projections || {};
    const divNames = Object.keys(divisions).sort();

    const grid = document.createElement("div");
    grid.className = "grid grid-2";
    grid.style.marginBottom = "16px";

    divNames.forEach(divName => {
      const teams = divisions[divName] || [];
      const card = document.createElement("div");
      card.className = "card";

      const header = document.createElement("div");
      header.className = "card-header";
      header.innerHTML = `<div class="card-title">${divName}</div><div class="card-sub">Division projection</div>`;
      card.appendChild(header);

      const tableWrap = document.createElement("div");
      tableWrap.className = "table-wrap";
      const table = document.createElement("table");

      const thead = document.createElement("thead");
      thead.innerHTML = `<tr>
        <th>Rk</th><th>Team</th>
        <th class="num">Cur W-L</th>
        <th class="num">Proj W</th>
        <th class="num">Proj Win%</th>
        <th>SOS</th>
      </tr>`;
      table.appendChild(thead);

      const tbody = document.createElement("tbody");
      teams.forEach(t => {
        const rec = t.current_record || {};
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${t.projected_division_rank}</td>
          <td><strong>${t.long_abbr}</strong></td>
          <td class="num">${rec.w ?? 0}–${rec.l ?? 0}</td>
          <td class="num">${fmt(t.projected_wins, 1)}</td>
          <td class="num">${fmtPct(t.projected_win_pct)}</td>
          <td><span class="sos-badge sos-${(t.sos_label || "avg").toLowerCase()}">${t.sos_label || "—"}</span></td>
        `;
        tbody.appendChild(tr);
      });
      table.appendChild(tbody);
      tableWrap.appendChild(table);
      card.appendChild(tableWrap);
      grid.appendChild(card);
    });

    divsContainer.appendChild(grid);
  }

  // All-teams table sorted by projected win %
  const tbody = document.getElementById("outlook-all-teams-body");
  if (tbody) {
    tbody.innerHTML = "";
    const allTeams = Object.values(d.teams).sort(
      (a, b) => (b.projected_win_pct ?? 0) - (a.projected_win_pct ?? 0)
    );
    allTeams.forEach((t, idx) => {
      const rec = t.current_record || {};
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${idx + 1}</td>
        <td><strong>${t.long_abbr}</strong></td>
        <td style="font-size:11px;color:var(--text-dim)">${t.division || "—"}</td>
        <td class="num">${rec.w ?? 0}</td>
        <td class="num">${rec.l ?? 0}</td>
        <td class="num">${t.games_remaining ?? "—"}</td>
        <td class="num">${fmt(t.projected_wins, 1)}</td>
        <td class="num">${fmt(t.projected_losses, 1)}</td>
        <td class="num">${fmtPct(t.projected_win_pct)}</td>
        <td><span class="sos-badge sos-${(t.sos_label || "avg").toLowerCase()}">${t.sos_label || "—"}</span></td>
      `;
      tbody.appendChild(tr);
    });
  }

  // Team selector for drilldown
  const sel = document.getElementById("outlook-team-select");
  if (sel) {
    sel.innerHTML = "";
    const teams = Object.values(d.teams).sort((a, b) =>
      (a.long_abbr || "").localeCompare(b.long_abbr || "")
    );
    teams.forEach(t => {
      const opt = document.createElement("option");
      opt.value = t.team_id;
      opt.textContent = `${t.long_abbr} — ${t.display_name}`;
      sel.appendChild(opt);
    });
    sel.addEventListener("change", () => renderOutlookSchedule(d));
    renderOutlookSchedule(d);
  }
}

function renderOutlookSchedule(d) {
  const sel = document.getElementById("outlook-team-select");
  const tbody = document.getElementById("outlook-schedule-body");
  if (!sel || !tbody || !d || !d.teams) return;

  const teamId = sel.value;
  const team = d.teams[teamId];
  tbody.innerHTML = "";

  if (!team || !team.remaining_matchups || team.remaining_matchups.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = '<td colspan="5" style="color:var(--text-dim)">No remaining matchups on record — schedule may not be fully published yet.</td>';
    tbody.appendChild(tr);
    return;
  }

  team.remaining_matchups
    .slice()
    .sort((a, b) => (a.period_id ?? 0) - (b.period_id ?? 0))
    .forEach(m => {
      const prob = Number(m.win_probability);
      const probClass = prob >= 0.55 ? "pos" : prob <= 0.45 ? "neg" : "";
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${m.period_label || `Period ${m.period_id}`}</td>
        <td style="font-size:11px;color:var(--text-dim)">${m.start_date || ""}${m.end_date ? " – " + m.end_date : ""}</td>
        <td><strong>${m.opponent_abbr}</strong></td>
        <td>${m.is_home ? "Home" : "Away"}</td>
        <td class="num ${probClass}">${isNaN(prob) ? "—" : (prob * 100).toFixed(1) + "%"}</td>
      `;
      tbody.appendChild(tr);
    });
}

/* ── Relievers (VIJAY Valuation) ────────────────────────────────────────── */

const vijayState = { filter: "all" };

const RISK_TIER_CLASS = {
  "Locked In":  "pos",
  "Solid":      "",
  "Volatile":   "warn",
  "High Risk":  "neg",
};

const RISK_TIER_CHIP = {
  "Locked In": "chip chip-ember",
  "Solid":     "chip chip-gray",
  "Volatile":  "chip chip-warn",
  "High Risk": "chip chip-red",
};

function renderRelievers() {
  const d = state.data.vijayValuation;
  if (!d || !d.relievers) {
    const tbody = document.getElementById("vijay-table-body");
    if (tbody) tbody.innerHTML = '<tr><td colspan="13" style="color:var(--text-dim)">VIJAY valuation data not yet available. Run: python py/vijay_valuation.py</td></tr>';
    return;
  }

  setText("vijay-total", d.total_relievers ?? "—");
  setText("vijay-fa-count", d.free_agent_count ?? "—");
  setText("vijay-rostered-count", d.rostered_count ?? "—");
  const age = d.generated_at_utc ? ageHours(d.generated_at_utc) : null;
  setText("vijay-generated-at", age !== null ? `${age.toFixed(1)}h ago` : "—");

  // Wire filter buttons
  const filters = { all: "vijay-filter-all", fa: "vijay-filter-fa", close: "vijay-filter-close", setup: "vijay-filter-setup" };
  Object.entries(filters).forEach(([key, btnId]) => {
    const btn = document.getElementById(btnId);
    if (!btn) return;
    btn.classList.toggle("active", vijayState.filter === key);
    btn.onclick = () => {
      vijayState.filter = key;
      Object.values(filters).forEach(id => {
        const b = document.getElementById(id);
        if (b) b.classList.remove("active");
      });
      btn.classList.add("active");
      renderVijayTable(d);
    };
  });

  renderVijayTable(d);
}

function renderVijayTable(d) {
  const tbody = document.getElementById("vijay-table-body");
  if (!tbody) return;
  tbody.innerHTML = "";

  let relievers = d.relievers || [];

  // Apply filter
  if (vijayState.filter === "fa") {
    relievers = relievers.filter(r => r.roster_status === "Free Agent");
  } else if (vijayState.filter === "close") {
    relievers = relievers.filter(r => r.role_type === "Closer" || r.role_type === "Co-Closer");
  } else if (vijayState.filter === "setup") {
    relievers = relievers.filter(r => r.role_type === "Elite Setup" || r.role_type === "Setup");
  }

  if (relievers.length === 0) {
    tbody.innerHTML = '<tr><td colspan="13" style="color:var(--text-dim)">No relievers match this filter.</td></tr>';
    return;
  }

  relievers.forEach(r => {
    const riskChip    = RISK_TIER_CHIP[r.risk_tier]  || "chip chip-gray";
    const statusLabel = r.roster_status === "Free Agent"
      ? '<span class="chip chip-gray" style="font-size:10px">FA</span>'
      : `<span style="font-size:11px">${r.rostered_by_team_name || "Rostered"}</span>`;
    const vijayDelta  = r.risk_adj_vijay - r.projected_vijay;
    const deltaClass  = vijayDelta < -1 ? "neg" : vijayDelta > 1 ? "pos" : "";
    const bsClass     = r.bs_rate_pct >= 22 ? "neg" : r.bs_rate_pct >= 15 ? "warn" : r.bs_rate_pct < 10 ? "pos" : "";

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td class="num dim">${r.rank}</td>
      <td><strong>${r.name}</strong></td>
      <td style="font-size:11px;color:var(--text-dim)">${r.mlb_team}</td>
      <td style="font-size:11px">${r.role_type}</td>
      <td><span class="${riskChip}">${r.risk_tier}</span></td>
      <td class="num">${fmt(r.proj_sv, 1)}</td>
      <td class="num">${fmt(r.proj_hld, 1)}</td>
      <td class="num">${fmt(r.proj_bs, 1)}</td>
      <td class="num ${bsClass}">${fmt(r.bs_rate_pct, 1)}%</td>
      <td class="num">${fmt(r.proj_ip, 1)}</td>
      <td class="num">${fmt(r.projected_vijay, 2)}</td>
      <td class="num ${deltaClass}">${fmt(r.risk_adj_vijay, 2)}</td>
      <td>${statusLabel}</td>
    `;
    tbody.appendChild(tr);
  });
}

/* ── Free Agents view ───────────────────────────────────────────────────── */

const faState = { filter: "all", search: "" };

function flagClass(flag) {
  if (!flag || flag === "insufficient_data") return "dim";
  if (flag === "ahead" || flag === "overperforming") return "pos";
  if (flag === "behind" || flag === "underperforming") return "neg";
  if (flag === "on_pace") return "";
  return "dim";
}

function flagLabel(flag) {
  if (!flag) return "—";
  return flag.replace(/_/g, " ");
}

const ROLE_LABELS = { batters: "Bat", sp: "SP", rp: "RP" };

function renderFreeAgents() {
  const d      = state.data.freeAgents   || {};
  const digest = state.data.weeklyDigest || {};

  // Summary cards
  const summary = d.summary || {};
  setText("fa-candidate-count", summary.candidate_count      ?? "—");
  setText("fa-rostered-count",  summary.rostered_player_count ?? "—");
  setText("fa-universe-count",  summary.universe_player_count ?? "—");

  const age = d.generated_at_utc ? ageHours(d.generated_at_utc) : null;
  const ageStr = age !== null ? `${age.toFixed(1)}h ago` : "—";
  setText("fa-generated-at", d.target_date ? `${ageStr} · ${d.target_date}` : ageStr);

  const scoring = d.scoring || {};
  const wLabel = (scoring.weekly_weight != null && scoring.daily_weight != null)
    ? ` · ${(scoring.weekly_weight * 100).toFixed(0)}% weekly + ${(scoring.daily_weight * 100).toFixed(0)}% daily`
    : "";
  const win = digest.window || {};
  const winLabel = (win.start_date && win.end_date)
    ? ` · Window: ${win.start_date} – ${win.end_date}`
    : "";
  setText("fa-source-label",
    d.candidates
      ? `Source: free_agent_candidates_latest.json${wLabel}${winLabel}`
      : "Source: unavailable");

  // Filter buttons
  const filterMap = { all: "fa-filter-all", batters: "fa-filter-bat", sp: "fa-filter-sp", rp: "fa-filter-rp" };
  Object.entries(filterMap).forEach(([key, btnId]) => {
    const btn = document.getElementById(btnId);
    if (!btn) return;
    btn.classList.toggle("active", faState.filter === key);
    btn.onclick = () => {
      faState.filter = key;
      Object.values(filterMap).forEach(id => {
        const b = document.getElementById(id);
        if (b) b.classList.remove("active");
      });
      btn.classList.add("active");
      renderFaCandidatesTable(d);
    };
  });

  // Search input
  const searchInput = document.getElementById("fa-search");
  if (searchInput) {
    searchInput.value = faState.search;
    searchInput.oninput = () => {
      faState.search = searchInput.value.trim().toLowerCase();
      renderFaCandidatesTable(d);
    };
  }

  renderFaCandidatesTable(d);
  renderFaSwaps(digest);
}

function renderFaCandidatesTable(d) {
  const body = clearEl("fa-candidates-body");
  let candidates = (d.candidates || []).slice();
  const totalRaw = (d.candidates || []).length;

  if (faState.filter !== "all") {
    candidates = candidates.filter(c => c.player_role === faState.filter);
  }
  if (faState.search) {
    candidates = candidates.filter(c =>
      (c.player_name || "").toLowerCase().includes(faState.search)
    );
  }

  if (!candidates.length) {
    const tr = document.createElement("tr");
    const c  = document.createElement("td");
    c.colSpan = 8; c.className = "dim"; c.style.padding = "16px 10px";
    const emptyRun = totalRaw === 0 && faState.filter === "all" && !faState.search;
    c.textContent = emptyRun
      ? "No free agent candidates in this run."
      : "No candidates match the current filter.";
    tr.appendChild(c); body.appendChild(tr);
    return;
  }

  candidates.slice(0, 100).forEach((cand, idx) => {
    const tr = document.createElement("tr");
    tr.appendChild(td(String(idx + 1), "num dim"));
    tr.appendChild(td(cand.player_name || "—", "team-name"));
    tr.appendChild(td(ROLE_LABELS[cand.player_role] || cand.player_role || "—", "dim"));
    tr.appendChild(td(fmt(cand.projected_points_weekly, 3), "num"));
    tr.appendChild(td(fmt(cand.projected_points_daily,  3), "num"));
    tr.appendChild(td(fmt(cand.composite_score,         3), "num"));

    const delta = cand.performance_delta;
    const deltaTd = document.createElement("td");
    deltaTd.className = delta == null ? "num dim" : delta > 0 ? "num pos" : delta < 0 ? "num neg" : "num";
    deltaTd.textContent = delta == null ? "—" : (delta > 0 ? "+" : "") + fmt(delta, 2);
    tr.appendChild(deltaTd);

    const flagTd = document.createElement("td");
    const cls = flagClass(cand.performance_flag);
    flagTd.innerHTML = `<span class="${cls}" style="font-size:11px">${flagLabel(cand.performance_flag)}</span>`;
    tr.appendChild(flagTd);

    body.appendChild(tr);
  });
}

function renderFaSwaps(digest) {
  const body  = clearEl("fa-swaps-body");
  const raw = digest.recommended_swaps;
  const swaps = Array.isArray(raw)
    ? raw.filter(s => s && typeof s === "object")
    : [];

  if (!swaps.length) {
    const tr = document.createElement("tr");
    const c  = document.createElement("td");
    c.colSpan = 6; c.className = "dim"; c.style.padding = "16px 10px";
    c.textContent = "No swap recommendations available.";
    tr.appendChild(c); body.appendChild(tr);
    return;
  }

  // Best swap per team by composite score
  const byTeam = {};
  swaps.forEach(s => {
    const id = s.team_id;
    if (!byTeam[id] || s.net_composite_score > byTeam[id].net_composite_score) {
      byTeam[id] = s;
    }
  });

  Object.values(byTeam)
    .sort((a, b) => b.net_composite_score - a.net_composite_score)
    .forEach(s => {
      const tr = document.createElement("tr");
      tr.appendChild(td(s.team_name || "—", "team-name"));
      const addRole  = ROLE_LABELS[s.add_player?.player_role]  || "?";
      const dropRole = ROLE_LABELS[s.drop_player?.player_role] || "?";
      tr.appendChild(td(`${s.add_player?.player_name  || "—"} (${addRole})`,  "pos"));
      tr.appendChild(td(`${s.drop_player?.player_name || "—"} (${dropRole})`, "neg"));
      tr.appendChild(td(fmt(s.net_points_daily,    3), "num"));
      tr.appendChild(td(fmt(s.net_points_weekly,   3), "num"));
      const scoreTd = document.createElement("td");
      scoreTd.className = s.net_composite_score >= 0 ? "num pos" : "num neg";
      scoreTd.textContent = (s.net_composite_score > 0 ? "+" : "") + fmt(s.net_composite_score, 3);
      tr.appendChild(scoreTd);
      body.appendChild(tr);
    });
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
    renderSeasonOutlook();
    renderFreeAgents();
    renderRelievers();

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
