const state = {
  data: {},
  activeTab: "scoreboard",
  rotoSort: {
    batting: { key: "battingPoints", dir: "desc" },
    pitching: { key: "pitchingPoints", dir: "desc" },
    summary: { key: "totalRotoPoints", dir: "desc" },
  },
};

async function loadJson(path) {
  const res = await fetch(path, { cache: "no-store" });
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return res.json();
}

async function loadText(path) {
  const res = await fetch(path, { cache: "no-store" });
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return res.text();
}

function fmt(value, d = 2) {
  const n = Number(value);
  return Number.isNaN(n) ? "—" : n.toFixed(d);
}

function pct(w, l) {
  const denom = Number(w) + Number(l);
  return denom > 0 ? Number(w) / denom : 0;
}

function setText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

function clear(id) {
  const el = document.getElementById(id);
  if (el) el.innerHTML = "";
  return el;
}

function td(text, cls = "") {
  const el = document.createElement("td");
  el.textContent = text ?? "—";
  if (cls) el.className = cls;
  return el;
}

function switchTab(tab) {
  state.activeTab = tab;
  document.querySelectorAll(".tab").forEach((b) => b.classList.toggle("active", b.dataset.tab === tab));
  document.querySelectorAll(".view").forEach((v) => v.classList.toggle("active", v.id === `view-${tab}`));
}

document.querySelectorAll(".tab").forEach((b) => {
  b.addEventListener("click", () => switchTab(b.dataset.tab));
});

function setupRotoSorting() {
  document.querySelectorAll("th[data-roto-table][data-sort-key]").forEach((th) => {
    if (th.dataset.boundSort === "1") return;
    th.dataset.boundSort = "1";
    th.dataset.baseLabel = th.textContent;
    th.style.cursor = "pointer";
    th.addEventListener("click", () => {
      const table = th.dataset.rotoTable;
      const key = th.dataset.sortKey;
      const current = state.rotoSort[table] || {};
      let dir = "desc";
      if (current.key === key) {
        dir = current.dir === "desc" ? "asc" : "desc";
      } else {
        dir = initialSortDirection(key);
      }
      state.rotoSort[table] = { key, dir };
      renderRoto();
    });
  });
  updateRotoSortIndicators();
}

function initialSortDirection(key) {
  if (key === "ERA" || key === "aWHIP" || key === "rank") return "asc";
  if (key === "team") return "asc";
  return "desc";
}

function sortRows(rows, key, dir) {
  const multiplier = dir === "asc" ? 1 : -1;
  return [...rows].sort((a, b) => {
    if (key === "team") return multiplier * String(a.team).localeCompare(String(b.team));
    const av = Number(a[key] ?? 0);
    const bv = Number(b[key] ?? 0);
    if (Math.abs(av - bv) < 1e-12) return String(a.team).localeCompare(String(b.team));
    return multiplier * (av - bv);
  });
}

function updateRotoSortIndicators() {
  document.querySelectorAll("th[data-roto-table][data-sort-key]").forEach((th) => {
    const table = th.dataset.rotoTable;
    const key = th.dataset.sortKey;
    const sortState = state.rotoSort[table] || {};
    const baseLabel = th.dataset.baseLabel || th.textContent.replace(/\s[▲▼]$/, "");
    th.dataset.baseLabel = baseLabel;
    if (sortState.key === key) {
      th.textContent = `${baseLabel} ${sortState.dir === "asc" ? "▲" : "▼"}`;
    } else {
      th.textContent = baseLabel;
    }
  });
}

function latestWeekKey(weeklyScores) {
  const weeks = Object.keys(weeklyScores || {});
  weeks.sort((a, b) => Number(a.replace("week", "")) - Number(b.replace("week", "")));
  return weeks[weeks.length - 1] || "week1";
}

function mapCategories(categories) {
  const map = {};
  (categories || []).forEach((entry) => {
    map[String(entry.name || "")] = entry.value;
  });
  return map;
}

function normalizeCategoryName(name) {
  const key = String(name || "").trim();
  if (key === "ASB") return "aSB";
  return key;
}

function buildCategoryIndex(team) {
  const index = {};
  (team?.categories || []).forEach((entry) => {
    const name = normalizeCategoryName(entry?.name);
    const value = Number(entry?.value);
    if (!name || Number.isNaN(value)) return;
    index[name] = {
      value,
      isBad: String(entry?.is_bad) === "true",
    };
  });
  return index;
}

function computeMatchupScores(teamA, teamB) {
  const a = buildCategoryIndex(teamA);
  const b = buildCategoryIndex(teamB);
  const categories = ["HR", "R", "OBP", "OPS", "aRBI", "aSB", "K", "HRA", "aWHIP", "VIJAY", "ERA", "MGS"];
  let aScore = 0;
  let bScore = 0;
  categories.forEach((name) => {
    const av = (a[name] && Number.isFinite(a[name].value)) ? a[name].value : 0;
    const bv = (b[name] && Number.isFinite(b[name].value)) ? b[name].value : 0;
    const lowerIsBetter = ["ERA", "aWHIP", "HRA"].includes(name);
    if (Math.abs(av - bv) < 1e-12) {
      // League tiebreak rule: tied category goes to home team.
      bScore += 1;
      return;
    }
    const aWins = lowerIsBetter ? av < bv : av > bv;
    if (aWins) aScore += 1;
    else bScore += 1;
  });
  return { a: aScore, b: bScore };
}

async function loadAll() {
  const status = await loadJson("/json/ingestion_status_latest.json");
  state.data.status = status;
  state.data.rules = await loadText("/LDB_League_Rules_Summary.md").catch(() => "Rules file not available.");

  // Current nightly raw snapshot
  const targetDate = status.target_date;
  if (targetDate) {
    const livePath = `/data/raw/${targetDate}/live_scoring_${targetDate}.json`;
    const schedulePath = `/data/raw/${targetDate}/schedule_${targetDate}.json`;
    const teamWeeklyTotalsPath = "/json/team_weekly_totals_latest.json";
    const teamWeeklyTotalsStatePath = "/json/team_weekly_totals_state.json";
    try {
      state.data.liveScoring = await loadJson(livePath);
      state.data.scoreboardSource = `current nightly snapshot (${targetDate})`;
    } catch (_) {
      state.data.scoreboardSource = "legacy fallback";
    }
    try {
      state.data.schedule = await loadJson(schedulePath);
    } catch (_) {
      state.data.schedule = null;
    }
    try {
      state.data.teamWeeklyTotals = await loadJson(teamWeeklyTotalsPath);
    } catch (_) {
      state.data.teamWeeklyTotals = null;
    }
    try {
      state.data.teamWeeklyTotalsState = await loadJson(teamWeeklyTotalsStatePath);
    } catch (_) {
      state.data.teamWeeklyTotalsState = null;
    }
  }

  // Always load legacy as fallback for sections not yet in current contract.
  const legacyEntries = await Promise.allSettled(
    [
      ["weeklyScores", "/json/weeklyScores.json"],
      ["weekMatchups", "/json/week_matchups.json"],
      ["teamScores", "/json/team_scores.json"],
    ].map(async ([k, p]) => [k, await loadJson(p)])
  );
  legacyEntries.forEach((r) => {
    if (r.status === "fulfilled") {
      const [k, v] = r.value;
      state.data[k] = v;
    }
  });
}

function renderHeader() {
  const status = state.data.status || {};
  const badge = document.getElementById("statusBadge");
  const s = status.status || "unknown";
  badge.textContent = s;
  badge.className = `badge badge-${s === "ok" ? "ok" : s === "warn" ? "warn" : s === "error" ? "error" : "unknown"}`;
  const ts = status.last_success_utc || status.generated_at_utc;
  setText("asOf", ts ? `Updated ${new Date(ts).toLocaleString()}` : "Updated —");
}

function renderScoreboardWeeklyTotals() {
  const payload = state.data.teamWeeklyTotals || {};
  const teams = payload.teams || [];
  const matchups = payload.matchups || [];
  if (!teams.length || !matchups.length) return false;
  const teamById = {};
  teams.forEach((team) => {
    teamById[String(team.team_id)] = team;
  });

  const period = payload.period || {};
  const label = period.label ? `${period.label} (${period.start || "?"} - ${period.end || "?"})` : `Target date ${payload.target_date || "—"}`;
  setText("scoreboard-week-label", label);
  setText("scoreboard-source-label", "Source: team_weekly_totals_latest.json");

  const body = clear("scoreboard-body");
  matchups.forEach((matchup) => {
    const away = teamById[String(matchup.away_team_id)] || {};
    const home = teamById[String(matchup.home_team_id)] || {};
    const rows = [
      {
        matchupLabel: `${matchup.away_team_abbr || away.team_abbr || "Away"} @ ${matchup.home_team_abbr || home.team_abbr || "Home"}`,
        team: away,
        score: matchup?.score?.away,
      },
      {
        matchupLabel: "",
        team: home,
        score: matchup?.score?.home,
      },
    ];
    rows.forEach((row) => {
      const c = row.team.category_totals || {};
      const tr = document.createElement("tr");
      tr.appendChild(td(row.matchupLabel));
      tr.appendChild(td(row.team.team_abbr || row.team.team_name || "—", "team-name"));
      tr.appendChild(td(fmt(c.HR, 0), "num"));
      tr.appendChild(td(fmt(c.R, 0), "num"));
      tr.appendChild(td(fmt(c.OBP, 3), "num"));
      tr.appendChild(td(fmt(c.OPS, 3), "num"));
      tr.appendChild(td(fmt(c.aRBI, 1), "num"));
      tr.appendChild(td(fmt(c.aSB, 1), "num"));
      tr.appendChild(td(fmt(c.K, 0), "num"));
      tr.appendChild(td(fmt(c.HRA, 1), "num"));
      tr.appendChild(td(fmt(c.aWHIP, 3), "num"));
      tr.appendChild(td(fmt(c.VIJAY, 3), "num"));
      tr.appendChild(td(fmt(c.ERA, 3), "num"));
      tr.appendChild(td(fmt(c.MGS, 2), "num"));
      tr.appendChild(td(fmt(row.score, 1), "num"));
      body.appendChild(tr);
    });
  });
  return true;
}

function renderScoreboardCurrent() {
  const payload = state.data.liveScoring || {};
  const teams = payload?.body?.live_scoring?.teams || [];
  if (!teams.length) return false;
  setText("scoreboard-week-label", `Target date ${state.data.status?.target_date || "—"}`);
  const hasSchedule = Boolean(state.data.schedule?.body?.schedule?.periods?.length);
  setText(
    "scoreboard-source-label",
    hasSchedule
      ? "Source: current nightly live_scoring + schedule snapshots"
      : "Source: current nightly live_scoring snapshot"
  );
  const teamById = {};
  teams.forEach((t) => { teamById[String(t.id)] = t; });
  const body = clear("scoreboard-body");
  const seen = new Set();

  const scheduledMatchups = extractScheduledMatchups(state.data.schedule, state.data.status?.target_date);
  const rowsByMatchup = scheduledMatchups.length
    ? scheduledMatchups.map((matchup) => {
      const awayId = String(matchup?.away_team?.id ?? "");
      const homeId = String(matchup?.home_team?.id ?? "");
      return [
        {
          team: teamById[awayId] || matchup.away_team || {},
          matchupLabel: `${matchup?.away_team?.long_abbr || matchup?.away_team?.name || "Away"} @ ${matchup?.home_team?.long_abbr || matchup?.home_team?.name || "Home"}`,
        },
        {
          team: teamById[homeId] || matchup.home_team || {},
          matchupLabel: "",
        },
      ];
    })
    : [...teams]
      .sort((a, b) => String(a.name).localeCompare(String(b.name)))
      .map((team) => {
        const matchup = (team.matchups || [])[0];
        if (matchup && matchup.opponent_id != null) {
          const opp = teamById[String(matchup.opponent_id)];
          if (opp) {
            return [
              { team, matchupLabel: `${team.long_abbr || team.name} @ ${opp.long_abbr || opp.name}` },
              { team: opp, matchupLabel: "" },
            ];
          }
        }
        return [{ team, matchupLabel: "—" }];
      });

  rowsByMatchup.forEach((pair) => {
    const matchupScores = pair.length >= 2 ? computeMatchupScores(pair[0].team, pair[1].team) : null;
    pair.forEach((entry, idx) => {
      const rowTeam = entry.team || {};
      const teamId = String(rowTeam.id || "");
      if (teamId && seen.has(teamId)) return;
      if (teamId) seen.add(teamId);
      const c = mapCategories(rowTeam.categories || []);
      const tr = document.createElement("tr");
      tr.appendChild(td(idx === 0 ? entry.matchupLabel : ""));
      tr.appendChild(td(rowTeam.long_abbr || rowTeam.name || "—", "team-name"));
      tr.appendChild(td(fmt(c.HR, 0), "num"));
      tr.appendChild(td(fmt(c.R, 0), "num"));
      tr.appendChild(td(fmt(c.OBP, 3), "num"));
      tr.appendChild(td(fmt(c.OPS, 3), "num"));
      tr.appendChild(td(fmt(c.aRBI, 1), "num"));
      tr.appendChild(td(fmt(c.ASB ?? c.aSB, 1), "num"));
      tr.appendChild(td(fmt(c.K, 0), "num"));
      tr.appendChild(td(fmt(c.HRA, 1), "num"));
      tr.appendChild(td(fmt(c.aWHIP, 3), "num"));
      tr.appendChild(td(fmt(c.VIJAY, 3), "num"));
      tr.appendChild(td(fmt(c.ERA, 3), "num"));
      tr.appendChild(td(fmt(c.MGS, 2), "num"));
      const derivedScore = matchupScores ? (idx === 0 ? matchupScores.a : matchupScores.b) : null;
      const scoreValue = derivedScore == null ? rowTeam.pts : derivedScore;
      tr.appendChild(td(fmt(scoreValue, 1), "num"));
      body.appendChild(tr);
    });
  });
  return true;
}

function extractScheduledMatchups(schedulePayload, targetDate) {
  const periods = schedulePayload?.body?.schedule?.periods || [];
  if (!periods.length) return [];
  const target = parseUsDate(targetDate);
  const scored = periods
    .map((period) => {
      const start = parseUsDate(period.start);
      const end = parseUsDate(period.end);
      let score = 1000;
      if (target && start && end && target >= start && target <= end) score = 0;
      else if (target && start) score = Math.abs((start - target) / 86400000) + 10;
      return { period, score };
    })
    .sort((a, b) => a.score - b.score);
  return scored[0]?.period?.matchups || [];
}

function parseUsDate(value) {
  if (!value) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return new Date(`${value}T00:00:00`);
  }
  const match = String(value).match(/^(\d{1,2})\/(\d{1,2})\/(\d{2})$/);
  if (!match) return null;
  const month = Number(match[1]);
  const day = Number(match[2]);
  const year = 2000 + Number(match[3]);
  return new Date(year, month - 1, day);
}

function renderScoreboardLegacy() {
  const ws = state.data.weeklyScores || {};
  const pairs = state.data.weekMatchups || [];
  const weekKey = latestWeekKey(ws);
  setText("scoreboard-week-label", weekKey.replace("week", "Week "));
  setText("scoreboard-source-label", "Source: legacy weeklyScores/week_matchups snapshot");
  const weekData = ws[weekKey] || {};
  const body = clear("scoreboard-body");
  pairs.forEach(([away, home]) => {
    const rows = [
      { team: away.team, matchup: `${away.team} @ ${home.team}` },
      { team: home.team, matchup: "" },
    ];
    rows.forEach((r) => {
      const s = weekData[r.team] || {};
      const tr = document.createElement("tr");
      tr.appendChild(td(r.matchup));
      tr.appendChild(td(r.team, "team-name"));
      tr.appendChild(td(fmt(s.HR, 0), "num"));
      tr.appendChild(td(fmt(s.R, 0), "num"));
      tr.appendChild(td(fmt(s.OBP, 3), "num"));
      tr.appendChild(td(fmt(s.OPS, 3), "num"));
      tr.appendChild(td(fmt(s.aRBI, 1), "num"));
      tr.appendChild(td(fmt(s.aSB, 1), "num"));
      tr.appendChild(td(fmt(s.K, 0), "num"));
      tr.appendChild(td(fmt(s.HRA, 1), "num"));
      tr.appendChild(td(fmt(s.aWHIP, 3), "num"));
      tr.appendChild(td(fmt(s.VIJAY, 3), "num"));
      tr.appendChild(td(fmt(s.ERA, 3), "num"));
      tr.appendChild(td(fmt(s.NQW, 3), "num"));
      const rec = (state.data.teamScores?.[r.team]?.[weekKey]?.record) || [0, 0];
      tr.appendChild(td(`${rec[0]}-${rec[1]}`, "num"));
      body.appendChild(tr);
    });
  });
}

function renderScoreboard() {
  const weeklyRendered = renderScoreboardWeeklyTotals();
  if (weeklyRendered) return;
  const currentRendered = renderScoreboardCurrent();
  if (currentRendered) return;
  renderScoreboardLegacy();
}

function renderStandings() {
  const body = clear("standings-body");
  const liveTeams = state.data.liveScoring?.body?.live_scoring?.teams || [];
  if (liveTeams.length) {
    setText("standings-source-label", "Source: current nightly live_scoring snapshot");
    const rows = liveTeams.map((team) => {
      const w = Number(team.w || 0);
      const l = Number(team.l || 0);
      return { team: team.long_abbr || team.name, w, l, p: pct(w, l) };
    });
    rows.sort((a, b) => b.p - a.p);
    const leader = rows[0];
    rows.forEach((r, i) => {
      const tr = document.createElement("tr");
      const gb = leader ? ((leader.w - r.w) / 2).toFixed(1) : "0.0";
      tr.appendChild(td(String(i + 1), "num"));
      tr.appendChild(td(r.team, "team-name"));
      tr.appendChild(td(String(r.w), "num"));
      tr.appendChild(td(String(r.l), "num"));
      tr.appendChild(td((r.p * 100).toFixed(1) + "%", "num"));
      tr.appendChild(td(i === 0 ? "-" : gb, "num"));
      body.appendChild(tr);
    });
    return;
  }

  setText("standings-source-label", "Source: legacy team_scores snapshot");
  const teamScores = state.data.teamScores || {};
  const rows = Object.entries(teamScores).map(([team, payload]) => {
    const rec = payload.season_record || [0, 0];
    return { team, w: Number(rec[0]), l: Number(rec[1]), p: pct(rec[0], rec[1]) };
  });
  rows.sort((a, b) => b.p - a.p);
  const leader = rows[0];
  rows.forEach((r, i) => {
    const tr = document.createElement("tr");
    const gb = leader ? ((leader.w - r.w) / 2).toFixed(1) : "0.0";
    tr.appendChild(td(String(i + 1), "num"));
    tr.appendChild(td(r.team, "team-name"));
    tr.appendChild(td(String(r.w), "num"));
    tr.appendChild(td(String(r.l), "num"));
    tr.appendChild(td((r.p * 100).toFixed(1) + "%", "num"));
    tr.appendChild(td(i === 0 ? "-" : gb, "num"));
    body.appendChild(tr);
  });
}

function renderSplits() {
  setText("splits-source-label", "Source: legacy team_scores snapshot");
  const teamScores = state.data.teamScores || {};
  const body = clear("splits-body");
  Object.entries(teamScores).forEach(([team, payload]) => {
    const weeks = Object.entries(payload).filter(([k]) => k.startsWith("week"));
    let hw = 0, hl = 0, aw = 0, al = 0;
    let hitW = 0, hitTot = 0, pitW = 0, pitTot = 0;
    weeks.forEach(([, stats]) => {
      const rec = stats.record || [0, 0];
      const w = Number(rec[0]) || 0;
      const l = Number(rec[1]) || 0;
      if (stats.away) { aw += w; al += l; } else { hw += w; hl += l; }
      const hitCats = ["HR", "R", "OBP", "OPS", "aRBI", "aSB"];
      const pitCats = ["K", "HRA", "aWHIP", "VIJAY", "ERA", "NQW"];
      hitCats.forEach((c) => { const v = Number(stats[c]); if (!Number.isNaN(v)) { hitTot += 1; if (v > 0) hitW += 0.5; } });
      pitCats.forEach((c) => { const v = Number(stats[c]); if (!Number.isNaN(v)) { pitTot += 1; if (v > 0) pitW += 0.5; } });
    });
    const tr = document.createElement("tr");
    tr.appendChild(td(team, "team-name"));
    tr.appendChild(td((pct(hw, hl) * 100).toFixed(1) + "%", "num"));
    tr.appendChild(td((pct(aw, al) * 100).toFixed(1) + "%", "num"));
    tr.appendChild(td((hitTot ? (hitW / hitTot) * 100 : 0).toFixed(1) + "%", "num"));
    tr.appendChild(td((pitTot ? (pitW / pitTot) * 100 : 0).toFixed(1) + "%", "num"));
    body.appendChild(tr);
  });
}

function renderRoto() {
  const statePayload = state.data.teamWeeklyTotalsState || {};
  const seasonRotoTeams = statePayload?.season_roto?.teams || {};
  const battingCategories = ["HR", "R", "OBP", "OPS", "aRBI", "aSB"];
  const pitchingCategories = ["K", "HRA", "aWHIP", "VIJAY", "ERA", "MGS"];
  const categories = [...battingCategories, ...pitchingCategories];
  const lowerIsBetter = new Set(["ERA", "aWHIP", "HRA"]);
  const teamAgg = {};

  if (Object.keys(seasonRotoTeams).length) {
    Object.values(seasonRotoTeams).forEach((teamState) => {
      const teamId = String(teamState?.team_id || "");
      if (!teamId) return;
      const row = {
        teamId,
        team: teamState?.team_abbr || teamState?.team_name || teamId,
        categoryTotals: {},
        rotoPoints: 0,
      };
      categories.forEach((category) => {
        const seasonTotal = Number(teamState?.categories?.[category]?.season_total ?? 0);
        row.categoryTotals[category] = Number.isNaN(seasonTotal) ? 0 : seasonTotal;
      });
      teamAgg[teamId] = row;
    });
  } else {
    // Backward-compatibility: derive season-like totals from period buckets if season_roto is not present yet.
    const periods = statePayload?.periods || {};
    Object.values(periods).forEach((periodState) => {
      const teams = periodState?.teams || {};
      Object.values(teams).forEach((teamState) => {
        const teamId = String(teamState?.team_id || "");
        if (!teamId) return;
        const row = teamAgg[teamId] || {
          teamId,
          team: teamState?.team_abbr || teamState?.team_name || teamId,
          categoryTotals: {},
          rotoPoints: 0,
        };
        categories.forEach((category) => {
          const weeklyTotal = Number(teamState?.categories?.[category]?.weekly_total ?? 0);
          row.categoryTotals[category] = (row.categoryTotals[category] || 0) + (Number.isNaN(weeklyTotal) ? 0 : weeklyTotal);
        });
        teamAgg[teamId] = row;
      });
    });
  }

  const teamRows = Object.values(teamAgg);
  if (teamRows.length) {
    const hasSeasonRoto = Object.keys(seasonRotoTeams).length > 0;
    setText(
      "roto-source-label",
      hasSeasonRoto
        ? "Source: team_weekly_totals_state.json season_roto (daily-updated season totals)"
        : "Source: team_weekly_totals_state.json periods fallback (season_roto not populated yet)"
    );
    const pointsByTeam = {};
    teamRows.forEach((row) => {
      pointsByTeam[row.teamId] = {};
    });

    categories.forEach((category) => {
      const scored = teamRows.map((row) => ({
        row,
        value: Number(row.categoryTotals[category] ?? 0),
      }));
      scored.sort((a, b) => {
        if (Math.abs(a.value - b.value) < 1e-12) return 0;
        return lowerIsBetter.has(category) ? a.value - b.value : b.value - a.value;
      });

      let idx = 0;
      const n = scored.length;
      while (idx < n) {
        const groupStart = idx;
        const value = scored[idx].value;
        while (idx < n && Math.abs(scored[idx].value - value) < 1e-12) idx += 1;
        const groupEnd = idx - 1;
        let pointsSum = 0;
        for (let pos = groupStart; pos <= groupEnd; pos += 1) {
          pointsSum += (n - pos);
        }
        const avgPoints = pointsSum / (groupEnd - groupStart + 1);
        for (let pos = groupStart; pos <= groupEnd; pos += 1) {
          scored[pos].row.rotoPoints += avgPoints;
          pointsByTeam[scored[pos].row.teamId][category] = avgPoints;
        }
      }
    });

    teamRows.forEach((row) => {
      const teamPoints = pointsByTeam[row.teamId] || {};
      row.battingPoints = battingCategories.reduce((sum, category) => sum + Number(teamPoints[category] || 0), 0);
      row.pitchingPoints = pitchingCategories.reduce((sum, category) => sum + Number(teamPoints[category] || 0), 0);
      row.totalRotoPoints = row.battingPoints + row.pitchingPoints;
      categories.forEach((category) => {
        row[`pts_${category}`] = Number(teamPoints[category] || 0);
      });
    });

    const byTotalRoto = [...teamRows].sort((a, b) => b.totalRotoPoints - a.totalRotoPoints || String(a.team).localeCompare(String(b.team)));
    byTotalRoto.forEach((row, idx) => {
      row.rank = idx + 1;
    });

    const battingSort = state.rotoSort.batting || { key: "battingPoints", dir: "desc" };
    const pitchingSort = state.rotoSort.pitching || { key: "pitchingPoints", dir: "desc" };
    const summarySort = state.rotoSort.summary || { key: "totalRotoPoints", dir: "desc" };
    const battingKey = battingCategories.includes(battingSort.key) ? battingSort.key : (battingSort.key === "team" || battingSort.key === "battingPoints" ? battingSort.key : "battingPoints");
    const pitchingKey = pitchingCategories.includes(pitchingSort.key) ? pitchingSort.key : (pitchingSort.key === "team" || pitchingSort.key === "pitchingPoints" ? pitchingSort.key : "pitchingPoints");
    const summaryKey = ["team", "battingPoints", "pitchingPoints", "totalRotoPoints", "rank"].includes(summarySort.key) ? summarySort.key : "totalRotoPoints";
    const battingRows = sortRows(teamRows, battingKey, battingSort.dir);
    const pitchingRows = sortRows(teamRows, pitchingKey, pitchingSort.dir);
    const summaryRows = sortRows(teamRows, summaryKey, summarySort.dir);

    const battingBody = clear("roto-batting-body");
    const pitchingBody = clear("roto-pitching-body");
    const summaryBody = clear("roto-summary-body");

    battingRows.forEach((r) => {
      const battingTotalsRow = document.createElement("tr");
      battingTotalsRow.appendChild(td(r.team, "team-name"));
      battingCategories.forEach((category) => {
        battingTotalsRow.appendChild(td(fmt(r.categoryTotals[category], ["OBP", "OPS"].includes(category) ? 3 : 1), "num"));
      });
      battingTotalsRow.appendChild(td(r.battingPoints.toFixed(2), "num"));
      battingBody.appendChild(battingTotalsRow);

      const battingPointsRow = document.createElement("tr");
      battingPointsRow.appendChild(td(`${r.team} (Pts)`, "team-name"));
      battingCategories.forEach((category) => {
        battingPointsRow.appendChild(td((r[`pts_${category}`] || 0).toFixed(2), "num"));
      });
      battingPointsRow.appendChild(td(r.battingPoints.toFixed(2), "num"));
      battingBody.appendChild(battingPointsRow);
    });

    pitchingRows.forEach((r) => {
      const pitchingTotalsRow = document.createElement("tr");
      pitchingTotalsRow.appendChild(td(r.team, "team-name"));
      pitchingCategories.forEach((category) => {
        pitchingTotalsRow.appendChild(td(fmt(r.categoryTotals[category], ["aWHIP", "VIJAY", "ERA", "MGS"].includes(category) ? 3 : 1), "num"));
      });
      pitchingTotalsRow.appendChild(td(r.pitchingPoints.toFixed(2), "num"));
      pitchingBody.appendChild(pitchingTotalsRow);

      const pitchingPointsRow = document.createElement("tr");
      pitchingPointsRow.appendChild(td(`${r.team} (Pts)`, "team-name"));
      pitchingCategories.forEach((category) => {
        pitchingPointsRow.appendChild(td((r[`pts_${category}`] || 0).toFixed(2), "num"));
      });
      pitchingPointsRow.appendChild(td(r.pitchingPoints.toFixed(2), "num"));
      pitchingBody.appendChild(pitchingPointsRow);
    });

    summaryRows.forEach((r) => {
      const summaryRow = document.createElement("tr");
      summaryRow.appendChild(td(r.team, "team-name"));
      summaryRow.appendChild(td(r.battingPoints.toFixed(2), "num"));
      summaryRow.appendChild(td(r.pitchingPoints.toFixed(2), "num"));
      summaryRow.appendChild(td(r.totalRotoPoints.toFixed(2), "num"));
      summaryRow.appendChild(td(String(r.rank), "num"));
      summaryBody.appendChild(summaryRow);
    });
    updateRotoSortIndicators();
    return;
  }

  setText("roto-source-label", "Source: legacy team_scores snapshot");
  const teamScores = state.data.teamScores || {};
  const fallbackRows = Object.entries(teamScores).map(([team, payload]) => {
    let total = 0;
    Object.entries(payload).forEach(([k, stats]) => {
      if (!k.startsWith("week")) return;
      const rec = stats.record || [0, 0];
      total += (Number(rec[0]) || 0) - (Number(rec[1]) || 0);
    });
    return { team, total };
  });
  fallbackRows.sort((a, b) => b.total - a.total);
  const summaryBody = clear("roto-summary-body");
  fallbackRows.forEach((r, i) => {
    const tr = document.createElement("tr");
    tr.appendChild(td(r.team, "team-name"));
    tr.appendChild(td("—", "num"));
    tr.appendChild(td("—", "num"));
    tr.appendChild(td(String(r.total), "num"));
    tr.appendChild(td(String(i + 1), "num"));
    summaryBody.appendChild(tr);
  });
  const battingBody = clear("roto-batting-body");
  const pitchingBody = clear("roto-pitching-body");
  const trBat = document.createElement("tr");
  trBat.appendChild(td("No season roto state available.", "team-name"));
  for (let idx = 0; idx < battingCategories.length + 1; idx += 1) {
    trBat.appendChild(td("—", "num"));
  }
  battingBody.appendChild(trBat);
  const trPit = document.createElement("tr");
  trPit.appendChild(td("No season roto state available.", "team-name"));
  for (let idx = 0; idx < pitchingCategories.length + 1; idx += 1) {
    trPit.appendChild(td("—", "num"));
  }
  pitchingBody.appendChild(trPit);
  updateRotoSortIndicators();
}

function renderRules() {
  setText("rules-pre", state.data.rules || "Rules file not available.");
}

async function init() {
  try {
    setupRotoSorting();
    await loadAll();
    renderHeader();
    renderScoreboard();
    renderStandings();
    renderSplits();
    renderRoto();
    renderRules();
  } catch (e) {
    setText("asOf", `Load error: ${e.message}`);
  }
}

init();
