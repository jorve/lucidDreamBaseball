import math
import random
from datetime import datetime, timedelta, timezone

from analytics.io import read_json, write_json
from project_config import (
	get_artifact_history_latest_path,
	get_clap_calibration_latest_path,
	get_clap_player_history_latest_path,
	get_clap_v2_latest_path,
	get_ingestion_config,
	get_matchup_expectations_latest_path,
	get_player_projection_weekly_latest_path,
	get_roster_state_latest_path,
	get_team_weekly_totals_latest_path,
	get_team_weekly_totals_state_path,
)


UTC = timezone.utc


class ClapV2Error(RuntimeError):
	pass


class ClapV2Builder:
	SCORE_CATEGORIES = ["HR", "R", "OBP", "OPS", "aRBI", "aSB", "K", "HRA", "aWHIP", "VIJAY", "ERA", "MGS"]
	LOWER_IS_BETTER = {"ERA", "aWHIP", "HRA"}
	ROLE_KEYS = ("batters", "rp", "sp")

	def __init__(self):
		self.ingestion_cfg = get_ingestion_config()
		self.clap_cfg = self.ingestion_cfg.get("clap_v2", {})

	def build(self, target_date, dry_run=False):
		clap_path = get_clap_v2_latest_path()
		player_history_path = get_clap_player_history_latest_path()
		matchup_path = get_matchup_expectations_latest_path()
		calibration_path = get_clap_calibration_latest_path()
		if not self.clap_cfg.get("enabled", True):
			return {
				"status": "skipped",
				"reason": "CLAP_V2_DISABLED",
				"output_path": clap_path,
				"player_history_output_path": player_history_path,
				"matchup_output_path": matchup_path,
				"calibration_output_path": calibration_path,
			}

		projection_path = get_player_projection_weekly_latest_path()
		roster_path = get_roster_state_latest_path()
		weekly_totals_path = get_team_weekly_totals_latest_path()
		weekly_state_path = get_team_weekly_totals_state_path()
		if (not projection_path.exists()) or (not roster_path.exists()) or (not weekly_totals_path.exists()) or (not weekly_state_path.exists()):
			return {
				"status": "skipped",
				"reason": "CLAP_V2_INPUTS_MISSING",
				"output_path": clap_path,
				"player_history_output_path": player_history_path,
				"matchup_output_path": matchup_path,
				"calibration_output_path": calibration_path,
			}

		projection_payload = read_json(projection_path)
		roster_payload = read_json(roster_path)
		weekly_totals_payload = read_json(weekly_totals_path)
		weekly_state_payload = read_json(weekly_state_path)

		player_history_payload = self._build_player_history_payload(
			target_date=target_date,
			projection_payload=projection_payload,
			weekly_state_payload=weekly_state_payload,
		)
		player_profiles = self._build_player_profiles(player_history_payload, projection_payload)
		team_models = self._build_team_models(roster_payload, player_profiles)
		if not team_models:
			return {
				"status": "skipped",
				"reason": "CLAP_V2_NO_TEAM_SIGNAL",
				"output_path": clap_path,
				"player_history_output_path": player_history_path,
				"matchup_output_path": matchup_path,
				"calibration_output_path": calibration_path,
			}

		matchups = self._resolve_matchups(weekly_totals_payload, team_models)
		analytic_rows = self._compute_matchup_probabilities(team_models, matchups, engine_name="analytic_normal")
		mc_rows = self._compute_matchup_probabilities(team_models, matchups, engine_name="monte_carlo")
		selected_engine = str(self.clap_cfg.get("selected_engine", "analytic_normal"))
		if selected_engine not in {"analytic_normal", "monte_carlo", "auto"}:
			selected_engine = "analytic_normal"

		league_baseline = self._league_baseline(team_models)
		now_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")
		clap_payload = {
			"schema_version": "1.0",
			"generated_at_utc": now_utc,
			"target_date": target_date.strftime("%Y-%m-%d"),
			"period_key": str(weekly_totals_payload.get("period_key", "")),
			"summary": {
				"teams": len(team_models),
				"matchups": len(matchups),
				"players_profiled": len(player_profiles),
			},
			"config": {
				"selected_engine": selected_engine,
				"monte_carlo_samples": int(self.clap_cfg.get("monte_carlo_samples", 3000)),
				"player_cv": float(self.clap_cfg.get("player_cv", 0.35)),
				"min_sigma": float(self.clap_cfg.get("min_sigma", 0.05)),
				"stabilization_samples_weekly": int(self.clap_cfg.get("stabilization_samples_weekly", 6)),
				"stabilization_samples_starts": int(self.clap_cfg.get("stabilization_samples_starts", 6)),
			},
			"league_baseline": league_baseline,
			"teams": [team_models[team_id] for team_id in sorted(team_models.keys())],
		}

		matchup_payload = {
			"schema_version": "1.0",
			"generated_at_utc": now_utc,
			"target_date": target_date.strftime("%Y-%m-%d"),
			"period_key": str(weekly_totals_payload.get("period_key", "")),
			"selected_engine": selected_engine,
			"summary": {
				"matchups": len(matchups),
			},
			"matchups": self._merge_engine_rows(
				analytic_rows=analytic_rows,
				monte_carlo_rows=mc_rows,
				selected_engine=selected_engine,
			),
		}
		calibration_payload = self._build_calibration_payload(target_date, matchup_payload)

		if not dry_run:
			write_json(clap_path, clap_payload)
			write_json(player_history_path, player_history_payload)
			write_json(matchup_path, matchup_payload)
			write_json(calibration_path, calibration_payload)
		return {
			"status": "ok",
			"output_path": clap_path,
			"player_history_output_path": player_history_path,
			"matchup_output_path": matchup_path,
			"calibration_output_path": calibration_path,
			"summary": {
				"teams": len(team_models),
				"matchups": len(matchups),
				"players_profiled": len(player_profiles),
				"recommended_engine": calibration_payload.get("engine_recommendation", {}).get("recommended"),
			},
		}

	def _build_player_history_payload(self, target_date, projection_payload, weekly_state_payload):
		projection_by_player = {}
		for row in projection_payload.get("players", []):
			player_id = str(row.get("player_id", "")).strip()
			if player_id:
				projection_by_player[player_id] = row

		weekly_samples_by_player = {}
		vijay_appearance_values_by_player_period = {}
		sp_start_samples_by_player = {}
		sp_start_count_by_player_week = {}
		start_seen = {}
		periods = weekly_state_payload.get("periods", {}) if isinstance(weekly_state_payload, dict) else {}
		min_outs = float(self.clap_cfg.get("sp_start_min_outs", 9))
		for period_key, period_state in (periods or {}).items():
			teams = (period_state or {}).get("teams", {})
			for team_state in (teams or {}).values():
				players = (team_state or {}).get("players", {})
				for player_id_raw, player_state in (players or {}).items():
					player_id = str(player_id_raw)
					role = self._player_role(projection_by_player.get(player_id, {}))
					if role not in self.ROLE_KEYS:
						continue
					player_name = (
						self._safe_str(projection_by_player.get(player_id, {}).get("player_name"))
						or self._safe_str((player_state or {}).get("player_name"))
						or f"UNKNOWN_{player_id}"
					)
					if role in {"batters", "rp"}:
						weekly_by_period = weekly_samples_by_player.setdefault(player_id, {"role": role, "player_name": player_name, "periods": {}})
						period_bucket = weekly_by_period["periods"].setdefault(period_key, {category: 0.0 for category in self.SCORE_CATEGORIES})
						for category in self.SCORE_CATEGORIES:
							category_state = (player_state.get("categories", {}).get(category, {}) or {})
							value = self._safe_float(category_state.get("weekly_total"))
							period_bucket[category] = period_bucket.get(category, 0.0) + value
							if category == "VIJAY":
								daily_values = category_state.get("daily_values", {}) if isinstance(category_state, dict) else {}
								period_appearances = vijay_appearance_values_by_player_period.setdefault(player_id, {}).setdefault(period_key, [])
								for date_key in sorted((daily_values or {}).keys()):
									period_appearances.append(self._safe_float((daily_values or {}).get(date_key)))
					else:
						weekly_count_map = sp_start_count_by_player_week.setdefault(player_id, {})
						start_dates = start_seen.setdefault(player_id, set())
						categories = (player_state or {}).get("categories", {})
						derived_daily = (((player_state or {}).get("derived_inputs", {}) or {}).get("daily", {}) or {})
						date_keys = set()
						for category_state in (categories or {}).values():
							daily_values = (category_state or {}).get("daily_values", {})
							date_keys.update(daily_values.keys())
						date_keys.update((derived_daily or {}).keys())
						for date_key in sorted(date_keys):
							components = (derived_daily or {}).get(date_key, {}) or {}
							outs_value = self._safe_float((components or {}).get("IP_OUTS"))
							is_start = outs_value >= min_outs
							if not is_start:
								continue
							start_token = f"{period_key}:{date_key}"
							if start_token in start_dates:
								continue
							start_dates.add(start_token)
							weekly_count_map[period_key] = int(weekly_count_map.get(period_key, 0)) + 1
							player_start_rows = sp_start_samples_by_player.setdefault(
								player_id,
								{"role": "sp", "player_name": player_name, "starts": []},
							)
							start_row = {"period_key": period_key, "date": date_key, "categories": {}}
							for category in self.SCORE_CATEGORIES:
								daily_values = (categories.get(category, {}) or {}).get("daily_values", {})
								start_row["categories"][category] = self._safe_float((daily_values or {}).get(date_key))
							player_start_rows["starts"].append(start_row)

		players = []
		for player_id, row in projection_by_player.items():
			role = self._player_role(row)
			player_name = self._safe_str(row.get("player_name")) or f"UNKNOWN_{player_id}"
			entry = {
				"player_id": player_id,
				"player_name": player_name,
				"role": role,
				"weekly_samples": {},
				"weekly_component_samples": {},
				"vijay_appearance_values": [],
				"vijay_weekly_sum_samples": {"values": [], "mu": 0.0, "sigma": 0.0, "n_weeks": 0},
				"per_start_samples": {},
				"weekly_start_count_signal": {
					"observed": {"values": [], "mu": 0.0, "sigma": 0.0, "n_weeks": 0},
					"expected_starts_week": self._expected_starts_week(row),
				},
			}
			if role in {"batters", "rp"}:
				period_map = (weekly_samples_by_player.get(player_id, {}) or {}).get("periods", {})
				period_values = list((period_map or {}).values())
				for category in self.SCORE_CATEGORIES:
					values = [self._safe_float((period_value or {}).get(category)) for period_value in period_values]
					entry["weekly_samples"][category] = self._sample_stats(values, n_label="n_weeks")
					entry["weekly_component_samples"][category] = {
						"source": "appearance_summed" if category == "VIJAY" else "component_derived",
						"sample_stats": self._sample_stats(values, n_label="n_weeks"),
					}
				vijay_period_values = []
				flat_appearances = []
				for period_key in sorted((period_map or {}).keys()):
					appearances = list((vijay_appearance_values_by_player_period.get(player_id, {}) or {}).get(period_key, []))
					if appearances:
						entry["vijay_appearance_values"].append({"period_key": period_key, "values": [round(self._safe_float(value), 6) for value in appearances]})
						vijay_period_values.append(sum(self._safe_float(value) for value in appearances))
						flat_appearances.extend([self._safe_float(value) for value in appearances])
					else:
						weekly_vijay = self._safe_float((period_map.get(period_key, {}) or {}).get("VIJAY"))
						if abs(weekly_vijay) > 0.0:
							entry["vijay_appearance_values"].append({"period_key": period_key, "values": [round(weekly_vijay, 6)]})
							vijay_period_values.append(weekly_vijay)
							flat_appearances.append(weekly_vijay)
				entry["vijay_weekly_sum_samples"] = self._sample_stats(vijay_period_values, n_label="n_weeks")
				entry["vijay_appearance_count"] = len(flat_appearances)
			elif role == "sp":
				start_rows = list((sp_start_samples_by_player.get(player_id, {}) or {}).get("starts", []))
				for category in self.SCORE_CATEGORIES:
					values = [self._safe_float((start_row.get("categories", {}) or {}).get(category)) for start_row in start_rows]
					stats = self._sample_stats(values, n_label="n_starts")
					entry["per_start_samples"][category] = {
						"values": stats.get("values", []),
						"mu_start": stats.get("mu", 0.0),
						"sigma_start": stats.get("sigma", 0.0),
						"n_starts": stats.get("n_starts", 0),
					}
				weekly_counts = [int(value) for value in (sp_start_count_by_player_week.get(player_id, {}) or {}).values()]
				entry["weekly_start_count_signal"]["observed"] = self._sample_stats(weekly_counts, n_label="n_weeks")
			players.append(entry)
		players.sort(key=lambda row: (row.get("role", ""), row.get("player_id", "")))
		return {
			"schema_version": "1.0",
			"generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
			"target_date": target_date.strftime("%Y-%m-%d"),
			"summary": {
				"players": len(players),
				"batters": len([row for row in players if row.get("role") == "batters"]),
				"rp": len([row for row in players if row.get("role") == "rp"]),
				"sp": len([row for row in players if row.get("role") == "sp"]),
			},
			"players": players,
		}

	def _build_player_profiles(self, player_history_payload, projection_payload):
		projection_by_player = {}
		for row in projection_payload.get("players", []):
			player_id = str(row.get("player_id", "")).strip()
			if player_id:
				projection_by_player[player_id] = row

		profiles = {}
		stability_weekly = max(1, int(self.clap_cfg.get("stabilization_samples_weekly", 6)))
		stability_starts = max(1, int(self.clap_cfg.get("stabilization_samples_starts", 6)))
		min_sigma = float(self.clap_cfg.get("min_sigma", 0.05))
		player_cv = float(self.clap_cfg.get("player_cv", 0.35))
		for history_row in player_history_payload.get("players", []):
			player_id = str(history_row.get("player_id", "")).strip()
			if not player_id:
				continue
			projection_row = projection_by_player.get(player_id, {})
			role = self._player_role(projection_row) or self._safe_str(history_row.get("role"))
			profile = {
				"player_id": player_id,
				"player_name": self._safe_str(history_row.get("player_name")) or self._safe_str(projection_row.get("player_name")) or f"UNKNOWN_{player_id}",
				"role": role,
				"projected_points_window": self._safe_float(projection_row.get("projected_points_window")),
				"expected_starts_week": self._expected_starts_week(projection_row),
				"categories": {},
			}
			for category in self.SCORE_CATEGORIES:
				if role in {"batters", "rp"}:
					if role == "rp" and category == "VIJAY":
						obs = (history_row.get("vijay_weekly_sum_samples", {}) or {})
					else:
						obs = (history_row.get("weekly_samples", {}) or {}).get(category, {})
					n_obs = int(obs.get("n_weeks", 0) or 0)
					obs_mu = self._safe_float(obs.get("mu"))
					obs_sigma = self._safe_float(obs.get("sigma"))
					prior_mu = self._projection_prior_mu(projection_row, category, role=role)
					prior_sigma = max(min_sigma, abs(prior_mu) * player_cv) if abs(prior_mu) > 0 else 0.0
					blend = self._blend_distribution(
						prior_mu=prior_mu,
						prior_sigma=prior_sigma,
						observed_mu=obs_mu,
						observed_sigma=obs_sigma,
						n_observed=n_obs,
						n_stabilize=stability_weekly,
						min_sigma=min_sigma,
					)
					profile["categories"][category] = {
						"mu": blend["mu"],
						"sigma": blend["sigma"],
						"n_observed": n_obs,
						"provenance": "appearance_summed" if (role == "rp" and category == "VIJAY") else "component_derived",
					}
				elif role == "sp":
					obs = (history_row.get("per_start_samples", {}) or {}).get(category, {})
					n_obs = int(obs.get("n_starts", 0) or 0)
					obs_mu_start = self._safe_float(obs.get("mu_start"))
					obs_sigma_start = self._safe_float(obs.get("sigma_start"))
					expected_starts = max(0.0, self._safe_float(history_row.get("weekly_start_count_signal", {}).get("expected_starts_week", profile["expected_starts_week"])))
					prior_weekly_mu = self._projection_prior_mu(projection_row, category, role=role)
					prior_start_mu = prior_weekly_mu / expected_starts if expected_starts > 1e-9 else prior_weekly_mu
					prior_start_sigma = max(min_sigma, abs(prior_start_mu) * player_cv) if abs(prior_start_mu) > 0 else 0.0
					blend = self._blend_distribution(
						prior_mu=prior_start_mu,
						prior_sigma=prior_start_sigma,
						observed_mu=obs_mu_start,
						observed_sigma=obs_sigma_start,
						n_observed=n_obs,
						n_stabilize=stability_starts,
						min_sigma=min_sigma,
					)
					weekly_mu = blend["mu"] * expected_starts
					weekly_sigma = math.sqrt(max(expected_starts, 0.0) * (blend["sigma"] ** 2)) if expected_starts > 0 else 0.0
					profile["categories"][category] = {
						"mu": round(weekly_mu, 6),
						"sigma": round(weekly_sigma, 6),
						"n_observed": n_obs,
						"provenance": "per_start_aggregated",
					}
				else:
					profile["categories"][category] = {"mu": 0.0, "sigma": 0.0, "n_observed": 0, "provenance": "unknown"}
			profiles[player_id] = profile

		for player_id, projection_row in projection_by_player.items():
			if player_id in profiles:
				continue
			role = self._player_role(projection_row)
			profile = {
				"player_id": player_id,
				"player_name": self._safe_str(projection_row.get("player_name")) or f"UNKNOWN_{player_id}",
				"role": role,
				"projected_points_window": self._safe_float(projection_row.get("projected_points_window")),
				"expected_starts_week": self._expected_starts_week(projection_row),
				"categories": {},
			}
			for category in self.SCORE_CATEGORIES:
				prior_mu = self._projection_prior_mu(projection_row, category, role=role)
				profile["categories"][category] = {
					"mu": round(prior_mu, 6),
					"sigma": 0.0,
					"n_observed": 0,
					"provenance": "prior_only",
				}
			profiles[player_id] = profile
		return profiles

	def _build_team_models(self, roster_payload, player_profiles):
		team_models = {}
		for team in roster_payload.get("teams", []):
			team_id = str(team.get("team_id", ""))
			if not team_id:
				continue
			categories = {category: {"mu": 0.0, "var": 0.0, "contributors": 0} for category in self.SCORE_CATEGORIES}
			role_categories = {
				role: {category: {"mu": 0.0, "var": 0.0} for category in self.SCORE_CATEGORIES}
				for role in self.ROLE_KEYS
			}
			team_projection_points = 0.0
			contributing_players = 0
			expected_sp_starts = 0.0
			for player in team.get("players", []):
				player_id = str(player.get("player_id", ""))
				if not player_id:
					continue
				profile = player_profiles.get(player_id)
				if not profile:
					continue
				role = profile.get("role", "")
				contributing_players += 1
				team_projection_points += self._safe_float(profile.get("projected_points_window"))
				if role == "sp":
					expected_sp_starts += self._safe_float(profile.get("expected_starts_week"))
				for category in self.SCORE_CATEGORIES:
					player_mu = self._safe_float((profile.get("categories", {}).get(category, {}) or {}).get("mu"))
					player_sigma = self._safe_float((profile.get("categories", {}).get(category, {}) or {}).get("sigma"))
					categories[category]["mu"] += player_mu
					categories[category]["var"] += player_sigma * player_sigma
					categories[category]["contributors"] += 1
					if role in role_categories:
						role_categories[role][category]["mu"] += player_mu
						role_categories[role][category]["var"] += player_sigma * player_sigma
			team_models[team_id] = {
				"team_id": team_id,
				"team_name": team.get("team_name"),
				"projected_points_window": round(team_projection_points, 6),
				"players_projected": contributing_players,
				"expected_sp_starts_week": round(expected_sp_starts, 6),
				"categories": {
					category: {
						"mu": round(category_state["mu"], 6),
						"sigma": round(math.sqrt(max(category_state["var"], 0.0)), 6),
						"contributors": int(category_state["contributors"]),
					}
					for category, category_state in categories.items()
				},
				"role_categories": {
					role: {
						category: {
							"mu": round(role_categories[role][category]["mu"], 6),
							"sigma": round(math.sqrt(max(role_categories[role][category]["var"], 0.0)), 6),
						}
						for category in self.SCORE_CATEGORIES
					}
					for role in self.ROLE_KEYS
				},
			}
		return team_models

	def _resolve_matchups(self, weekly_totals_payload, team_models):
		matchups = []
		for matchup in weekly_totals_payload.get("matchups", []):
			away_id = str(matchup.get("away_team_id", "")).strip()
			home_id = str(matchup.get("home_team_id", "")).strip()
			if not away_id or not home_id:
				continue
			if away_id not in team_models or home_id not in team_models:
				continue
			matchups.append(
				{
					"matchup_id": str(matchup.get("matchup_id", "")),
					"away_team_id": away_id,
					"home_team_id": home_id,
					"away_team_abbr": matchup.get("away_team_abbr"),
					"home_team_abbr": matchup.get("home_team_abbr"),
				}
			)
		return matchups

	def _compute_matchup_probabilities(self, team_models, matchups, engine_name):
		rows = []
		for matchup in matchups:
			away_id = matchup["away_team_id"]
			home_id = matchup["home_team_id"]
			away_team = team_models.get(away_id, {})
			home_team = team_models.get(home_id, {})
			categories = {}
			away_expected = 0.0
			home_expected = 0.0
			for category in self.SCORE_CATEGORIES:
				away_signal = away_team.get("categories", {}).get(category, {})
				home_signal = home_team.get("categories", {}).get(category, {})
				away_mu = self._safe_float(away_signal.get("mu"))
				away_sigma = self._safe_float(away_signal.get("sigma"))
				home_mu = self._safe_float(home_signal.get("mu"))
				home_sigma = self._safe_float(home_signal.get("sigma"))
				if engine_name == "monte_carlo":
					p_away = self._monte_carlo_probability(
						away_mu=away_mu,
						away_sigma=away_sigma,
						home_mu=home_mu,
						home_sigma=home_sigma,
						category=category,
						seed_material=f"{away_id}:{home_id}:{category}",
					)
				else:
					p_away = self._analytic_probability(
						away_mu=away_mu,
						away_sigma=away_sigma,
						home_mu=home_mu,
						home_sigma=home_sigma,
						category=category,
					)
				p_away = min(max(p_away, 0.0), 1.0)
				p_home = 1.0 - p_away
				away_expected += p_away
				home_expected += p_home
				role_contributions = {
					"away": self._role_contribution_snapshot(away_team, category),
					"home": self._role_contribution_snapshot(home_team, category),
				}
				dominant_role = self._dominant_role(role_contributions)
				category_source = "appearance_summed" if category == "VIJAY" else "component_derived"
				categories[category] = {
					"away_win_prob": round(p_away, 6),
					"home_win_prob": round(p_home, 6),
					"away_mu": round(away_mu, 6),
					"away_sigma": round(max(away_sigma, 0.0), 6),
					"home_mu": round(home_mu, 6),
					"home_sigma": round(max(home_sigma, 0.0), 6),
					"away_sp_expected_starts": round(self._safe_float(away_team.get("expected_sp_starts_week")), 6),
					"home_sp_expected_starts": round(self._safe_float(home_team.get("expected_sp_starts_week")), 6),
					"provenance": {"batters": "component_derived", "rp": "component_derived", "sp": "per_start_aggregated"},
					"category_source": category_source,
					"role_contributions": role_contributions,
					"dominant_role": dominant_role,
				}
			rows.append(
				{
					"matchup_id": matchup["matchup_id"],
					"away_team_id": away_id,
					"home_team_id": home_id,
					"away_team_abbr": matchup.get("away_team_abbr"),
					"home_team_abbr": matchup.get("home_team_abbr"),
					"categories": categories,
					"expected_score": {
						"away": round(away_expected, 6),
						"home": round(home_expected, 6),
					},
				}
			)
		return rows

	def _analytic_probability(self, away_mu, away_sigma, home_mu, home_sigma, category):
		diff_mu = away_mu - home_mu
		if category in self.LOWER_IS_BETTER:
			diff_mu = home_mu - away_mu
		diff_sigma = math.sqrt(max((away_sigma * away_sigma) + (home_sigma * home_sigma), 0.0))
		if diff_sigma <= 1e-9:
			if abs(diff_mu) <= 1e-12:
				return 0.0
			return 1.0 if diff_mu > 0 else 0.0
		return self._normal_cdf(diff_mu / diff_sigma)

	def _monte_carlo_probability(self, away_mu, away_sigma, home_mu, home_sigma, category, seed_material):
		samples = int(self.clap_cfg.get("monte_carlo_samples", 3000))
		samples = max(250, samples)
		seed = int(self.clap_cfg.get("random_seed", 42)) + self._stable_seed(seed_material)
		rng = random.Random(seed)
		away_wins = 0
		for _ in range(samples):
			away_sample = rng.gauss(away_mu, max(away_sigma, 1e-9))
			home_sample = rng.gauss(home_mu, max(home_sigma, 1e-9))
			if abs(away_sample - home_sample) <= 1e-12:
				continue
			if category in self.LOWER_IS_BETTER:
				away_wins += 1 if away_sample < home_sample else 0
			else:
				away_wins += 1 if away_sample > home_sample else 0
		return away_wins / float(samples)

	def _merge_engine_rows(self, analytic_rows, monte_carlo_rows, selected_engine):
		analytic_by_matchup = {row["matchup_id"]: row for row in analytic_rows}
		mc_by_matchup = {row["matchup_id"]: row for row in monte_carlo_rows}
		merged = []
		for matchup_id in sorted(set(analytic_by_matchup.keys()) | set(mc_by_matchup.keys())):
			analytic = analytic_by_matchup.get(matchup_id, {})
			mc = mc_by_matchup.get(matchup_id, {})
			chosen = analytic if selected_engine == "analytic_normal" else mc
			if selected_engine == "auto":
				chosen = analytic
			merged.append(
				{
					"matchup_id": matchup_id,
					"away_team_id": chosen.get("away_team_id", analytic.get("away_team_id", mc.get("away_team_id"))),
					"home_team_id": chosen.get("home_team_id", analytic.get("home_team_id", mc.get("home_team_id"))),
					"away_team_abbr": chosen.get("away_team_abbr", analytic.get("away_team_abbr", mc.get("away_team_abbr"))),
					"home_team_abbr": chosen.get("home_team_abbr", analytic.get("home_team_abbr", mc.get("home_team_abbr"))),
					"engines": {
						"analytic_normal": {
							"categories": analytic.get("categories", {}),
							"expected_score": analytic.get("expected_score", {"away": 0.0, "home": 12.0}),
						},
						"monte_carlo": {
							"categories": mc.get("categories", {}),
							"expected_score": mc.get("expected_score", {"away": 0.0, "home": 12.0}),
						},
					},
					"selected_engine": selected_engine,
					"selected": {
						"categories": chosen.get("categories", {}),
						"expected_score": chosen.get("expected_score", {"away": 0.0, "home": 12.0}),
					},
				}
			)
		return merged

	def _league_baseline(self, team_models):
		team_count = max(1, len(team_models))
		rows = {}
		for category in self.SCORE_CATEGORIES:
			mu_values = [self._safe_float(team.get("categories", {}).get(category, {}).get("mu")) for team in team_models.values()]
			sigma_values = [self._safe_float(team.get("categories", {}).get(category, {}).get("sigma")) for team in team_models.values()]
			mu = sum(mu_values) / float(team_count)
			sigma = sum(sigma_values) / float(team_count)
			stability = 0.0 if abs(mu) <= 1e-12 else max(0.0, 1.0 - (abs(sigma / mu)))
			rows[category] = {
				"mu": round(mu, 6),
				"sigma": round(sigma, 6),
				"sample_size": int(team_count),
				"stability": round(min(max(stability, 0.0), 1.0), 6),
			}
		return rows

	def _build_calibration_payload(self, target_date, matchup_payload):
		lookback_days = int(self.clap_cfg.get("calibration_lookback_days", 45))
		engine_rows = {"analytic_normal": [], "monte_carlo": []}
		role_rows = {role: {"analytic_normal": [], "monte_carlo": []} for role in self.ROLE_KEYS}
		sp_bucket_rows = {"1-start": {"analytic_normal": [], "monte_carlo": []}, "2-start": {"analytic_normal": [], "monte_carlo": []}}
		source_rows = {"component_derived": {"analytic_normal": [], "monte_carlo": []}, "appearance_summed": {"analytic_normal": [], "monte_carlo": []}}
		history_root = get_artifact_history_latest_path().parent / "history"
		cutoff_date = target_date.date() - timedelta(days=max(1, lookback_days))
		if history_root.exists():
			for child in sorted(history_root.iterdir(), reverse=True):
				if not child.is_dir():
					continue
				try:
					day_value = datetime.strptime(child.name, "%Y-%m-%d").date()
				except Exception:
					continue
				if day_value < cutoff_date:
					continue
				matchup_history_path = child / "matchup_expectations_latest.json"
				weekly_totals_path = child / "team_weekly_totals_latest.json"
				if not matchup_history_path.exists() or not weekly_totals_path.exists():
					continue
				matchup_history = read_json(matchup_history_path)
				realized_history = read_json(weekly_totals_path)
				self._collect_engine_calibration_rows(engine_rows, role_rows, sp_bucket_rows, source_rows, matchup_history, realized_history)

		metrics = {}
		for engine_name, rows in engine_rows.items():
			metrics[engine_name] = self._engine_metric_snapshot(rows)
		metrics["role_segments"] = {
			role: {engine_name: self._engine_metric_snapshot(rows) for engine_name, rows in role_payload.items()}
			for role, role_payload in role_rows.items()
		}
		metrics["sp_start_buckets"] = {
			bucket: {engine_name: self._engine_metric_snapshot(rows) for engine_name, rows in bucket_payload.items()}
			for bucket, bucket_payload in sp_bucket_rows.items()
		}
		metrics["category_source_diagnostics"] = {
			"component_derived_categories": [category for category in self.SCORE_CATEGORIES if category != "VIJAY"],
			"appearance_summed_categories": ["VIJAY"],
			"source_segments": {
				source_name: {engine_name: self._engine_metric_snapshot(rows) for engine_name, rows in source_payload.items()}
				for source_name, source_payload in source_rows.items()
			},
		}
		recommended = self._recommended_engine(metrics)
		selected = str(matchup_payload.get("selected_engine", "analytic_normal"))
		if selected == "auto":
			selected = recommended
		return {
			"schema_version": "1.0",
			"generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
			"target_date": target_date.strftime("%Y-%m-%d"),
			"window": {"lookback_days": lookback_days},
			"metrics": metrics,
			"engine_recommendation": {
				"recommended": recommended,
				"selected": selected,
				"selection_mode": matchup_payload.get("selected_engine", "analytic_normal"),
			},
		}

	def _collect_engine_calibration_rows(self, engine_rows, role_rows, sp_bucket_rows, source_rows, matchup_history, realized_history):
		realized_map = self._realized_category_outcomes(realized_history)
		for matchup in matchup_history.get("matchups", []):
			matchup_id = str(matchup.get("matchup_id", ""))
			realized_categories = realized_map.get(matchup_id)
			if not realized_categories:
				continue
			engines = matchup.get("engines", {})
			for engine_name in ("analytic_normal", "monte_carlo"):
				engine_payload = engines.get(engine_name, {})
				category_probs = engine_payload.get("categories", {})
				for category in self.SCORE_CATEGORIES:
					prob_row = category_probs.get(category, {})
					p_away = self._safe_float(prob_row.get("away_win_prob"))
					if p_away < 0.0 or p_away > 1.0:
						continue
					y = realized_categories.get(category)
					if y is None:
						continue
					entry = {"p_away": p_away, "actual_away": y}
					engine_rows[engine_name].append(entry)
					category_source = self._safe_str(prob_row.get("category_source")) or ("appearance_summed" if category == "VIJAY" else "component_derived")
					if category_source in source_rows:
						source_rows[category_source][engine_name].append(entry)
					dominant_role = self._safe_str(prob_row.get("dominant_role"))
					if dominant_role in role_rows:
						role_rows[dominant_role][engine_name].append(entry)
					away_sp_starts = self._safe_float(prob_row.get("away_sp_expected_starts"))
					bucket = "2-start" if away_sp_starts >= float(self.clap_cfg.get("sp_two_start_threshold", 1.5)) else "1-start"
					sp_bucket_rows[bucket][engine_name].append(entry)

	def _realized_category_outcomes(self, weekly_totals_payload):
		team_by_id = {str(team.get("team_id", "")): team for team in weekly_totals_payload.get("teams", [])}
		rows = {}
		for matchup in weekly_totals_payload.get("matchups", []):
			matchup_id = str(matchup.get("matchup_id", ""))
			away_id = str(matchup.get("away_team_id", ""))
			home_id = str(matchup.get("home_team_id", ""))
			away_team = team_by_id.get(away_id, {})
			home_team = team_by_id.get(home_id, {})
			away_totals = away_team.get("category_totals", {})
			home_totals = home_team.get("category_totals", {})
			category_outcomes = {}
			for category in self.SCORE_CATEGORIES:
				away_value = self._safe_float(away_totals.get(category))
				home_value = self._safe_float(home_totals.get(category))
				if abs(away_value - home_value) <= 1e-12:
					category_outcomes[category] = 0.0
				elif category in self.LOWER_IS_BETTER:
					category_outcomes[category] = 1.0 if away_value < home_value else 0.0
				else:
					category_outcomes[category] = 1.0 if away_value > home_value else 0.0
			rows[matchup_id] = category_outcomes
		return rows

	def _engine_metric_snapshot(self, rows):
		if not rows:
			return {"samples": 0, "brier_score": None, "mae": None}
		samples = len(rows)
		brier = sum((row["p_away"] - row["actual_away"]) ** 2 for row in rows) / float(samples)
		mae = sum(abs(row["p_away"] - row["actual_away"]) for row in rows) / float(samples)
		return {"samples": int(samples), "brier_score": round(brier, 6), "mae": round(mae, 6)}

	def _recommended_engine(self, metrics):
		analytic = metrics.get("analytic_normal", {})
		mc = metrics.get("monte_carlo", {})
		analytic_brier = analytic.get("brier_score")
		mc_brier = mc.get("brier_score")
		if isinstance(analytic_brier, (int, float)) and isinstance(mc_brier, (int, float)):
			return "analytic_normal" if analytic_brier <= mc_brier else "monte_carlo"
		if isinstance(analytic_brier, (int, float)):
			return "analytic_normal"
		if isinstance(mc_brier, (int, float)):
			return "monte_carlo"
		default_engine = str(self.clap_cfg.get("selected_engine", "analytic_normal"))
		if default_engine in {"analytic_normal", "monte_carlo"}:
			return default_engine
		return "analytic_normal"

	def _projection_prior_mu(self, projection_row, category, role):
		mapping = {
			"aRBI": "aRBI_window",
			"aSB": "aSB_window",
			"MGS": "MGS_window",
			"VIJAY": "VIJAY_window",
		}
		key = mapping.get(category)
		if key is None:
			return 0.0
		value = self._safe_float(projection_row.get(key))
		if role == "sp":
			return value
		return value

	def _expected_starts_week(self, projection_row):
		role = self._player_role(projection_row)
		if role != "sp":
			return 0.0
		for key in ("projected_starts_window", "expected_starts_week", "projected_appearances_window"):
			value = projection_row.get(key)
			try:
				parsed = float(value)
				if parsed >= 0:
					return parsed
			except Exception:
				continue
		return 1.0

	def _blend_distribution(self, prior_mu, prior_sigma, observed_mu, observed_sigma, n_observed, n_stabilize, min_sigma):
		if n_observed <= 0:
			return {"mu": round(prior_mu, 6), "sigma": round(max(prior_sigma, 0.0), 6)}
		obs_weight = min(1.0, float(n_observed) / float(max(1, n_stabilize)))
		prior_weight = 1.0 - obs_weight
		blended_mu = (prior_weight * prior_mu) + (obs_weight * observed_mu)
		blended_sigma = (prior_weight * max(prior_sigma, 0.0)) + (obs_weight * max(observed_sigma, 0.0))
		if (prior_sigma > 0.0 or observed_sigma > 0.0) and blended_sigma < min_sigma:
			blended_sigma = min_sigma
		return {"mu": round(blended_mu, 6), "sigma": round(blended_sigma, 6)}

	def _sample_stats(self, values, n_label):
		clean = []
		for value in values:
			try:
				clean.append(float(value))
			except Exception:
				continue
		n = len(clean)
		if n == 0:
			return {"values": [], "mu": 0.0, "sigma": 0.0, n_label: 0}
		mu = sum(clean) / float(n)
		var = sum((value - mu) ** 2 for value in clean) / float(n)
		return {"values": [round(value, 6) for value in clean], "mu": round(mu, 6), "sigma": round(math.sqrt(max(var, 0.0)), 6), n_label: n}

	def _role_contribution_snapshot(self, team_model, category):
		role_map = {}
		for role in self.ROLE_KEYS:
			row = (team_model.get("role_categories", {}).get(role, {}) or {}).get(category, {})
			role_map[role] = {
				"mu": round(self._safe_float(row.get("mu")), 6),
				"sigma": round(self._safe_float(row.get("sigma")), 6),
			}
		return role_map

	def _dominant_role(self, role_contributions):
		scores = {}
		for role in self.ROLE_KEYS:
			away_mu = self._safe_float(((role_contributions.get("away", {}) or {}).get(role, {}) or {}).get("mu"))
			home_mu = self._safe_float(((role_contributions.get("home", {}) or {}).get(role, {}) or {}).get("mu"))
			scores[role] = abs(away_mu) + abs(home_mu)
		return max(scores.items(), key=lambda item: item[1])[0] if scores else "batters"

	def _player_role(self, projection_row):
		role = self._safe_str((projection_row or {}).get("player_role")).lower()
		if role in {"batters", "sp", "rp"}:
			return role
		return ""

	def _safe_str(self, value):
		if value is None:
			return ""
		return str(value).strip()

	def _normal_cdf(self, z_value):
		return 0.5 * (1.0 + math.erf(z_value / math.sqrt(2.0)))

	def _stable_seed(self, text):
		value = 0
		for ch in str(text):
			value = (value * 131 + ord(ch)) % 2147483647
		return value

	def _safe_float(self, value):
		try:
			return float(value)
		except Exception:
			return 0.0
