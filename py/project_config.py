import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

DEFAULT_PROJECT_CONFIG = {
	"current_year": 2016,
	"current_week": 12,
	"paths": {
		"data_dir": "data",
		"json_dir": "json",
		"csv_dir": "csv",
		"ingestion_raw_dir": "data/raw",
		"ingestion_state_dir": ".state",
		"logs_dir": "logs",
	},
	"ingestion": {
		"enabled": True,
		"auth_mode": "hybrid",
		"live_scoring_include_players": False,
		"retention_days": 45,
		"health_max_age_hours": 30,
		"transaction_health_max_age_hours": 168,
		"request_policy": {
			"max_attempts": 3,
			"retry_backoff_seconds": 2.0,
			"min_interval_seconds": 1.5,
			"jitter_seconds": 0.75,
			"user_agent": "lucidDreamBaseball-ingestion/1.0 (+respectful-request-policy)",
		},
		"run_lock": {
			"enabled": True,
			"stale_hours": 8,
		},
		"history": {
			"enabled": True,
			"retention_days": 180,
		},
		"calibration": {
			"enabled": True,
			"trend_weeks": 4,
			"degrade_mae_pct": 5.0,
		},
		"clap_v2": {
			"enabled": True,
			"selected_engine": "analytic_normal",
			"monte_carlo_samples": 3000,
			"player_cv": 0.35,
			"min_sigma": 0.05,
			"random_seed": 42,
			"calibration_lookback_days": 45,
			"stabilization_samples_weekly": 6,
			"stabilization_samples_starts": 6,
			"sp_start_min_outs": 9,
			"sp_two_start_threshold": 1.5,
		},
		"transactions": {
			"enabled": True,
		},
		"recompute": {
			"trigger_enabled": True,
			"force_full_on_integrity_error": True,
		},
		"player_blend": {
			"enabled": True,
			"preseason_csvs": {
				"batters": "data/{year}/preseason/batter_priors.csv",
				"sp": "data/{year}/preseason/sp_priors.csv",
				"rp": "data/{year}/preseason/rp_priors.csv",
			},
			# Backward-compatible fallback for older single-file setups.
			"preseason_csv": "data/{year}/preseason/player_priors.csv",
			"transition_weeks": 6,
			"season_days": 183,
			"prior_variance": 25.0,
			"observed_variance": 36.0,
			"overperform_threshold": 1.0,
			"underperform_threshold": 1.0,
			"performance_thresholds_percent": {
				"overall": {"over": 10.0, "under": 10.0},
				"aRBI": {"over": 15.0, "under": 15.0},
				"aSB": {"over": 20.0, "under": 20.0},
				"MGS": {"over": 12.0, "under": 12.0},
				"VIJAY": {"over": 20.0, "under": 20.0},
			},
		},
		"projections": {
			"scoring_week_end_weekday": 6,
			"free_agents": {
				"enabled": True,
				"daily_weight": 0.3,
				"weekly_weight": 0.7,
				"max_candidates": 250,
				"drop_pool_size": 8,
				"max_replacement_suggestions": 120,
				"min_net_gain": 0.0,
			},
			"weekly_email": {
				"enabled": True,
				"top_players": 8,
				"top_swaps": 5,
				"send_day_weekday": 0,
				"send_time_local": "08:00",
				"subject_template": "LDB Weekly Recap + Outlook ({week_start} to {week_end})",
				"recipients": [],
			},
		},
		"eligibility": {
			"enabled": True,
			"csvs": {
				"batters": "data/{year}/preseason/CBS_batter_elig.csv",
				"sp": "data/{year}/preseason/CBS_SP_elig.csv",
				"rp": "data/{year}/preseason/CBS_RP_elig.csv",
			},
		},
		"optional_resources": ["player_stats", "league_lineup", "scout_team"],
		"timezone": "local",
		"cbs": {
			"league_id": "luciddreambaseball",
			"base_url": "https://api.cbssports.com/fantasy",
			"login_url": "https://www.cbssports.com/login",
			"token_source_url": "https://www.cbssports.com/fantasy/baseball/",
			"token_source_urls": [
				"https://luciddreambaseball.baseball.cbssports.com/",
				"https://www.cbssports.com/fantasy/baseball/leagues/luciddreambaseball/",
			],
			"response_format": "json",
			"version": "3.0",
			"endpoints": {
				"live_scoring": "league/scoring/live",
				"schedule": "league/schedules",
				"rosters": "league/rosters",
				"player_stats": "stats/players",
				"league_lineup": "https://luciddreambaseball.baseball.cbssports.com/api/league/transactions/lineup",
				"scout_team": "https://luciddreambaseball.baseball.cbssports.com/api/players/scout-team",
			},
		},
		"auth": {
			"username_env": "CBS_USERNAME",
			"password_env": "CBS_PASSWORD",
			"keyring_service": "lucidDreamBaseball",
			"session_key_name": "cbs_session",
			"token_key_name": "cbs_api_token",
			"headless": True,
			"timeout_seconds": 60,
			"max_session_age_hours": 72,
		},
	},
}


def _merge_project_config(default_config, user_config):
	merged = dict(default_config)
	merged_paths = dict(default_config.get("paths", {}))
	merged_ingestion = dict(default_config.get("ingestion", {}))
	merged_ingestion_cbs = dict(default_config.get("ingestion", {}).get("cbs", {}))
	merged_ingestion_auth = dict(default_config.get("ingestion", {}).get("auth", {}))
	merged_ingestion_transactions = dict(default_config.get("ingestion", {}).get("transactions", {}))
	merged_ingestion_recompute = dict(default_config.get("ingestion", {}).get("recompute", {}))
	merged_ingestion_history = dict(default_config.get("ingestion", {}).get("history", {}))
	merged_ingestion_calibration = dict(default_config.get("ingestion", {}).get("calibration", {}))
	merged_ingestion_clap_v2 = dict(default_config.get("ingestion", {}).get("clap_v2", {}))
	merged_ingestion_request_policy = dict(default_config.get("ingestion", {}).get("request_policy", {}))
	merged_ingestion_run_lock = dict(default_config.get("ingestion", {}).get("run_lock", {}))
	merged_ingestion_player_blend = dict(default_config.get("ingestion", {}).get("player_blend", {}))
	merged_ingestion_eligibility = dict(default_config.get("ingestion", {}).get("eligibility", {}))
	merged_ingestion_projections = dict(default_config.get("ingestion", {}).get("projections", {}))
	merged_paths.update(user_config.get("paths", {}))
	merged_ingestion.update(user_config.get("ingestion", {}))
	merged_ingestion_cbs.update(user_config.get("ingestion", {}).get("cbs", {}))
	merged_ingestion_auth.update(user_config.get("ingestion", {}).get("auth", {}))
	merged_ingestion_transactions.update(user_config.get("ingestion", {}).get("transactions", {}))
	merged_ingestion_recompute.update(user_config.get("ingestion", {}).get("recompute", {}))
	merged_ingestion_history.update(user_config.get("ingestion", {}).get("history", {}))
	merged_ingestion_calibration.update(user_config.get("ingestion", {}).get("calibration", {}))
	merged_ingestion_clap_v2.update(user_config.get("ingestion", {}).get("clap_v2", {}))
	merged_ingestion_request_policy.update(user_config.get("ingestion", {}).get("request_policy", {}))
	merged_ingestion_run_lock.update(user_config.get("ingestion", {}).get("run_lock", {}))
	merged_ingestion_player_blend.update(user_config.get("ingestion", {}).get("player_blend", {}))
	merged_ingestion_eligibility.update(user_config.get("ingestion", {}).get("eligibility", {}))
	merged_ingestion_projections.update(user_config.get("ingestion", {}).get("projections", {}))
	merged_ingestion["cbs"] = merged_ingestion_cbs
	merged_ingestion["auth"] = merged_ingestion_auth
	merged_ingestion["transactions"] = merged_ingestion_transactions
	merged_ingestion["recompute"] = merged_ingestion_recompute
	merged_ingestion["history"] = merged_ingestion_history
	merged_ingestion["calibration"] = merged_ingestion_calibration
	merged_ingestion["clap_v2"] = merged_ingestion_clap_v2
	merged_ingestion["request_policy"] = merged_ingestion_request_policy
	merged_ingestion["run_lock"] = merged_ingestion_run_lock
	merged_ingestion["player_blend"] = merged_ingestion_player_blend
	merged_ingestion["eligibility"] = merged_ingestion_eligibility
	merged_ingestion["projections"] = merged_ingestion_projections
	merged.update(user_config)
	merged["paths"] = merged_paths
	merged["ingestion"] = merged_ingestion
	return merged


def load_project_config():
	config_path = BASE_DIR / "project_config.json"
	if not config_path.exists():
		return DEFAULT_PROJECT_CONFIG
	with config_path.open() as infile:
		user_config = json.load(infile)
	return _merge_project_config(DEFAULT_PROJECT_CONFIG, user_config)


PROJECT_CONFIG = load_project_config()
CURRENT_YEAR = int(PROJECT_CONFIG["current_year"])
CURRENT_WEEK = int(PROJECT_CONFIG["current_week"])
PATHS = PROJECT_CONFIG["paths"]
INGESTION_CONFIG = PROJECT_CONFIG.get("ingestion", {})


def resolve_existing_path(path_candidates, not_found_message):
	for candidate in path_candidates:
		if candidate.exists():
			return candidate
	raise FileNotFoundError(not_found_message)


def get_data_year_dir(year=None):
	target_year = year if year is not None else CURRENT_YEAR
	return BASE_DIR / PATHS["data_dir"] / str(target_year)


def _ensure_dir(path_value):
	path_value.mkdir(parents=True, exist_ok=True)
	return path_value


def get_json_output_dir():
	return _ensure_dir(BASE_DIR / PATHS["json_dir"])


def get_csv_output_dir():
	return _ensure_dir(BASE_DIR / PATHS["csv_dir"])


def get_json_output_path(filename):
	return get_json_output_dir() / filename


def get_csv_output_path(filename):
	return get_csv_output_dir() / filename


def get_logs_dir():
	return _ensure_dir(BASE_DIR / PATHS.get("logs_dir", "logs"))


def get_ingestion_raw_dir(date_value=None):
	root_dir = _ensure_dir(BASE_DIR / PATHS.get("ingestion_raw_dir", "data/raw"))
	if date_value is None:
		return root_dir
	return _ensure_dir(root_dir / date_value.strftime("%Y-%m-%d"))


def get_ingestion_state_dir():
	return _ensure_dir(BASE_DIR / PATHS.get("ingestion_state_dir", ".state"))


def get_ingestion_auth_cache_path():
	return get_ingestion_state_dir() / "ingestion_auth_cache.json"


def get_ingestion_run_log_path(date_value):
	return get_logs_dir() / f"ingestion_{date_value.strftime('%Y-%m-%d')}.log"


def get_transactions_latest_path():
	return get_json_output_path("transactions_latest.json")


def get_transactions_quarantine_latest_path():
	return get_json_output_path("transactions_quarantine_latest.json")


def get_roster_state_latest_path():
	return get_json_output_path("roster_state_latest.json")


def get_roster_state_diagnostics_latest_path():
	return get_json_output_path("roster_state_diagnostics_latest.json")


def get_recompute_request_latest_path():
	return get_json_output_path("recompute_request_latest.json")


def get_ingestion_status_latest_path():
	return get_json_output_path("ingestion_status_latest.json")


def get_preseason_player_priors_path():
	return get_json_output_path("preseason_player_priors.json")


def get_player_projection_deltas_latest_path():
	return get_json_output_path("player_projection_deltas_latest.json")


def get_player_projection_daily_latest_path():
	return get_json_output_path("player_projection_daily_latest.json")


def get_player_projection_weekly_latest_path():
	return get_json_output_path("player_projection_weekly_latest.json")


def get_view_league_daily_latest_path():
	return get_json_output_path("view_league_daily_latest.json")


def get_view_league_weekly_latest_path():
	return get_json_output_path("view_league_weekly_latest.json")


def get_view_gm_daily_latest_path():
	return get_json_output_path("view_gm_daily_latest.json")


def get_view_gm_weekly_latest_path():
	return get_json_output_path("view_gm_weekly_latest.json")


def get_free_agent_candidates_latest_path():
	return get_json_output_path("free_agent_candidates_latest.json")


def get_weekly_digest_latest_path():
	return get_json_output_path("weekly_digest_latest.json")


def get_weekly_digest_latest_text_path():
	return get_json_output_path("weekly_digest_latest.txt")


def get_weekly_email_payload_latest_path():
	return get_json_output_path("weekly_email_payload_latest.json")


def get_weekly_email_text_latest_path():
	return get_json_output_path("weekly_email_latest.txt")


def get_artifact_history_latest_path():
	return get_json_output_path("artifact_history_latest.json")


def get_weekly_calibration_latest_path():
	return get_json_output_path("weekly_calibration_latest.json")


def get_player_eligibility_latest_path():
	return get_json_output_path("player_eligibility_latest.json")


def get_player_eligibility_changes_latest_path():
	return get_json_output_path("player_eligibility_changes_latest.json")


def get_team_weekly_totals_latest_path():
	return get_json_output_path("team_weekly_totals_latest.json")


def get_team_weekly_totals_state_path():
	return get_json_output_path("team_weekly_totals_state.json")


def get_clap_v2_latest_path():
	return get_json_output_path("clap_v2_latest.json")


def get_clap_player_history_latest_path():
	return get_json_output_path("clap_player_history_latest.json")


def get_matchup_expectations_latest_path():
	return get_json_output_path("matchup_expectations_latest.json")


def get_clap_calibration_latest_path():
	return get_json_output_path("clap_calibration_latest.json")


def get_ingestion_config():
	return INGESTION_CONFIG


def get_ingestion_cbs_config():
	return INGESTION_CONFIG.get("cbs", {})


def get_ingestion_auth_config():
	return INGESTION_CONFIG.get("auth", {})


def get_required_input_path(filename):
	return resolve_existing_path(
		[
			BASE_DIR / PATHS["json_dir"] / filename,
			BASE_DIR / filename,
		],
		f"Missing required input: {filename}",
	)


def get_week_file_path(week, year=None):
	target_year = year if year is not None else CURRENT_YEAR
	return resolve_existing_path(
		[
			get_data_year_dir(target_year) / f"week{week}.json",
			BASE_DIR / str(target_year) / f"week{week}.json",
		],
		f"Could not find weekly scoring file for week {week}.",
	)


def get_schedule_path(year=None):
	target_year = year if year is not None else CURRENT_YEAR
	return resolve_existing_path(
		[
			get_data_year_dir(target_year) / "schedule.json",
			BASE_DIR / PATHS["json_dir"] / "schedule.json",
			BASE_DIR / str(target_year) / "schedule.json",
		],
		"Could not find schedule.json in known project locations.",
	)


def get_batters_source_path(filename):
	return resolve_existing_path(
		[
			get_data_year_dir(CURRENT_YEAR) / filename,
			BASE_DIR / str(CURRENT_YEAR) / filename,
		],
		f"Could not find {filename} in known project locations.",
	)


def load_project_json(filename):
	json_path = resolve_existing_path(
		[
			BASE_DIR / PATHS["json_dir"] / filename,
			BASE_DIR / filename,
		],
		f"Could not find {filename} in known project locations.",
	)
	with json_path.open() as infile:
		return json.load(infile)
