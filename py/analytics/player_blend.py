from datetime import datetime, timezone

from analytics.io import read_json, write_json
from project_config import (
	CURRENT_WEEK,
	get_ingestion_config,
	get_ingestion_raw_dir,
	get_player_projection_deltas_latest_path,
	get_preseason_player_priors_path,
)


UTC = timezone.utc


class PlayerBlendError(RuntimeError):
	pass


class PlayerBlendBuilder:
	def __init__(self):
		self.ingestion_cfg = get_ingestion_config()
		self.player_blend_cfg = self.ingestion_cfg.get("player_blend", {})

	def build(self, target_date, dry_run=False):
		output_path = get_player_projection_deltas_latest_path()
		if not self.player_blend_cfg.get("enabled", True):
			return {"status": "skipped", "reason": "PLAYER_BLEND_DISABLED", "output_path": output_path}

		priors_path = get_preseason_player_priors_path()
		if not priors_path.exists():
			return {"status": "skipped", "reason": "PRESEASON_PRIORS_MISSING", "output_path": output_path}
		priors_payload = read_json(priors_path)
		priors = priors_payload.get("players", [])
		if not priors:
			return {"status": "skipped", "reason": "PRESEASON_PRIORS_EMPTY", "output_path": output_path}

		observed_by_player = self._load_observed_points(target_date)
		transition_weeks = max(1, int(self.player_blend_cfg.get("transition_weeks", 6)))
		effective_weeks = min(max(int(CURRENT_WEEK), 1), transition_weeks)
		prior_variance = float(self.player_blend_cfg.get("prior_variance", 25.0))
		observed_variance = float(self.player_blend_cfg.get("observed_variance", 36.0))
		overperform_threshold = float(self.player_blend_cfg.get("overperform_threshold", 1.0))
		underperform_threshold = float(self.player_blend_cfg.get("underperform_threshold", 1.0))
		thresholds_pct = self._load_thresholds_percent()

		entries = []
		for prior in priors:
			player_id = str(prior["player_id"])
			prior_projection = float(prior["prior_projection"])
			observed_profile = observed_by_player.get(player_id, {})
			observed_points = observed_profile.get("points")
			if observed_points is None:
				prior_precision = 1.0 / prior_variance
				observed_precision = 0.0
				blended_projection = prior_projection
				performance_delta = None
			else:
				prior_precision = 1.0 / prior_variance
				observed_precision = float(effective_weeks) / observed_variance
				total_precision = prior_precision + observed_precision
				blended_projection = (
					(prior_projection * prior_precision) + (float(observed_points) * observed_precision)
				) / total_precision
				performance_delta = float(observed_points) - blended_projection
			total_precision = prior_precision + observed_precision
			prior_weight = prior_precision / total_precision if total_precision > 0 else 1.0
			observed_weight = observed_precision / total_precision if total_precision > 0 else 0.0
			performance_delta_pct = self._percent_delta(observed_points, blended_projection)
			performance_flag = self._performance_flag(
				performance_delta=performance_delta,
				performance_delta_pct=performance_delta_pct,
				overperform_threshold=overperform_threshold,
				underperform_threshold=underperform_threshold,
				thresholds_pct=thresholds_pct,
			)
			category_signals = self._category_signals(prior, observed_profile, thresholds_pct)
			entries.append(
				{
					"player_id": player_id,
					"player_name": prior.get("player_name", f"UNKNOWN_{player_id}"),
					"prior_projection": round(prior_projection, 4),
					"observed_points": None if observed_points is None else round(float(observed_points), 4),
					"blended_projection": round(blended_projection, 4),
					"prior_weight": round(prior_weight, 6),
					"observed_weight": round(observed_weight, 6),
					"performance_delta": None if performance_delta is None else round(performance_delta, 4),
					"performance_delta_pct": None if performance_delta_pct is None else round(performance_delta_pct, 4),
					"performance_flag": performance_flag,
					"category_delta_pct": category_signals["category_delta_pct"],
					"category_performance_flags": category_signals["category_performance_flags"],
				}
			)

		with_observed = [entry for entry in entries if entry["performance_delta"] is not None]
		overperformers = sorted(with_observed, key=lambda entry: entry["performance_delta"], reverse=True)[:20]
		underperformers = sorted(with_observed, key=lambda entry: entry["performance_delta"])[:20]

		now_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")
		payload = {
			"schema_version": "1.0",
			"generated_at_utc": now_utc,
			"target_date": target_date.strftime("%Y-%m-%d"),
			"model": {
				"type": "bayesian_shrinkage",
				"transition_weeks": transition_weeks,
				"effective_weeks": effective_weeks,
				"prior_variance": prior_variance,
				"observed_variance": observed_variance,
				"overperform_threshold": overperform_threshold,
				"underperform_threshold": underperform_threshold,
				"performance_thresholds_percent": thresholds_pct,
			},
			"summary": {
				"players_with_priors": len(entries),
				"players_with_observed": len(with_observed),
				"players_without_observed": len(entries) - len(with_observed),
				"overperforming_count": len([entry for entry in entries if entry["performance_flag"] == "overperforming"]),
				"underperforming_count": len([entry for entry in entries if entry["performance_flag"] == "underperforming"]),
			},
			"top_overperformers": overperformers,
			"top_underperformers": underperformers,
			"players": entries,
		}
		if not dry_run:
			write_json(output_path, payload)
		return {"status": "ok", "output_path": output_path, "summary": payload["summary"]}

	def _performance_flag(self, performance_delta, performance_delta_pct, overperform_threshold, underperform_threshold, thresholds_pct):
		overall_threshold = thresholds_pct.get("overall", {})
		if performance_delta_pct is not None:
			return self._classify_percent_delta(performance_delta_pct, overall_threshold)
		if performance_delta is None:
			return "insufficient_data"
		if performance_delta >= overperform_threshold:
			return "overperforming"
		if performance_delta <= (-1.0 * underperform_threshold):
			return "underperforming"
		return "on_track"

	def _load_observed_points(self, target_date):
		raw_dir = get_ingestion_raw_dir(target_date)
		roster_path = raw_dir / f"rosters_{target_date.strftime('%Y-%m-%d')}.json"
		if not roster_path.exists():
			return {}
		payload = read_json(roster_path)
		try:
			teams = payload["body"]["rosters"]["teams"]
		except Exception:
			return {}

		observed = {}
		for team in teams:
			for player in team.get("players", []):
				player_id = player.get("id")
				if player_id is None:
					continue
				player_key = str(player_id)
				profile = observed.setdefault(player_key, {"points": None})
				ytd_points = player.get("ytd_points")
				try:
					profile["points"] = float(ytd_points)
				except Exception:
					pass
				profile.update(self._extract_observed_category_rates(player))
		return observed

	def _extract_observed_category_rates(self, player):
		def to_float(keys):
			for key in keys:
				value = player.get(key)
				if value is None:
					continue
				try:
					return float(value)
				except Exception:
					continue
			return None

		g = to_float(["G", "games", "appearances", "APP"])
		rbi = to_float(["RBI"])
		gdp = to_float(["GDP", "GIDP"])
		sb = to_float(["SB"])
		cs = to_float(["CS"])
		ip = to_float(["IP", "INNs"])
		gs = to_float(["GS"])
		so = to_float(["SO", "K"])
		bb = to_float(["BB"])
		h = to_float(["H"])
		r = to_float(["R"])
		hr = to_float(["HR"])
		sv = to_float(["SV", "S"]) or 0.0
		hold = to_float(["HLD", "HD"]) or 0.0
		bs = to_float(["BS"]) or 0.0
		rl = to_float(["L"]) or 0.0

		result = {}
		if g is not None and g > 0 and None not in (rbi, gdp, sb, cs):
			result["aRBI_per_app"] = (rbi - gdp) / g
			result["aSB_per_app"] = (sb - (0.5 * cs)) / g
		if g is not None and g > 0 and None not in (ip, so, bb, h, r, hr):
			outs_per_app = (3.0 * ip) / g
			result["MGS_per_app"] = 40.0 + (2.0 * outs_per_app) + (1.0 * (so / g)) - (2.0 * (bb / g)) - (2.0 * (h / g)) - (3.0 * (r / g)) - (6.0 * (hr / g))
			gs = gs or 0.0
			inn_per_gs = (ip / gs) if gs > 0 else 0.0
			relief_innings = ip - (inn_per_gs * gs)
			vijay_total = (((relief_innings) + (3.0 * sv) + (3.0 * hold)) / 4.0) - ((bs + rl) * 2.0)
			relief_apps = max((g - gs), 1.0)
			result["VIJAY_per_app"] = vijay_total / relief_apps
		return result

	def _load_thresholds_percent(self):
		config = self.player_blend_cfg.get("performance_thresholds_percent", {})
		defaults = {
			"overall": {"over": 10.0, "under": 10.0},
			"aRBI": {"over": 15.0, "under": 15.0},
			"aSB": {"over": 20.0, "under": 20.0},
			"MGS": {"over": 12.0, "under": 12.0},
			"VIJAY": {"over": 20.0, "under": 20.0},
		}
		merged = {}
		for category, category_defaults in defaults.items():
			candidate = config.get(category, {}) if isinstance(config, dict) else {}
			over = candidate.get("over", category_defaults["over"]) if isinstance(candidate, dict) else category_defaults["over"]
			under = candidate.get("under", category_defaults["under"]) if isinstance(candidate, dict) else category_defaults["under"]
			merged[category] = {"over": float(over), "under": float(under)}
		return merged

	def _percent_delta(self, observed_value, baseline_value):
		if observed_value is None:
			return None
		try:
			baseline = float(baseline_value)
			observed = float(observed_value)
		except Exception:
			return None
		if abs(baseline) <= 1e-9:
			return None
		return ((observed - baseline) / abs(baseline)) * 100.0

	def _classify_percent_delta(self, percent_delta, threshold):
		if percent_delta is None:
			return "insufficient_data"
		over = float(threshold.get("over", 10.0))
		under = float(threshold.get("under", 10.0))
		if percent_delta >= over:
			return "overperforming"
		if percent_delta <= (-1.0 * under):
			return "underperforming"
		return "on_track"

	def _category_signals(self, prior, observed_profile, thresholds_pct):
		pairs = {
			"aRBI": ("aRBI_per_app", "aRBI_per_app"),
			"aSB": ("aSB_per_app", "aSB_per_app"),
			"MGS": ("MGS_per_app", "MGS_per_app"),
			"VIJAY": ("VIJAY_per_app", "VIJAY_per_app"),
		}
		deltas = {}
		flags = {}
		for category, (prior_key, observed_key) in pairs.items():
			prior_rate = prior.get(prior_key)
			observed_rate = observed_profile.get(observed_key)
			percent_delta = self._percent_delta(observed_rate, prior_rate)
			deltas[category] = None if percent_delta is None else round(percent_delta, 4)
			flags[category] = self._classify_percent_delta(percent_delta, thresholds_pct.get(category, {}))
		return {"category_delta_pct": deltas, "category_performance_flags": flags}
