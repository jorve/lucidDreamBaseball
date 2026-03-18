import pathlib
import sys
import unittest
from datetime import datetime
from unittest.mock import patch


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from ingestion.fetch_cbs_data import CbsApiFetcher, FetchError  # noqa: E402


class _DummyAuthSession:
	api_token = "token"

	def as_request_headers(self):
		return {}


class TestScheduleExpansion(unittest.TestCase):
	@patch("ingestion.fetch_cbs_data.get_ingestion_config")
	@patch("ingestion.fetch_cbs_data.get_ingestion_cbs_config")
	def test_schedule_expands_to_all_expected_periods(self, mock_cbs_cfg, mock_ingestion_cfg):
		mock_ingestion_cfg.return_value = {
			"optional_resources": ["player_stats"],
			"schedule_expected_periods": 3,
			"request_policy": {},
		}
		mock_cbs_cfg.return_value = {
			"league_id": "luciddreambaseball",
			"base_url": "https://api.cbssports.com/fantasy",
			"response_format": "json",
			"version": "3.0",
			"endpoints": {"schedule": "league/schedules"},
		}
		fetcher = CbsApiFetcher()
		auth_session = _DummyAuthSession()
		base_payload = {"body": {"schedule": {"periods": [{"id": "1", "label": "Period 1"}]}}}

		def fake_request_resource(resource_name, endpoint, target_date, auth_session, additional_params=None):
			period_value = (additional_params or {}).get("period")
			if period_value == "2":
				return {"body": {"schedule": {"periods": [{"id": "2", "label": "Period 2"}]}}}
			if period_value == "3":
				return {"body": {"schedule": {"periods": [{"id": "3", "label": "Period 3"}]}}}
			raise FetchError("not found")

		with patch.object(fetcher, "_request_resource", side_effect=fake_request_resource):
			expanded = fetcher._expand_schedule_for_all_periods(
				base_payload=base_payload,
				endpoint="league/schedules",
				target_date=datetime.now(),
				auth_session=auth_session,
			)

		period_ids = [str(p.get("id")) for p in expanded["body"]["schedule"]["periods"]]
		self.assertEqual(period_ids, ["1", "2", "3"])


if __name__ == "__main__":
	unittest.main()
