import pathlib
import sys
import unittest


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "py"))

from ingestion.fetch_cbs_data import CbsApiFetcher  # noqa: E402


class TestFetchRequestPolicy(unittest.TestCase):
	def test_retry_only_transient_statuses(self):
		fetcher = CbsApiFetcher()
		self.assertTrue(fetcher._should_retry_status(None))
		self.assertTrue(fetcher._should_retry_status(429))
		self.assertTrue(fetcher._should_retry_status(500))
		self.assertTrue(fetcher._should_retry_status(503))
		self.assertFalse(fetcher._should_retry_status(400))
		self.assertFalse(fetcher._should_retry_status(401))
		self.assertFalse(fetcher._should_retry_status(403))
		self.assertFalse(fetcher._should_retry_status(404))


if __name__ == "__main__":
	unittest.main()
