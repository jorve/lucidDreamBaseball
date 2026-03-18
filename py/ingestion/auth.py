import json
import os
import re
from urllib.parse import parse_qs, urlparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from project_config import (
	get_ingestion_auth_cache_path,
	get_ingestion_auth_config,
	get_ingestion_cbs_config,
)

try:
	import keyring
except ImportError:
	keyring = None

try:
	from playwright.sync_api import sync_playwright
except ImportError:
	sync_playwright = None


UTC = timezone.utc


@dataclass
class AuthSession:
	cookie_header: str
	api_token: Optional[str]
	updated_at: datetime

	def as_request_headers(self):
		headers = {}
		if self.cookie_header:
			headers["Cookie"] = self.cookie_header
		if self.api_token:
			headers["Authorization"] = f"Bearer {self.api_token}"
		return headers


class AuthError(RuntimeError):
	pass


class AuthManager:
	def __init__(self):
		self.auth_cfg = get_ingestion_auth_config()
		self.cbs_cfg = get_ingestion_cbs_config()
		self.cache_path = get_ingestion_auth_cache_path()
		self.service_name = self.auth_cfg.get("keyring_service", "lucidDreamBaseball")
		self.session_key_name = self.auth_cfg.get("session_key_name", "cbs_session")
		self.token_key_name = self.auth_cfg.get("token_key_name", "cbs_api_token")
		self.username_env = self.auth_cfg.get("username_env", "CBS_USERNAME")
		self.password_env = self.auth_cfg.get("password_env", "CBS_PASSWORD")
		self.max_session_age_hours = int(self.auth_cfg.get("max_session_age_hours", 72))

	def get_session(self, force_refresh=False, dry_run=False, skip_auth=False):
		if dry_run:
			now = datetime.now(UTC)
			return AuthSession(cookie_header="dry_run_cookie=1", api_token="dry_run_token", updated_at=now)

		if skip_auth:
			cached = self._load_cached_session()
			if cached:
				return cached
			raise AuthError("skip-auth requested but no cached auth session available.")

		if not force_refresh:
			cached = self._load_cached_session()
			if cached and not self._is_expired(cached.updated_at):
				return cached

		refreshed = self._refresh_hybrid_session()
		self._save_session(refreshed)
		return refreshed

	def _refresh_hybrid_session(self):
		if sync_playwright is None:
			raise AuthError(
				"Playwright is not installed. Install dependencies and run "
				"`python -m playwright install chromium`."
			)

		username = os.getenv(self.username_env)
		password = os.getenv(self.password_env)
		if not username or not password:
			raise AuthError(
				f"Missing credentials. Set {self.username_env} and {self.password_env} environment variables."
			)

		login_url = self.cbs_cfg.get("login_url", "https://www.cbssports.com/login")
		token_source_urls = self._build_token_source_urls()
		headless = bool(self.auth_cfg.get("headless", True))
		timeout_seconds = int(self.auth_cfg.get("timeout_seconds", 60))
		api_token = os.getenv("CBS_API_TOKEN")

		with sync_playwright() as playwright:
			browser = playwright.chromium.launch(headless=headless)
			context = browser.new_context()
			page = context.new_page()
			request_urls = []
			page.on("request", lambda request: request_urls.append(request.url))
			page.goto(login_url, wait_until="domcontentloaded", timeout=timeout_seconds * 1000)
			self._fill_first(page, ["input[name='email']", "input[type='email']", "input[name='username']"], username)
			self._fill_first(page, ["input[name='password']", "input[type='password']"], password)
			self._click_first(page, ["button[type='submit']", "input[type='submit']"])
			page.wait_for_timeout(5000)

			cookie_header = self._cookie_header_from_context(context)
			local_storage_token = self._extract_local_storage_token(context)
			if local_storage_token:
				api_token = local_storage_token
			for token_source_url in token_source_urls:
				if api_token:
					break
				try:
					page.goto(token_source_url, wait_until="domcontentloaded", timeout=timeout_seconds * 1000)
					page.wait_for_timeout(3000)
				except Exception:
					continue
				api_token = self._extract_token_from_page_content(page.content())
				if not api_token:
					api_token = self._extract_token_from_page_runtime(page)
			if not api_token:
				api_token = self._extract_token_from_urls(request_urls)

			browser.close()

		now = datetime.now(UTC)
		if not cookie_header:
			raise AuthError("Browser login did not produce session cookies.")
		return AuthSession(cookie_header=cookie_header, api_token=api_token, updated_at=now)

	def _fill_first(self, page, selectors, value):
		for selector in selectors:
			locator = page.locator(selector)
			if locator.count() > 0:
				locator.first.fill(value)
				return
		raise AuthError(f"Could not find input field for selectors: {selectors}")

	def _click_first(self, page, selectors):
		for selector in selectors:
			locator = page.locator(selector)
			if locator.count() > 0:
				locator.first.click()
				return
		raise AuthError(f"Could not find clickable submit element for selectors: {selectors}")

	def _cookie_header_from_context(self, context):
		cookies = context.cookies()
		pairs = [f"{cookie['name']}={cookie['value']}" for cookie in cookies if cookie.get("name")]
		return "; ".join(pairs)

	def _extract_local_storage_token(self, context):
		token_keys = self.auth_cfg.get("token_local_storage_keys", ["api.Token", "apiToken", "access_token"])
		try:
			storage_state = context.storage_state()
		except Exception:
			return None

		for origin in storage_state.get("origins", []):
			for item in origin.get("localStorage", []):
				if item.get("name") in token_keys and item.get("value"):
					return item.get("value")
		return None

	def _extract_token_from_page_content(self, html_content):
		token_patterns = [
			r'api\.Token["\']?\s*[:=]\s*["\']([^"\']+)["\']',
			r'["\']api\.Token["\']\s*[:=]\s*["\']([^"\']+)["\']',
			r'apiToken["\']?\s*[:=]\s*["\']([^"\']+)["\']',
			r'"access_token"\s*:\s*"([^"]+)"',
			r'["\']access_token["\']\s*[:=]\s*["\']([^"\']+)["\']',
			r"access_token=([A-Za-z0-9._-]+)",
		]
		for pattern in token_patterns:
			match = re.search(pattern, html_content)
			if match:
				return match.group(1)
		return None

	def _extract_token_from_urls(self, request_urls):
		for request_url in request_urls:
			parsed = urlparse(request_url)
			query_params = parse_qs(parsed.query)
			if "access_token" in query_params and len(query_params["access_token"]) > 0:
				token_value = query_params["access_token"][0]
				if token_value:
					return token_value
		return None

	def _build_token_source_urls(self):
		league_id = self.cbs_cfg.get("league_id", "luciddreambaseball")
		configured_urls = self.cbs_cfg.get("token_source_urls", [])
		default_url = self.cbs_cfg.get("token_source_url")

		candidate_urls = [
			"https://www.cbssports.com/fantasy/baseball/",
			f"https://{league_id}.baseball.cbssports.com/",
			f"https://www.cbssports.com/fantasy/baseball/leagues/{league_id}/",
			f"https://www.cbssports.com/fantasy/baseball/league/{league_id}/",
		]
		if default_url:
			candidate_urls.insert(0, default_url)
		candidate_urls = configured_urls + candidate_urls

		normalized = []
		seen = set()
		for item in candidate_urls:
			if not item:
				continue
			url = item.strip()
			if url and url not in seen:
				normalized.append(url)
				seen.add(url)
		return normalized

	def _extract_token_from_page_runtime(self, page):
		js_expressions = [
			"window.api && window.api.Token",
			"window.api && window.api.token",
			"window.CBS && window.CBS.api && window.CBS.api.Token",
			"window.__INITIAL_STATE__ && window.__INITIAL_STATE__.api && window.__INITIAL_STATE__.api.Token",
			"window.__PRELOADED_STATE__ && window.__PRELOADED_STATE__.api && window.__PRELOADED_STATE__.api.Token",
			"window.localStorage && window.localStorage.getItem('api.Token')",
			"window.localStorage && window.localStorage.getItem('access_token')",
			"window.sessionStorage && window.sessionStorage.getItem('api.Token')",
			"window.sessionStorage && window.sessionStorage.getItem('access_token')",
		]

		for expression in js_expressions:
			value = self._safe_page_eval(page, expression)
			if value and isinstance(value, str):
				return value
		return None

	def _safe_page_eval(self, page, expression, retries=3):
		for _ in range(retries):
			try:
				return page.evaluate(expression)
			except Exception as error:
				error_text = str(error)
				if "Execution context was destroyed" in error_text:
					page.wait_for_timeout(750)
					continue
				return None
		return None

	def _is_expired(self, updated_at):
		return datetime.now(UTC) - updated_at > timedelta(hours=self.max_session_age_hours)

	def _load_cached_session(self):
		if not self.cache_path.exists():
			return None

		with self.cache_path.open() as infile:
			cache_payload = json.load(infile)

		updated_at_raw = cache_payload.get("updated_at")
		if not updated_at_raw:
			return None

		updated_at = datetime.fromisoformat(updated_at_raw)
		cookie_header = self._load_secret(self.session_key_name) or cache_payload.get("cookie_header")
		api_token = self._load_secret(self.token_key_name) or cache_payload.get("api_token")
		if not cookie_header:
			return None
		return AuthSession(cookie_header=cookie_header, api_token=api_token, updated_at=updated_at)

	def _save_session(self, session):
		self._save_secret(self.session_key_name, session.cookie_header)
		if session.api_token:
			self._save_secret(self.token_key_name, session.api_token)

		cache_payload = {
			"updated_at": session.updated_at.isoformat(),
			"cookie_header": None if keyring is not None else session.cookie_header,
			"api_token": None if keyring is not None else session.api_token,
		}
		with self.cache_path.open("w") as outfile:
			json.dump(cache_payload, outfile, indent=2)

	def _save_secret(self, key_name, value):
		if not value:
			return
		if keyring is None:
			return
		try:
			keyring.set_password(self.service_name, key_name, value)
		except Exception:
			# Fall back to cache-file-only mode if keyring backend is unavailable.
			return

	def _load_secret(self, key_name):
		if keyring is None:
			return None
		try:
			return keyring.get_password(self.service_name, key_name)
		except Exception:
			return None
