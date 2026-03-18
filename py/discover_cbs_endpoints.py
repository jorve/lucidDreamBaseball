import argparse
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import sync_playwright

from project_config import get_ingestion_auth_config, get_ingestion_cbs_config, get_json_output_path


def parse_args():
	parser = argparse.ArgumentParser(
		description="Discover CBS fantasy API endpoints from browser network traffic."
	)
	parser.add_argument(
		"--headless",
		action="store_true",
		help="Run browser headless (default: false for easier first-run debugging).",
	)
	parser.add_argument(
		"--output",
		help="Optional output path (defaults to json/cbs_discovered_endpoints_<date>.json).",
	)
	parser.add_argument(
		"--max-query-keys",
		type=int,
		default=10,
		help="Max query-string keys to keep per endpoint sample.",
	)
	return parser.parse_args()


def login(page, username, password, login_url):
	page.goto(login_url, wait_until="domcontentloaded", timeout=90000)

	email_selectors = ["input[name='email']", "input[type='email']", "input[name='username']"]
	password_selectors = ["input[name='password']", "input[type='password']"]
	submit_selectors = ["button[type='submit']", "input[type='submit']"]

	email_locator = first_existing_locator(page, email_selectors)
	password_locator = first_existing_locator(page, password_selectors)
	submit_locator = first_existing_locator(page, submit_selectors)

	email_locator.fill(username)
	password_locator.fill(password)
	submit_locator.click()
	page.wait_for_timeout(5000)
	return page.url


def first_existing_locator(page, selectors):
	for selector in selectors:
		locator = page.locator(selector)
		if locator.count() > 0:
			return locator.first
	raise RuntimeError(f"Could not find any selector from: {selectors}")


def record_request(url, sink, method=None, resource_type=None):
	parsed = urlparse(url)
	host = parsed.netloc.lower()
	if "cbssports.com" not in host:
		return
	query_params = parse_qs(parsed.query)
	is_fantasy_path = "/fantasy/" in parsed.path
	is_api_path = "/api/" in parsed.path
	has_fantasy_query = "league_id" in query_params or "access_token" in query_params
	if not is_fantasy_path and not is_api_path and not has_fantasy_query:
		return

	path = parsed.path
	query_keys = sorted(query_params.keys())

	resource = sink[path]
	resource["count"] += 1
	resource["hosts"].add(host)
	resource["sample_urls"].add(url)
	if method:
		resource["methods"].add(method)
	if resource_type:
		resource["resource_types"].add(resource_type)
	for key in query_keys:
		resource["query_keys"].add(key)


def build_default_pages(cbs_cfg):
	league_id = cbs_cfg.get("league_id", "luciddreambaseball")
	return [
		f"https://{league_id}.baseball.cbssports.com/scoring/standard",
		f"https://{league_id}.baseball.cbssports.com/teams",
	]


def serialize_results(results_by_path, max_query_keys):
	serialized = []
	for path, data in sorted(results_by_path.items()):
		query_keys = sorted(data["query_keys"])[:max_query_keys]
		serialized.append(
			{
				"path": path,
				"hosts": sorted(data["hosts"]),
				"request_count": data["count"],
				"methods": sorted(data["methods"]),
				"resource_types": sorted(data["resource_types"]),
				"query_keys": query_keys,
				"sample_url": sorted(data["sample_urls"])[0] if data["sample_urls"] else None,
			}
		)
	return serialized


def extract_endpoint_hints_from_html(html_content):
	patterns = [
		r"https://api\.cbssports\.com/fantasy/[A-Za-z0-9/_\-.?=&]+",
		r"/fantasy/[A-Za-z0-9/_\-.?=&]+",
		r"/api/[A-Za-z0-9/_\-.?=&]+",
		r"league/[A-Za-z0-9/_\-.]+",
		r"stats/[A-Za-z0-9/_\-.]+",
	]
	hints = set()
	for pattern in patterns:
		for match in re.findall(pattern, html_content):
			hints.add(match)
	return sorted(hints)


def main():
	args = parse_args()
	auth_cfg = get_ingestion_auth_config()
	cbs_cfg = get_ingestion_cbs_config()

	username_env = auth_cfg.get("username_env", "CBS_USERNAME")
	password_env = auth_cfg.get("password_env", "CBS_PASSWORD")
	username = os.getenv(username_env)
	password = os.getenv(password_env)
	if not username or not password:
		raise RuntimeError(
			f"Missing credentials. Set {username_env} and {password_env} before running discovery."
		)

	login_url = cbs_cfg.get("login_url", "https://www.cbssports.com/login")
	pages_to_visit = build_default_pages(cbs_cfg)
	results_by_path = defaultdict(
		lambda: {
			"count": 0,
			"hosts": set(),
			"query_keys": set(),
			"sample_urls": set(),
			"methods": set(),
			"resource_types": set(),
		}
	)
	all_cbssports_urls = set()
	visited_results = []
	html_endpoint_hints = defaultdict(set)

	with sync_playwright() as playwright:
		browser = playwright.chromium.launch(headless=args.headless)
		context = browser.new_context()
		page = context.new_page()
		context.on(
			"request",
			lambda request: record_request(
				request.url,
				results_by_path,
				method=request.method,
				resource_type=request.resource_type,
			),
		)
		context.on(
			"response",
			lambda response: record_request(
				response.url,
				results_by_path,
				method=response.request.method,
				resource_type=response.request.resource_type,
			),
		)
		context.on(
			"request",
			lambda request: all_cbssports_urls.add(request.url)
			if "cbssports.com" in urlparse(request.url).netloc.lower()
			else None,
		)

		login_final_url = login(page, username, password, login_url)
		for page_url in pages_to_visit:
			page.goto(page_url, wait_until="domcontentloaded", timeout=90000)
			page.wait_for_timeout(8000)
			page_title = page.title()
			page_html_raw = page.content()
			page_html = page_html_raw.lower()
			for hint in extract_endpoint_hints_from_html(page_html_raw):
				html_endpoint_hints[page_url].add(hint)
			visited_results.append(
				{
					"requested_url": page_url,
					"final_url": page.url,
					"title": page_title,
					"is_sign_in_page": ("sign in" in page_title.lower()) or ("log in" in page_html),
				}
			)

		browser.close()

	discovered = serialize_results(results_by_path, args.max_query_keys)
	payload = {
		"generated_at": datetime.now(timezone.utc).isoformat(),
		"login_final_url": login_final_url,
		"visited_pages": pages_to_visit,
		"visited_results": visited_results,
		"cbssports_request_count": len(all_cbssports_urls),
		"cbssports_request_samples": sorted(all_cbssports_urls)[:25],
		"html_endpoint_hints": {url: sorted(hints)[:120] for url, hints in html_endpoint_hints.items()},
		"endpoints": discovered,
	}

	if args.output:
		output_path = args.output
	else:
		date_stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
		output_path = str(get_json_output_path(f"cbs_discovered_endpoints_{date_stamp}.json"))

	with open(output_path, "w") as outfile:
		json.dump(payload, outfile, indent=2)

	print(f"Discovered {len(discovered)} endpoints.")
	if len(discovered) == 0:
		sign_in_hits = [item for item in visited_results if item["is_sign_in_page"]]
		if sign_in_hits:
			print("No endpoints discovered because pages appear unauthenticated (Sign In detected).")
		else:
			print("No endpoints discovered; inspect discovery JSON for request samples and visited page diagnostics.")
	print(f"Wrote results to: {output_path}")
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
