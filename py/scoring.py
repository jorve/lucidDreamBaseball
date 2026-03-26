import json
import pprint
import statistics
import csv
from project_config import CURRENT_WEEK, get_week_file_path, get_json_output_path, get_csv_output_path

pp = pprint.PrettyPrinter(indent=2)

api_token = ""
data_url = "http://api.cbssports.com/fantasy/league/scoring/live?version=3.0&league_id=luciddreambaseball&period=" + str(CURRENT_WEEK) + "0&no_players=1&access_token=" + api_token + "&response_format=json"
weekly_scores = {}
team_scores = {}
cat_means = {}
ldb_teams = []
ldb_cats = []
ldb_bad_cats = []
ldb_good_cats = []
ldb_CLAP = {}

for i in range(1, CURRENT_WEEK + 1):
	weekly_scores["week" + str(i)] = {}
	file_path = get_week_file_path(i)
	with file_path.open() as json_file:
		json_data = json.load(json_file)
		teams = json_data["body"]["live_scoring"]["teams"]
		for team in teams:
			if not team["long_abbr"] in ldb_teams:
				ldb_teams.append(team["long_abbr"])
			weekly_scores["week" + str(i)][team["long_abbr"]] = {}
			weekly_team_scores = team["categories"]
			for score in weekly_team_scores:
				if not score["name"] in ldb_cats:
					ldb_cats.append(score["name"])
					if score["is_bad"] == "true":
						ldb_bad_cats.append(score["name"])
					else:
						ldb_good_cats.append(score["name"])
				weekly_scores["week" + str(i)][team["long_abbr"]][score["name"]] = score["value"]
				if not score["name"] in cat_means:
					cat_means[score["name"]] = []
	for ldb_team in ldb_teams:
		for ldb_cat in ldb_cats:
			team_week_scores = weekly_scores["week" + str(i)].get(ldb_team, {})
			raw_value = team_week_scores.get(ldb_cat)
			if raw_value is None:
				# Upstream API occasionally omits a category for a team/week.
				# Keep the nightly pipeline moving with a neutral numeric fallback.
				print(
					f"Warning: missing category {ldb_cat!r} for team {ldb_team!r} in week {i}; using 0.0"
				)
				raw_value = 0.0
			cat_means[ldb_cat].append(float(raw_value))

list_CLAP = []
for ldb_cat in ldb_cats:
	cat_means[ldb_cat + "_mean"] = statistics.mean(cat_means[ldb_cat])
	# Early season (or partial API payloads) can yield zero variance; avoid crashing the pipeline.
	try:
		stddev = statistics.stdev(cat_means[ldb_cat])
	except statistics.StatisticsError:
		stddev = 0.0
	cat_means[ldb_cat + "_stddev"] = stddev
	ldb_CLAP[ldb_cat] = [cat_means[ldb_cat + "_mean"], stddev]
	list_CLAP.append([ldb_cat, cat_means[ldb_cat + "_mean"], stddev])
	cat_means["z" + ldb_cat] = []
	for item in cat_means[ldb_cat]:
		if stddev == 0:
			cat_means["z" + ldb_cat].append(0.0)
			continue
		if ldb_cat in ldb_good_cats:
			cat_means["z" + ldb_cat].append((item - cat_means[ldb_cat + "_mean"])/cat_means[ldb_cat + "_stddev"])
		else:
			cat_means["z" + ldb_cat].append((cat_means[ldb_cat + "_mean"] - item)/cat_means[ldb_cat + "_stddev"])


with get_csv_output_path("ldbClap.csv").open('wt', newline='') as csv_file:
	csvCLAP = csv.writer(csv_file)
	for item in list_CLAP:
		csvCLAP.writerow(item)

with get_json_output_path("weeklyScores.json").open('wt') as f:
	json.dump(weekly_scores, f, indent=2)

with get_json_output_path("ldbCLAP.json").open('wt') as f:
	json.dump(ldb_CLAP, f, indent=2)

key_variables = {
	"current_week": CURRENT_WEEK,
	"ldb_teams": ldb_teams,
	"ldb_cats": ldb_cats,
	"ldb_bad_cats": ldb_bad_cats,
	"ldb_good_cats": ldb_good_cats,
	"ldb_bat_cats": [
    "HR",
    "OBP",
    "OPS",
    "R",
    "aRBI",
    "aSB"
  ],
  "ldb_pit_cats": [
    "ERA",
    "HRA",
    "K",
    "NQW",
    "VIJAY",
    "aWHIP"
  ], 
  "WAR_adjustments": {
  	"at_bats": 25,
  	"sp_inns": 8,
  	"rp_inns": 3,
  	"off_RL": 0.29,
  	"pit_RL": 0.38
  }
}

with get_json_output_path("key_variables.json").open('wt') as f:
	json.dump(key_variables, f, indent=2)
