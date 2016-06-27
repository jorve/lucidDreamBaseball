# must use Python 2x to use the statistics package
import json
import pprint
import statistics
import csv

pp = pprint.PrettyPrinter(indent=2)

current_year = 2016
current_week = 12
api_token = ""
data_url = "http://api.cbssports.com/fantasy/league/scoring/live?version=3.0&league_id=luciddreambaseball&period=" + str(current_week) + "0&no_players=1&access_token=" + api_token + "&response_format=json"

team_scores = {}
team_CLAPS = {}
cat_means = {}
ldb_teams = []
ldb_cats = []
ldb_bad_cats = []
ldb_good_cats = []

inn_adjustments = {
	"AIDS": [5,-5],
	"ICHI":	[-5,5]
}

for i in range(1, current_week + 1):
	with open("./" + str(current_year) + "/week" + str(i)+ ".json") as json_file:
		json_data = json.load(json_file)
		teams = json_data["body"]["live_scoring"]["teams"]
		for team in teams:
			if not team["long_abbr"] in ldb_teams:
				ldb_teams.append(team["long_abbr"])
			if not team["long_abbr"] in team_scores:
				team_scores[team["long_abbr"]] = {}
			team_scores[team["long_abbr"]]["week" + str(i)] = {}
			team_scores[team["long_abbr"]]["week" + str(i)]["record"] = team["matchups"][0]["pts"].split('-')
			if team["home_away"] == "away":
				team_scores[team["long_abbr"]]["week" + str(i)]["record"] = [int(team_scores[team["long_abbr"]]["week" + str(i)]["record"][0]), int(team_scores[team["long_abbr"]]["week" + str(i)]["record"][1]) + int(team_scores[team["long_abbr"]]["week" + str(i)]["record"][2])]
				team_scores[team["long_abbr"]]["week" + str(i)]["away"] = True
			else:
				team_scores[team["long_abbr"]]["week" + str(i)]["record"] = [int(team_scores[team["long_abbr"]]["week" + str(i)]["record"][0]) + int(team_scores[team["long_abbr"]]["week" + str(i)]["record"][2]), int(team_scores[team["long_abbr"]]["week" + str(i)]["record"][1])]
				team_scores[team["long_abbr"]]["week" + str(i)]["away"] = False
			if not "season_record" in team_scores[team["long_abbr"]]:
				team_scores[team["long_abbr"]]["season_record"] = [0,0]
			team_scores[team["long_abbr"]]["season_record"][0] += team_scores[team["long_abbr"]]["week" + str(i)]["record"][0]
			team_scores[team["long_abbr"]]["season_record"][1] += team_scores[team["long_abbr"]]["week" + str(i)]["record"][1]
			weekly_team_scores = team["categories"]
			for score in weekly_team_scores:
				if not score["name"] in ldb_cats:
					ldb_cats.append(score["name"])
					if score["is_bad"] == "true":
						ldb_bad_cats.append(score["name"])
					else:
						ldb_good_cats.append(score["name"])
				team_scores[team["long_abbr"]]["week" + str(i)][score["name"]] = score["value"]
				if not score["name"] in cat_means:
					cat_means[score["name"]] = []

pp.pprint(team_scores)					

for ldb_team in ldb_teams:
	if ldb_team in inn_adjustments:
		team_scores[ldb_team]["season_record"][0] += inn_adjustments[ldb_team][0]
		team_scores[ldb_team]["season_record"][1] += inn_adjustments[ldb_team][1] 
	pp.pprint(team_scores[ldb_team]["season_record"])

f = open('team_scores.json', 'wt')
json.dump(team_scores, f,indent = 4)
f.close()

for team in ldb_teams:
	if not team in team_CLAPS:
		team_CLAPS[team] = {}
	team_CLAP = team_CLAPS[team]
	for ldb_cat in ldb_cats:
		if not ldb_cat in team_CLAP:
			team_CLAP[ldb_cat + "_array"] = []
		for i in range(1, current_week + 1):
			team_CLAP[ldb_cat + "_array"].append(float(team_scores[team]["week" + str(i)][ldb_cat]))

for team in ldb_teams:
	team_CLAP = team_CLAPS[team]
	for ldb_cat in ldb_cats:
		cat_mean = str(ldb_cat + "_mean")
		cat_stddev = str(ldb_cat + "_stddev")
		team_CLAP[cat_mean] = statistics.mean(team_CLAP[ldb_cat + "_array"])
		team_CLAP[cat_stddev] = statistics.stdev(team_CLAP[ldb_cat + "_array"])


f = open('team_CLAPS.json', 'wt')
json.dump(team_CLAPS, f,indent = 4)
f.close()


