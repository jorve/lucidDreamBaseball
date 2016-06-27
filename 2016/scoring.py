import json
import pprint
import statistics
import csv

pp = pprint.PrettyPrinter(indent=2)

current_year = 2016
current_week = 12
api_token = ""
data_url = "http://api.cbssports.com/fantasy/league/scoring/live?version=3.0&league_id=luciddreambaseball&period=" + str(current_week) + "0&no_players=1&access_token=" + api_token + "&response_format=json"
weekly_scores = {}
team_scores = {}
cat_means = {}
ldb_teams = []
ldb_cats = []
ldb_bad_cats = []
ldb_good_cats = []
ldb_CLAP = {}

for i in range(1, current_week + 1):
	weekly_scores[str(current"week" + str(i)] = {}
	file_path = os.path.dirname(__file__) + "/" + str(current_year) + "/week" + str(i)+ ".json"
	with open(file_path) as json_file:
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
			cat_means[ldb_cat].append(float(weekly_scores["week" + str(i)][ldb_team][ldb_cat]))

list_CLAP = []
for ldb_cat in ldb_cats:
	cat_means[ldb_cat + "_mean"] = statistics.mean(cat_means[ldb_cat])
	cat_means[ldb_cat + "_stddev"] = statistics.stdev(cat_means[ldb_cat])
	ldb_CLAP[ldb_cat] = [statistics.mean(cat_means[ldb_cat]), statistics.stdev(cat_means[ldb_cat])]
	list_CLAP.append([ldb_cat, statistics.mean(cat_means[ldb_cat]), statistics.stdev(cat_means[ldb_cat])])
	cat_means["z" + ldb_cat] = []
	for item in cat_means[ldb_cat]:
		if ldb_cat in ldb_good_cats:
			cat_means["z" + ldb_cat].append((item - cat_means[ldb_cat + "_mean"])/cat_means[ldb_cat + "_stddev"])
		else:
			cat_means["z" + ldb_cat].append((cat_means[ldb_cat + "_mean"] - item)/cat_means[ldb_cat + "_stddev"])


csv_file = open('ldbClap.csv', 'wt')
csvCLAP = csv.writer(csv_file)
for item in list_CLAP:
	csvCLAP.writerow(item)
csv_file.close()

f = open('weeklyScores.json', 'wt')
json.dump(weekly_scores, f, indent = 2)
f.close()

f = open('ldbCLAP.json', 'wt')
json.dump(ldb_CLAP, f, indent = 2)
f.close()

key_variables = {
	"current_week": current_week,
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
  ]
}

f = open('key_variables.json', 'wt')
json.dump(key_variables, f, indent = 2)
f.close()
