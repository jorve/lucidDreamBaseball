import json
import scipy
import pprint
import csv
from project_config import load_project_json, get_csv_output_path

pp = pprint.PrettyPrinter(indent=2)
ldbvars = load_project_json("key_variables.json")

ldb_xwins = {}
ldb_xins_toprint = [["Team", "Wins", "xWins", "LUCK"]]

team_CLAPS = load_project_json("team_CLAPS.json")
ldbCLAP = load_project_json("ldbCLAP.json")
team_scores = load_project_json("team_scores.json")
ldb_teams = ldbvars["ldb_teams"]
ldb_cats = ldbvars["ldb_cats"]
ldb_bad_cats = ldbvars["ldb_bad_cats"]
for ldb_team in ldb_teams:
	if not ldb_team in ldb_xwins:
		ldb_xwins[ldb_team] = {}
	ldb_xwins[ldb_team]["total_wins"] = 0
	ldb_xwins[ldb_team]["total_losses"] = 0
	for ldb_cat in ldb_cats:
		zclapcat = "zclapdiff" + ldb_cat
		pclapcat = "pclapdiff" + ldb_cat
		if not zclapcat in team_CLAPS[ldb_team]:
			team_CLAPS[ldb_team][zclapcat] = []
		if not pclapcat in team_CLAPS[ldb_team]:
			team_CLAPS[ldb_team][pclapcat] = []
		zarray = team_CLAPS[ldb_team][zclapcat]
		parray = team_CLAPS[ldb_team][pclapcat]
		score_array = team_CLAPS[ldb_team][ldb_cat + "_array"]
		cat_stddev = float(ldbCLAP[ldb_cat][1] or 0)
		for score in score_array:
			# Early season can produce zero-variance categories; use neutral z-score.
			if cat_stddev == 0:
				zarray.append(0.0)
			elif ldb_cat in ldb_bad_cats:
				zarray.append((ldbCLAP[ldb_cat][0] - score) / cat_stddev)
			else:
				zarray.append((score - ldbCLAP[ldb_cat][0]) / cat_stddev)
		for zscore in zarray:
			parray.append(scipy.stats.norm.cdf(zscore))
		if not ldb_cat in ldb_xwins:
			ldb_xwins[ldb_team][ldb_cat] = 0
		for pscore in parray:
			ldb_xwins[ldb_team][ldb_cat] += pscore
			ldb_xwins[ldb_team]["total_wins"] += pscore
			ldb_xwins[ldb_team]["total_losses"] += (1 - pscore)
	ldb_xwins[ldb_team]["LUCK"] = team_scores[ldb_team]["season_record"][0] - ldb_xwins[ldb_team]["total_wins"]
	ldb_xins_toprint.append([ldb_team, team_scores[ldb_team]["season_record"][0], ldb_xwins[ldb_team]["total_wins"], ldb_xwins[ldb_team]["LUCK"]])

with get_csv_output_path("ldb_xwins.csv").open('wt', newline='') as csv_file:
	csv_xwins = csv.writer(csv_file)
	for item in ldb_xins_toprint:
		csv_xwins.writerow(item)

