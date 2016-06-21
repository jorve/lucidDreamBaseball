import json
import scipy
from scipy import stats
import pprint
import csv

pp = pprint.PrettyPrinter(indent=2)

ldbvarfile = open('key_variables.json')
ldbvars = json.load(ldbvarfile)

ldb_xwins = {}
ldb_xins_toprint = [["Team", "Wins", "xWins", "LUCK"]]

with open('team_CLAPS.json') as json_file1, open('ldbClap.json') as json_file2, open('team_scores.json') as json_file3:
	team_CLAPS = json.load(json_file1)
	ldbCLAP = json.load(json_file2)
	team_scores = json.load(json_file3)
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
			for score in score_array:
				if ldb_cat in ldb_bad_cats:	
					zarray.append((ldbCLAP[ldb_cat][0] - score)/ldbCLAP[ldb_cat][1])
				else:
					zarray.append((score - ldbCLAP[ldb_cat][0])/ldbCLAP[ldb_cat][1])
			for zscore in zarray:
				parray.append(scipy.stats.norm.cdf(zscore))
			if not ldb_cat in ldb_xwins:
				ldb_xwins[ldb_team][ldb_cat] = 0
			for pscore in parray:
				ldb_xwins[ldb_team][ldb_cat] += pscore
				ldb_xwins[ldb_team]["total_wins"] += pscore
				ldb_xwins[ldb_team]["total_losses"] += (1-pscore)
		ldb_xwins[ldb_team]["LUCK"] = team_scores[ldb_team]["season_record"][0] - ldb_xwins[ldb_team]["total_wins"]
		ldb_xins_toprint.append([ldb_team, team_scores[ldb_team]["season_record"][0], ldb_xwins[ldb_team]["total_wins"], ldb_xwins[ldb_team]["LUCK"]])

csv_file = open('ldb_xwins.csv', 'wt')
csv_xwins = csv.writer(csv_file)
for item in ldb_xins_toprint:
	csv_xwins.writerow(item)
csv_file.close()

