import json
import pprint
import csv
from project_config import load_project_json, get_csv_output_path

pp = pprint.PrettyPrinter(indent=2)
ldbvars = load_project_json("key_variables.json")

ldb_week_preview = []
print_preview = []

week_matchups = load_project_json("week_matchups.json")
ldb_xmatchups = load_project_json("ldb_xmatchups.json")
ldb_teams = ldbvars["ldb_teams"]
ldb_cats = ldbvars["ldb_cats"]
ldb_bat_cats = ldbvars["ldb_bat_cats"]
ldb_pit_cats = ldbvars["ldb_pit_cats"]
ldb_bad_cats = ldbvars["ldb_bad_cats"]
current_week = ldbvars["current_week"]
for matchup in week_matchups:
	for item in matchup:
		item["bat_xwins"] = 0
		item["pit_xwins"] = 0
	for ldb_cat in ldb_bat_cats:
		matchup[0][ldb_cat] = ldb_xmatchups[matchup[0]["team"]][matchup[1]["team"]][ldb_cat]
		matchup[1][ldb_cat] = ldb_xmatchups[matchup[1]["team"]][matchup[0]["team"]][ldb_cat]
		matchup[0]["bat_xwins"] += ldb_xmatchups[matchup[0]["team"]][matchup[1]["team"]][ldb_cat]
		matchup[1]["bat_xwins"] += ldb_xmatchups[matchup[1]["team"]][matchup[0]["team"]][ldb_cat]
	for ldb_cat in ldb_pit_cats:
		matchup[0][ldb_cat] = ldb_xmatchups[matchup[0]["team"]][matchup[1]["team"]][ldb_cat]
		matchup[1][ldb_cat] = ldb_xmatchups[matchup[1]["team"]][matchup[0]["team"]][ldb_cat]
		matchup[0]["pit_xwins"] += ldb_xmatchups[matchup[0]["team"]][matchup[1]["team"]][ldb_cat]
		matchup[1]["pit_xwins"] += ldb_xmatchups[matchup[1]["team"]][matchup[0]["team"]][ldb_cat]
	for item in matchup:
		item["xwins"] = item["bat_xwins"] + item["pit_xwins"]

for matchup in week_matchups:
	print(matchup)
	con_cats = ""
	con_count = 0
	for ldb_cat in ldb_cats:
		cat_chasm = abs(matchup[0][ldb_cat] - matchup[1][ldb_cat])
		if cat_chasm < 0.2:
			if not ldb_cat in con_cats:
				con_count += 1
				con_cats += (str(ldb_cat) + " ")
	contested = str(con_count) + " Categories Contested"
	print_preview.append(["Team", "xBat", "xPit", "xWins", contested])
	for item in matchup:
		bxwins = round(item["bat_xwins"])
		pxwins = round(item["pit_xwins"])
		xscores = [item["team"], bxwins, pxwins, bxwins + pxwins, str(con_cats)]
		print_preview.append(xscores)
	print_preview.append([" "])
 
pp.pprint(print_preview)
	
with get_csv_output_path("week_preview.csv").open('wt', newline='') as csv_file:
	csvCLAP = csv.writer(csv_file)
	for item in print_preview:
		csvCLAP.writerow(item)
		

# f = open('week_preview.json', 'wt')
# json.dump(ldb_weekly_xmatchups, f,indent = 2)
# f.close()


