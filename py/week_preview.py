import json
import simplejson
import pprint
import csv

pp = pprint.PrettyPrinter(indent=2)

ldbvarfile = open('key_variables.json', 'r')
ldbvars = simplejson.load(ldbvarfile)

ldb_week_preview = []
print_preview = []

with open('week_matchups.json') as json_file1, open('ldb_xmatchups.json') as json_file2:
	week_matchups = json.load(json_file1)
	ldb_xmatchups = json.load(json_file2)
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
	
csv_file = open('week_preview.csv', 'wt')
csvCLAP = csv.writer(csv_file)
for item in print_preview:
	csvCLAP.writerow(item)
csv_file.close()
		

# f = open('week_preview.json', 'wt')
# json.dump(ldb_weekly_xmatchups, f,indent = 2)
# f.close()


