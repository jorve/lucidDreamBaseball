import csv
import json
import pprint
import scipy
from scipy import stats
import math
from operator import itemgetter

pp = pprint.PrettyPrinter(indent=2)

ldbvarfile = open('key_variables.json')
ldbvars = json.load(ldbvarfile)
ldb_teams = ldbvars["ldb_teams"]
ldb_cats = ldbvars["ldb_cats"]
ldb_bad_cats = ldbvars["ldb_bad_cats"]
ldb_bat_cats = ldbvars["ldb_bat_cats"]
current_week = ldbvars["current_week"]
WAR_adjustments = ldbvars["WAR_adjustments"]

batting_stats = {}
row_headers = ["Avail", "Player", "BPA", "R", "HR", "aRBI", "OBP", "OPS", "aSB", "Rank"]
rate_stats = ["OBP", "OPS"]
count_stats = ["R", "HR", "aRBI", "aSB"]
team_names = [ 'AIDS', 'Americ...', 'Bed St...', 'Brookl...', 'Brophy...', 'Capito...', 'Cornba...', 'Honeyn...', 'Izzy A...', 'Los Po...', 'The Co...', 'The Po...', 'Whitey...', 'Wind o...']

with open("./2016/batters2016.csv", "r") as f:
	with open("./2016/batters_updated.csv", "w") as f1:
		f.next()
		f.next()
		for line in f:
			f1.write(line)
			f1.write('\n')

with open("./2016/batters_updated.csv", "r") as f:
	with open("./2016/batters.json", "wt") as f1:
		f_csv = csv.DictReader(f, fieldnames=row_headers)
		batting_stats = list(f_csv)
batting_stats.pop()

with open('ldbCLAP.json') as jsonfile1, open('replacementLevel.json') as jsonfile2:
	ldbCLAP = json.load(jsonfile1)
	replacementLevel = json.load(jsonfile2)
	for item in batting_stats:
		for i in range(0,len(team_names)):
			if item["Avail"] == team_names[i]:
				item["Avail"] = ldb_teams[i]
		for header in row_headers[2:]:
			if item[header] != '':
				item[header] = float(item[header])
		item["Player"] = filter(None, item["Player"].split(" "))
		if len(item["Player"]) > 0:
			item["Team"] = item["Player"].pop()
			item["Player"].pop()
			item["Position"] = item["Player"].pop()
			item["LastName"] = item["Player"].pop()
			item["FirstName"] = item["Player"].pop(0)
			if len(item["Player"]) > 0:
				item["MiddleName"] = item["Player"]
			del item["Player"]
			item["ldbTeam"] = item["Avail"]
			del item["Avail"]
		elig = (item["BPA"]/WAR_adjustments["at_bats"])
		if elig >= current_week:
			item["weeks_elig"] = current_week
			item["pw_pa"] = elig/current_week
		else:
			item["weeks_elig"] = elig
			item["pw_pa"] = 1
		item["cat_WAR"] = {}
		item["cat_WAR"]["total_WAR"] = 0
		for stat in rate_stats:
			item["cat_WAR"]["pw" + stat] = item[stat]
			if stat in ldb_bad_cats:
				item["cat_WAR"]["zpw_" + stat] = (scipy.stats.norm.cdf(item[stat], loc=ldbCLAP[stat][0], scale=ldbCLAP[stat][1])) * -1
			else:
				item["cat_WAR"]["zpw_" + stat] = scipy.stats.norm.cdf(item[stat], loc=ldbCLAP[stat][0], scale=ldbCLAP[stat][1])
			item["cat_WAR"]["adj_zpw_" + stat] = (item["cat_WAR"]["zpw_" + stat] - WAR_adjustments["off_RL"]) * 0.1
			if item["pw_pa"] > 1:
				item["cat_WAR"]["adj_zpw_" + stat] *= item["pw_pa"]
			item["cat_WAR"]["total_WAR"] += item["cat_WAR"]["adj_zpw_" + stat]
		for stat in count_stats:
			if item["weeks_elig"] != 0:
				item["cat_WAR"]["pw" + stat] = (item[stat]/item["weeks_elig"])
				if stat in ldb_bad_cats:
					item["cat_WAR"]["zpw_" + stat] = (scipy.stats.norm.cdf((item["cat_WAR"]["pw" + stat]), loc=(ldbCLAP[stat][0]/10), scale=(ldbCLAP[stat][1]/10))) * -1
				else:
					item["cat_WAR"]["zpw_" + stat] = (scipy.stats.norm.cdf(item["cat_WAR"]["pw" + stat], loc=(ldbCLAP[stat][0]/10), scale=(ldbCLAP[stat][1]/10)))
				item["cat_WAR"]["adj_zpw_" + stat] = (item["cat_WAR"]["zpw_" + stat] - WAR_adjustments["off_RL"]) * 0.1
				item["cat_WAR"]["total_WAR"] += item["cat_WAR"]["adj_zpw_" + stat]
		item["cat_WAR"]["total_WAR"] *= item["weeks_elig"]
		# if item["ldbTeam"] != "FA": 
		# 	if item["Rank"] != 9999:
		# 		if item["weeks_elig"] > 5:
		# 			pp.pprint(item)

batting_stats_to_print = []
for item in batting_stats:
	if item["cat_WAR"]["total_WAR"] != 0:
					batting_stats_to_print.append([item["FirstName"], item["LastName"], item["ldbTeam"], item["Position"], item["cat_WAR"]["total_WAR"]])

printed = sorted(batting_stats_to_print, key=itemgetter(4))
pp.pprint(printed)

f = open('ldb_batters.json', 'wt')
json.dump(batting_stats, f, indent = 2)
f.close()

f = open('battingWAR.csv', 'wt')
fw = csv.writer(f)
for item in printed:
	fw.writerow(item)
f.close()


