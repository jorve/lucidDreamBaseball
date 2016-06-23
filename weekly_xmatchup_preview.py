import json
import pprint

pp = pprint.PrettyPrinter(indent=2)

ldbvarfile = open('key_variables.json')
ldbvars = json.load(ldbvarfile)

ldb_weekly_xmatchups = []

with open('ldb_xmatchups.json') as json_file1, open('schedule.json') as json_file2:
	ldb_xmatchups = json.load(json_file1)
	schedule = json.load(json_file2)
	ldb_teams = ldbvars["ldb_teams"]
	ldb_cats = ldbvars["ldb_cats"]
	ldb_bad_cats = ldbvars["ldb_bad_cats"]
	current_week = ldbvars["current_week"]
	week_matchups = schedule["body"]["schedule"]["periods"]
	current_matchups = week_matchups[current_week]["matchups"]
	pp.pprint(current_matchups)
	for current_matchup in current_matchups:
		matchup = [{"team":current_matchup["away_team"]["long_abbr"], "away": True}, {"team":current_matchup["home_team"]["long_abbr"], "away": False}]
		ldb_weekly_xmatchups.append(matchup)
		
pp.pprint(ldb_weekly_xmatchups)
f = open('week_matchups.json', 'wt')
json.dump(ldb_weekly_xmatchups, f,indent = 2)
f.close()


