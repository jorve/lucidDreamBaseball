import json
import pprint
from project_config import load_project_json, get_json_output_path

pp = pprint.PrettyPrinter(indent=2)
ldbvars = load_project_json("key_variables.json")

ldb_weekly_xmatchups = []

ldb_xmatchups = load_project_json("ldb_xmatchups.json")
schedule = load_project_json("schedule.json")
ldb_teams = ldbvars["ldb_teams"]
ldb_cats = ldbvars["ldb_cats"]
ldb_bad_cats = ldbvars["ldb_bad_cats"]
current_week = ldbvars["current_week"]
week_matchups = schedule["body"]["schedule"]["periods"]
current_matchups = week_matchups[current_week]["matchups"]
pp.pprint(current_matchups)
for current_matchup in current_matchups:
	matchup = [{"team": current_matchup["away_team"]["long_abbr"], "away": True}, {"team": current_matchup["home_team"]["long_abbr"], "away": False}]
	ldb_weekly_xmatchups.append(matchup)
		
pp.pprint(ldb_weekly_xmatchups)
with get_json_output_path("week_matchups.json").open('wt') as f:
	json.dump(ldb_weekly_xmatchups, f, indent=2)


