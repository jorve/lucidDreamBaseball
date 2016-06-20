import json
import scipy
import pprint

pp = pprint.PrettyPrinter(indent=2)

ldbvarfile = open('key_variables.json')
ldbvars = json.load(ldbvarfile)

with open('team_CLAPS.json') as json_file:
	team_CLAPS = json.load(json_file)

pp.pprint(team_CLAPS)
