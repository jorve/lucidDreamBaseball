import codecs, json
import scipy
from scipy import stats
import pprint
import decimal
import numpy

pp = pprint.PrettyPrinter(indent=2)

ldbvarfile = open('key_variables.json')
ldbvars = json.load(ldbvarfile)

ldb_xmatchups = {}

with open('team_CLAPS.json') as json_file:
	team_CLAPS = json.load(json_file)
	ldb_teams = ldbvars["ldb_teams"]
	ldb_cats = ldbvars["ldb_cats"]
	ldb_bad_cats = ldbvars["ldb_bad_cats"]
	for ldb_team1 in ldb_teams:
		if not ldb_team1 in ldb_xmatchups:
			ldb_xmatchups[ldb_team1] = {}
		for ldb_team2 in ldb_teams:
			if not ldb_team2 in ldb_xmatchups[ldb_team1]:
				ldb_xmatchups[ldb_team1][ldb_team2] = []
			for ldb_cat in ldb_cats:
				f = numpy.array(scipy.stats.ttest_ind(team_CLAPS[ldb_team1][ldb_cat + "_array"], team_CLAPS[ldb_team2][ldb_cat + "_array"]))
				ldb_xmatchups[ldb_team1][ldb_team2].append(f.tolist())


pp.pprint(ldb_xmatchups)

f = open('ldb_xmatchups.json', 'wt')
json.dump(ldb_xmatchups, f,indent = 4)
f.close()
