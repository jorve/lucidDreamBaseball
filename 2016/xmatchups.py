import codecs, json
import scipy
from scipy import stats
import pprint
import numpy
import math

pp = pprint.PrettyPrinter(indent=2)

ldbvarfile = open('key_variables.json')
ldbvars = json.load(ldbvarfile)

ldb_xmatchups = {
}

with open('team_CLAPS.json') as json_file:
	team_CLAPS = json.load(json_file)
	ldb_teams = ldbvars["ldb_teams"]
	ldb_cats = ldbvars["ldb_cats"]
	ldb_bad_cats = ldbvars["ldb_bad_cats"]
	for ldb_team1 in ldb_teams:
		if not ldb_team1 in ldb_xmatchups:
			ldb_xmatchups[ldb_team1] = {
				"xscores": {}
			}
		for ldb_team2 in ldb_teams:
			if not ldb_team2 in ldb_xmatchups[ldb_team1]:
				ldb_xmatchups[ldb_team1][ldb_team2] = {}
			for ldb_cat in ldb_cats:
				cat_mean_diff = team_CLAPS[ldb_team1][ldb_cat + "_mean"] - team_CLAPS[ldb_team2][ldb_cat + "_mean"]
				cat_stddev_diff = team_CLAPS[ldb_team1][ldb_cat + "_stddev"] + team_CLAPS[ldb_team2][ldb_cat + "_stddev"]
				if cat_stddev_diff != 0:
					norm_diff = cat_mean_diff/math.sqrt(cat_stddev_diff)
				else:
					norm_diff = 0
				if ldb_cat in ldb_bad_cats:
					norm_diff *= -1
				ldb_xmatchups[ldb_team1][ldb_team2][ldb_cat] = scipy.stats.norm.cdf(norm_diff)
				if not ldb_team2 in ldb_xmatchups[ldb_team1]["xscores"]:
					ldb_xmatchups[ldb_team1]["xscores"][ldb_team2] = 0
				ldb_xmatchups[ldb_team1]["xscores"][ldb_team2] += ldb_xmatchups[ldb_team1][ldb_team2][ldb_cat]

pp.pprint(ldb_xmatchups)

f = open('ldb_xmatchups.json', 'wt')
json.dump(ldb_xmatchups, f,indent = 4)
f.close()
