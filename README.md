# Lucid Dream Baseball 
## Data Analysis for Head-to-Head Leagues on Top of the CBS Fantasy Sports API

This package is primarily for head-to-head leagues, and I haven't given much thought to how something similar would work for roto leagues. If you're interested in adding something for roto format, let me know and I'd be happy to think it through with you.

### Categorical League Average Performance (CLAP)
The big (and probably erroneous) assumption of this model is that weekly scores in each category will be normally distributed around a true population mean. As of right now, `scoring.py` loops through each week's scores and calculates this mean per category. The CLAP is then the benchmark for two calculations: (1) the expected results for the upcoming week's matchups and (2) league-specific Wins Above Replacement values by player.
1. _Expected Results of Upcoming Weekly Matchups:_ 

A 

### Using the Fantasy Sports API
This package primarily uses two calls to the API: Live Scoring and Schedules. Because there are some inaccuracies with the Live Scoring API, some values need to be manually changed, which requires saving & altering the resultant JSON file (as opposed to working directly from the API results). The Schedules API has no inaccuracies, but is only used once so the results are saved locally here as well.

Complicating matters further is the CBS access token. These tokens are good for about 3 days, and I usually get mine by viewing the HTML source code from a CBS fantasy sports page I am logged into. At some point I might build a Phantom.js script to grab this automatically, but given the weekly frequency of this requirement, I'll likely work on improving the Python package before automating the access token capture. If you search for `api.Token` in the source code it should be the first result.



