import random
import csv

def gen_team_dict(team, entries):
    team_dict = {
        'team': team,
        'entries': entries
    }
    return team_dict

def gen_draft_dict(team, pick, odds):
    draft_dict = {
        'team': team,
        'pick': pick,
        'odds': odds
    }
    return draft_dict

lottery = []
draft_order = []
results = []

with open('teams_and_entries.csv') as f:
    reader = csv.reader(f)
    data = list(reader)

for a in data:
    team = gen_team_dict(a[0], int(a[1]))
    lottery.append(team)

while (len(draft_order) < len(lottery)):
    entries = 0
    entry_list = []
    for team in lottery:
        if team['team'] not in draft_order:
            for i in range(team['entries']):
                print(team['team'])
                entry_list.append(str(team['team']))
            entries += team['entries']
    selected_team = random.choice(entry_list)
    draft_order.append(selected_team)
    for team in lottery:
        if team['team'] is selected_team:
            team_odds = team['entries']/entries
    selected_team_dict = gen_draft_dict(selected_team, len(draft_order), team_odds)
    results.append(selected_team_dict)
    pass

print(results)