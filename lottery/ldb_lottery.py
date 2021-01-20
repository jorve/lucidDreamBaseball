import random
import csv

def gen_team_dict(team, entries):
    team_dict = {
        'team': team,
        'entries': entries
    }
    return team_dict

def gen_draft_dict(team, pick, odds, first_pick):
    draft_dict = {
        'team': team,
        'pick': pick,
        'odds': odds,
        'first_pick': first_pick
    }
    return draft_dict

lottery = []
draft_order = []
results = []
total_entries = 0

with open('./lottery/teams_and_entries.csv') as f:
    reader = csv.reader(f)
    data = list(reader)

for a in data:
    team = gen_team_dict(a[0], int(a[1]))
    total_entries += int(a[1])
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
            first_pick_odds = team['entries']/total_entries
    selected_team_dict = gen_draft_dict(selected_team, len(draft_order), team_odds, first_pick_odds)
    results.append(selected_team_dict)
    pass

keys = results[0].keys()

with open('./lottery/ldb_draft_order.csv', 'wt')  as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(results)