import json
import pprint
import datetime

pp = pprint.PrettyPrinter(indent=2)

simple_schedule = []
current_period = 0
complete_period = 0

with open("./2016/schedule.json") as json_file1:
	json_data = json.load(json_file1)
	periods = json_data["body"]["schedule"]["periods"]
	for period in periods:
		start_date = datetime.datetime.strptime(period["start"], '%m/%d/%y')
		end_date = datetime.datetime.strptime(period["end"], '%m/%d/%y')
		simple_schedule.append([int(period["id"]), start_date, end_date])
		if datetime.datetime.now() <= end_date and datetime.datetime.now() >= start_date:
			current_period = int(period["id"])
			complete_period = int(period["id"]) - 1

print datetime.datetime.now()

print(current_period)
print(complete_period)

