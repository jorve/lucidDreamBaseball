import datetime
import json
from project_config import get_schedule_path

simple_schedule = []
current_period = 0
complete_period = 0
schedule_path = get_schedule_path()
now = datetime.datetime.now()

with schedule_path.open() as json_file1:
	json_data = json.load(json_file1)
	periods = json_data["body"]["schedule"]["periods"]
	for period in periods:
		start_date = datetime.datetime.strptime(period["start"], "%m/%d/%y")
		end_date = datetime.datetime.strptime(period["end"], "%m/%d/%y")
		simple_schedule.append([int(period["id"]), start_date, end_date])
		if start_date <= now <= end_date:
			current_period = int(period["id"])
			complete_period = int(period["id"]) - 1

print(now)
print(current_period)
print(complete_period)

