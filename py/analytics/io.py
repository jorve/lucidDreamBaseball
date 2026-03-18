import json


def read_json(path_value):
	with path_value.open() as infile:
		return json.load(infile)


def write_json(path_value, payload):
	path_value.parent.mkdir(parents=True, exist_ok=True)
	with path_value.open("w") as outfile:
		json.dump(payload, outfile, indent=2)
