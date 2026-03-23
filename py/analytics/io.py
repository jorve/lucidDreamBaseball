import json
from storage import StorageRecorder


def read_json(path_value):
	with path_value.open() as infile:
		return json.load(infile)


_STORAGE_RECORDER = StorageRecorder()


def write_json(path_value, payload):
	path_value.parent.mkdir(parents=True, exist_ok=True)
	with path_value.open("w") as outfile:
		json.dump(payload, outfile, indent=2)
	_STORAGE_RECORDER.record_json_artifact(
		path_value=path_value,
		payload=payload,
		artifact_kind="analytics",
		write_source="analytics.io.write_json",
	)
