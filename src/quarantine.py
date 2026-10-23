import json
import os
from datetime import datetime

quarantine_folder = "data/quarantine/"


def save_bad_record(stream, record, errors):
    os.makedirs(quarantine_folder, exist_ok=True)

    line = {
        "stream": stream,
        "errors": errors,
        "record": record,
        "quarantined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    with open(quarantine_folder + stream + "_bad.jsonl", "a") as f:
        f.write(json.dumps(line) + "\n")