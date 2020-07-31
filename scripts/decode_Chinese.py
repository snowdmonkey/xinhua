import json
from pathlib import Path

INPUT_FILE = Path("/Users/liuxuefe/PycharmProjects/xinhua/data/comprehend_output/output_key_phrase")

output_file = INPUT_FILE.with_name(INPUT_FILE.stem+"_decode")

with INPUT_FILE.open("r") as f:
    lines = f.readlines()

lines = [json.loads(line) for line in lines]
lines = [json.dumps(line, ensure_ascii=False) for line in lines]

with output_file.open("w") as f:
    f.writelines(lines)
