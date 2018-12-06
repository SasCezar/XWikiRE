import json
import os
import sys


def create_splits(path, preprocess_path, out):
    with open(path, "rt", encoding="utf8") as inf:
        props = set(prop.strip().strip("\"") for prop in inf.readlines())

    with open(os.path.join(preprocess_path),
              "rt", encoding="utf8") as inf, open(out, "wt", encoding="utf8") as outf:
        for line in inf:
            obj = json.loads(line)
            prop_id = obj['prop_id'].strip()

            if prop_id in props:
                outf.write(line)


if __name__ == '__main__':
    path = sys.argv[1]
    preprocess_path = sys.argv[2]
    out = sys.argv[3]

    create_splits(path, preprocess_path, out)
