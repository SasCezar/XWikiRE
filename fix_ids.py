import hashlib
import logging
import re
import sys

key_re = re.compile(r"\"_key\": \"(?P<key>.*)\"")
from_re = re.compile(r"\"_from\": \"(?P<key>[^\"]*)\"")
to_re = re.compile(r"\"_to\": \"(?P<key>.*)\"")
TMPL = "\"_key\": \"{}\""

def _get_id(string):
  return hashlib.sha1(string.encode("utf-8")).hexdigest()


def fix(path, out_path):
  seen = set()
  with open(path, "rt", encoding="utf8") as inf, \
          open(out_path, "wt", encoding="utf8") as outf:
    for line in inf:
      line = line.replace("_id", "_key")
      key_value = key_re.search(line).group("key")

      if key_value in seen:
        continue

      seen.add(key_value)

      hash = _get_id(key_value)
      fixed = line.replace(TMPL.format(key_value), TMPL.format(hash))

      outf.write(fixed)




def fix_edges(path, out_path):
  with open(path, "rt", encoding="utf8") as inf, \
          open(out_path, "wt", encoding="utf8") as outf:
    for line in inf:
      try:
        key_value = from_re.search(line).group("key")

        hash = _get_id(key_value)
        line = line.replace(key_value, hash)

        key_value = to_re.search(line).group("key")

        hash = _get_id(key_value)
        fixed = line.replace(key_value, hash)
      except:
        print(line)
        raise KeyError

      outf.write(fixed)


if __name__ == '__main__':
  logging.basicConfig(format='%(asctime)s - %(module)s - %(levelname)s - %(message)s', level=logging.INFO)
  logging.info("Running %s", " ".join(sys.argv))
  # fix("F:\Data Arango\\nodes.json", "F:\Data Arango\\nodes_fixed.json")
  fix("F:\Data Arango\\entities_objs.json", "F:\Data Arango\\entities_objs_fixed.json")
  # fix_edges("F:\Data Arango\\edges.json", "F:\Data Arango\\edges_fixed.json")
  logging.info("Completed %s", " ".join(sys.argv))
