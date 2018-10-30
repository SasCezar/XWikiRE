import csv
import json
import os
import traceback

import pandas


def omer_to_json(filename, out_path):
    dataset = pandas.read_csv(filename, names=['property', 'template', 'entity', 'context', 'answer'],
                              dtype=str, na_filter=False, delimiter="\t").to_dict('records')
    with open(out_path, "wt", encoding="utf8") as outf:
        i = 0
        for example in dataset:
            question = example['template'].replace("XXX", example['entity'])
            example['question'] = question
            if example['answer']:
                try:
                    context = example['context']
                    answer_text = example['answer']
                    start_index = context.index(answer_text)
                    end_index = start_index + len(answer_text)
                except:
                    traceback.print_exc()
                    i += 1
                    print("Answer: {} ---- Context: {}".format(example['answer'], example['context']))
                    continue
                example['start_index'] = start_index
                example['end_index'] = end_index
                example['na'] = 1
            else:
                example['start_index'] = -1
                example['end_index'] = -1
                example['na'] = 0
            outf.write(json.dumps(example, ensure_ascii=False) + "\n")
    print(i)


def extract_templates_set(path, outpath):
    seen = set()
    with open(path, "rt", encoding="utf8") as inf, \
            open(outpath, "wt", encoding="utf8") as outf:
        csvreader = csv.reader(inf, delimiter="\t")
        csvwriter = csv.writer(outf, delimiter="\t")
        for line in csvreader:
            if line[1] not in seen:
                seen.add(line[1])
                csvwriter.writerow([line[0], line[1]])


# extract_templates_set("C:\\Users\sasce\Downloads\\raw_data\\positive_examples", "C:\\Users\sasce\Downloads\\raw_data\\positive_examples_templates.tsv")


MAIN_FOLDER = "C:\\Users\sasce\Downloads\\all_splits\\"
OUT_FOLDER = "C:\\Users\sasce\Downloads\\all_splits_json\\"

SUBFOLDERS = ["entity_split", "relation_splits", "template_splits"]

if not os.path.exists(OUT_FOLDER):
    os.makedirs(OUT_FOLDER)

for subfolder in SUBFOLDERS:
    folder = os.path.join(MAIN_FOLDER, subfolder)
    out_folder = os.path.join(OUT_FOLDER, subfolder)
    files = os.listdir(folder)
    if not os.path.exists(out_folder):
        os.makedirs(out_folder)
    for file in files:
        file_path = os.path.join(folder, file)
        out_path = os.path.join(out_folder, file)
        omer_to_json(file_path, out_path)
