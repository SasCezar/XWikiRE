import ast
import csv
import json
import os
from collections import Counter, defaultdict


def get_prop_count(file):
    prop_count = Counter()
    with open(file, "rt", encoding="utf8") as inf:
        reader = csv.reader(inf, delimiter="\t")
        next(reader)
        for prop, count in reader:
            prop_count[prop] = prop_count[prop] + int(float(count))

    return prop_count


def get_folds(prop_cont):
    folds = defaultdict(list)
    folds_count = defaultdict(lambda: 0)
    half = int(len(prop_cont) / 2)
    i = 0
    for t in zip(prop_cont[:half], list(reversed(prop_cont))[:half]):
        fold_n = i % 10
        i += 1
        folds[fold_n].append(t[0][0])
        folds[fold_n].append(t[1][0])
        folds_count[fold_n] += t[0][1]
        folds_count[fold_n] += t[1][1]

    return folds, folds_count


def create_folds():
    combinations = [['it', 'en'], ['it'], ['en'], ['es'], ['de'], ['fr'], ['fr', 'en'], ['de', 'en'], ['es', 'en'], ['it', 'en', 'es', 'fr', 'de']]
    for langs in combinations:
        counts = Counter()
        for lang in langs:
            # counts.update(get_prop_count("{}_negative_example_stats.tsv".format(lang)))
            counts.update(get_prop_count("{}_positive_example_stats.tsv".format(lang)))

        folds, folds_count = get_folds(counts.most_common())

        with open("out/folds_{}.txt".format("-".join(langs)), "wt", encoding="utf8", newline="") as outf:
            writer = csv.writer(outf, delimiter="\t")
            for fold in folds:
                assert len(folds[fold]) == 12 and len(set(folds[fold])) == 12
                writer.writerow([fold, folds_count[fold], folds[fold]])


create_folds()


def load_folds(file):
    prop_fold = dict()
    with open(file, "rt", encoding="utf8") as inf:
        reader = csv.reader(inf, delimiter="\t")
        for fold, _, props in reader:
            for prop in ast.literal_eval(props):
                prop_fold[prop] = fold

    return prop_fold


def split_data():
    langs = ['it', 'en']

    for lang in langs:
        folds = load_folds("folds_{}.txt".format(lang))
        if not os.path.exists(lang):
            os.makedirs(lang)

        count = defaultdict(lambda: defaultdict(lambda: 0))
        for typ in ['negative', 'positive']:
            with open("{}_qa_{}.json".format(lang, typ), "rt", encoding="utf8") as inf:
                for line in inf:
                    qa = json.loads(line)
                    prop = qa['prop_id']
                    count[typ][folds[prop]] += 1

        fold_min = {}
        for i in range(0, 10):
            fold_min[str(i)] = min(count['positive'][str(i)], count['negative'][str(i)])
            print("Fold {}: {} examples".format(i, min(count['positive'][str(i)], count['negative'][str(i)])))

        files = {str(fold): open(os.path.join(lang, "{}_rel_split_{}.txt".format(lang, fold)), "wt", encoding="utf8")
                 for fold in range(0, 10)}
        fold_count = defaultdict(lambda: defaultdict(lambda: 0))
        for typ in ['negative', 'positive']:
            with open("{}_qa_{}.json".format(lang, typ), "rt", encoding="utf8") as inf:
                for line in inf:
                    qa = json.loads(line)
                    prop = qa['prop_id']
                    fold = folds[prop]
                    if fold_min[fold] == fold_count[typ][fold]:
                        continue
                    fold_count[typ][fold] += 1
                    files[fold].write(line)

                for i in range(0, 10):
                    assert fold_min[str(i)] == fold_count[typ][str(i)]

        for i in range(0, 10):
            assert fold_count['positive'][str(i)] == fold_count['negative'][str(i)]

        for file in files:
            files[file].close()

    for lang in langs:
        parallel = 'parallel'
        folds = load_folds("folds_{}_{}.txt".format(parallel, "-".join(langs)))
        if not os.path.exists(parallel):
            os.makedirs(parallel)

        count = defaultdict(lambda: defaultdict(lambda: 0))
        for typ in ['negative', 'positive']:
            with open("{}_qa_{}.json".format(lang, typ), "rt", encoding="utf8") as inf:
                for line in inf:
                    qa = json.loads(line)
                    prop = qa['prop_id']
                    count[typ][folds[prop]] += 1

        fold_min = {}
        for i in range(0, 10):
            fold_min[str(i)] = min(count['positive'][str(i)], count['negative'][str(i)])
            print("Fold {}: {} examples".format(i, min(count['positive'][str(i)], count['negative'][str(i)])))

        files = {str(fold): open(os.path.join(parallel, "{}_rel_split_{}.txt".format(lang, fold)), "wt", encoding="utf8")
                 for fold in range(0, 10)}
        fold_count = defaultdict(lambda: defaultdict(lambda: 0))
        for typ in ['negative', 'positive']:
            with open("{}_qa_{}.json".format(lang, typ), "rt", encoding="utf8") as inf:
                for line in inf:
                    qa = json.loads(line)
                    prop = qa['prop_id']
                    fold = folds[prop]
                    if fold_min[fold] == fold_count[typ][fold]:
                        continue
                    fold_count[typ][fold] += 1
                    files[fold].write(line)

                for i in range(0, 10):
                    assert fold_min[str(i)] == fold_count[typ][str(i)]

        for i in range(0, 10):
            assert fold_count['positive'][str(i)] == fold_count['negative'][str(i)]

        for file in files:
            files[file].close()


# split_data()
