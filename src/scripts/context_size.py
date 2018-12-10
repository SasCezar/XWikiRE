import csv
import json


def read(path):
    with open(path, "rt", encoding="utf8") as inf:
        for line in inf:
            obj = json.loads(line)
            context = obj['document']
            yield context


def get_avarage(contexts):
    i = 0
    total = 0
    for context in contexts:
        total += len(context)
        i += 1

    return total / i


if __name__ == '__main__':
    with open("stats_context.tsv", "wt", encoding="utf8") as outf:
        writer = csv.writer(outf, delimiter="\t")
        writer.writerow(["lang", "train", "dev", "test"])

        writer.writerow(["OMER"])
        omer_path = "/image/nlp-letre/all_splits_json/entity_split/{set}.configd{n}.prepend_nil-processed-corenlp.txt"

        results = ['en']

        for set_type, n in [('train', '5'), ('dev', '5'), ('test', '')]:
            file = omer_path.format(set=set_type, n=n)
            contexts = read(file)
            avg = get_avarage(contexts)
            results.append(avg)

        writer.writerow(results)

        writer.writerow(["OUR"])
        train_our_path = "/image/nlp-letre/QA/preprocessed_data/{set}/qas_{lang}_{set}_set_for-{of}{sub}.json.configd.prepend_nil-processed-{tok}.txt"
        langs = [("en", "es", ".subsample", "corenlp"), ("es", "en", "", "spacy"), ("fr", "en", ".subsample", "spacy"),
                 ("de", "en", ".subsample", "spacy"), ("it", "en", "", "spacy")]
        for lang, of, sub, tok in langs:
            results = [lang]
            for set_type in ['train', 'dev', 'test']:
                if set_type == "train":
                    if lang != "es":
                        file = train_our_path.format(lang=lang, set=set_type, of=of, sub=sub, tok=tok)
                    else:
                        "/image/nlp-letre/QA/preprocessed_data/{set}/qas_{lang}_{set}_set.json.configd.prepend_nil-processed-{tok}.txt"
                else:
                    if lang == "en":
                        our_path = "/image/nlp-letre/QA/preprocessed_data/{set}/qas_{lang}_parallel_es-en_{set}_set.json.configd.prepend_nil-processed-{tok}.txt"
                    else:
                        our_path = "/image/nlp-letre/QA/preprocessed_data/{set}/qas_{lang}_parallel_{lang}-{of}_{set}_set.json.configd.prepend_nil-processed-{tok}.txt"

                    file = our_path.format(lang=lang, set=set_type, of=of, tok=tok)

                contexts = read(file)
                avg = get_avarage(contexts)
                results.append(avg)

            writer.writerow(results)
