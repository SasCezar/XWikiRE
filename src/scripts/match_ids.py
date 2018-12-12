import json
import logging


def match(source_file, target_file, outfile):
    mapping = {}
    with open(source_file, "rt", encoding="utf8") as inf:
        obj = json.load(inf)

    data = obj['data'][0]
    assert len(obj['data']) == 1
    for paragraph in data["paragraphs"]:
        for qa in paragraph['qas']:
            mapping[qa['question_id']] = qa['id']

    logging.info(len(mapping))

    with open(target_file, "rt", encoding="utf8") as inf, open(outfile, "wt", encoding="utf8") as outf:
        for line in inf:
            obj = json.loads(line)
            obj['id'] = mapping[obj['q_id']]
            outf.write(json.dumps(obj, ensure_ascii=False) + "\n")


if __name__ == '__main__':
    base_path = "/image/nlp-letre/QA/data/relation_split_5/{lang}/{fold}/unprocessed_balanced_examples_{set}_{fold}.json.configd.prepend_nil"
    target_base = "/image/nlp-letre/QA/data/relation_split_5/{lang}/{fold}/balanced_examples_{set}_{fold}.json{prepend}"
    for lang in ['en']:
        for fold in [0]:  # , 1, 2, 3, 4]:
            for type_set in ['dev', 'test']:
                source_file = base_path.format(lang=lang, fold=fold, set=type_set)
                target_file = target_base.format(lang=lang, fold=fold, set=type_set, prepend='')
                outfile = target_base.format(lang=lang, fold=fold, set=type_set, prepend='.fixed_id')

                match(source_file, target_file, outfile)
