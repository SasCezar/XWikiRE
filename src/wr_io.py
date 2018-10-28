import csv
import json
import os
import re
import traceback
from collections import Counter

import pandas
from nltk.tokenize import word_tokenize, sent_tokenize
from tqdm import tqdm


def omer_to_json(filename, out_path):
    dataset = pandas.read_csv(filename, names=['property', 'template', 'entity', 'context', 'answer'],
                              dtype=str, na_filter=False, delimiter="\t").to_dict('records')
    with open(out_path, "wt", encoding="utf8") as outf:
        i = 0
        for example in dataset:
            question = example['template'].replace("XXX", example['entity'])
            example['question'] = question
            q_tokens = word_tokenize(question)
            example['question_sequence'] = q_tokens
            example['answer_sequence'] = []
            xi = list(map(word_tokenize, sent_tokenize(example['context'])))
            c_tokens = [process_tokens(tokens) for tokens in xi]
            example['context_sequence'] = c_tokens
            if example['answer']:
                try:
                    context = example['context']
                    answer_text = example['answer']
                    start_index = context.index(answer_text)
                    end_index = start_index + len(example['answer'])
                    yi0, yi1 = get_word_span(example['context'], xi, start_index, end_index)
                    example['start_index'] = yi0[1]
                    example['end_index'] = yi1[1]-1
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


def levy2squad(levy_path, squad_path):
    squad = {'data': [{'paragraphs': []}], 'version': '0.1'}
    paras = squad['data'][0]['paragraphs']

    with open(levy_path, 'rt') as fp:
        reader = csv.reader(fp, delimiter='\t')
        for i, each in enumerate(reader):
            rel, ques_temp, ques_arg, sent = each[:4]

            ques = ques_temp.replace('XXX', ques_arg)

            qa = {'question': ques, 'answers': [], 'id': str(i)}
            if len(each) > 4:
                ans_list = each[4:]
                indices = [(sent.index(ans), sent.index(ans) + len(ans)) for ans in ans_list]
                starts, ends = zip(*indices)
                ans_start = min(starts)
                ans_end = max(ends)
                ans = sent[ans_start:ans_end]
                qa['answers'].append({'text': ans, 'answer_start': ans_start})
            paras.append({'context': sent, 'qas': [qa]})

    with open(squad_path, 'w') as fp:
        json.dump(squad, fp)


def get_2d_spans(text, tokenss):
    spanss = []
    cur_idx = 0
    for tokens in tokenss:
        spans = []
        for token in tokens:
            if text.find(token, cur_idx) < 0:
                print(tokens)
                print("{} {} {}".format(token, cur_idx, text))
                raise Exception()
            cur_idx = text.find(token, cur_idx)
            spans.append((cur_idx, cur_idx + len(token)))
            cur_idx += len(token)
        spanss.append(spans)
    return spanss


def process_tokens(temp_tokens):
    tokens = []
    for token in temp_tokens:
        flag = False
        l = ("-", "\u2212", "\u2014", "\u2013", "/", "~", '"', "'", "\u201C", "\u2019", "\u201D", "\u2018", "\u00B0")
        # \u2013 is en-dash. Used for number to nubmer
        # l = ("-", "\u2212", "\u2014", "\u2013")
        # l = ("\u2013",)
        tokens.extend(re.split("([{}])".format("".join(l)), token))
    return tokens


def get_word_span(context, wordss, start, stop):
    spanss = get_2d_spans(context, wordss)
    idxs = []
    for sent_idx, spans in enumerate(spanss):
        for word_idx, span in enumerate(spans):
            if not (stop <= span[0] or start >= span[1]):
                idxs.append((sent_idx, word_idx))

    assert len(idxs) > 0, "{} {} {} {}".format(context, spanss, start, stop)
    return idxs[0], (idxs[-1][0], idxs[-1][1] + 1)


def get_word_idx(context, wordss, idx):
    spanss = get_2d_spans(context, wordss)
    return spanss[idx[0]][idx[1]][0]


def prepro_each(args, data_type, start_ratio=0.0, stop_ratio=1.0, out_name="default", in_path=None):
    if args.tokenizer == "PTB":
        import nltk
        sent_tokenize = nltk.sent_tokenize

        def word_tokenize(tokens):
            return [token.replace("''", '"').replace("``", '"') for token in nltk.word_tokenize(tokens)]
    else:
        raise Exception()

    if not args.split:
        sent_tokenize = lambda para: [para]

    source_path = in_path or os.path.join(args.source_dir, "{}-v1.1.json".format(data_type))
    source_data = json.load(open(source_path, 'r'))

    q, cq, y, rx, rcx, ids, idxs = [], [], [], [], [], [], []
    na = []
    cy = []
    x, cx = [], []
    answerss = []
    p = []
    word_counter, char_counter, lower_word_counter = Counter(), Counter(), Counter()
    start_ai = int(round(len(source_data['data']) * start_ratio))
    stop_ai = int(round(len(source_data['data']) * stop_ratio))
    for ai, article in enumerate(tqdm(source_data['data'][start_ai:stop_ai])):
        xp, cxp = [], []
        pp = []
        x.append(xp)
        cx.append(cxp)
        p.append(pp)
        for pi, para in enumerate(article['paragraphs']):
            # wordss
            context = para['context']
            context = context.replace("''", '" ')
            context = context.replace("``", '" ')
            xi = list(map(word_tokenize, sent_tokenize(context)))
            xi = [process_tokens(tokens) for tokens in xi]  # process tokens
            # given xi, add chars
            cxi = [[list(xijk) for xijk in xij] for xij in xi]
            xp.append(xi)
            cxp.append(cxi)
            pp.append(context)

            for xij in xi:
                for xijk in xij:
                    word_counter[xijk] += len(para['qas'])
                    lower_word_counter[xijk.lower()] += len(para['qas'])
                    for xijkl in xijk:
                        char_counter[xijkl] += len(para['qas'])

            rxi = [ai, pi]
            assert len(x) - 1 == ai
            assert len(x[ai]) - 1 == pi
            for qa in para['qas']:
                # get words
                qi = word_tokenize(qa['question'])
                qi = process_tokens(qi)

                answers = []
                for answer in qa['answers']:
                    answer_text = answer['text']
                    answers.append(answer_text)
                    answer_start = answer['answer_start']
                    answer_stop = answer_start + len(answer_text)
                    # TODO : put some function that gives word_start, word_stop here
                    yi0, yi1 = get_word_span(context, xi, answer_start, answer_stop)



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


# omer_to_json("C:\\Users\sasce\Downloads\\raw_data\\negative_examples",
#             "C:\\Users\sasce\Downloads\\raw_data\\negative_examples.json")

# omer_to_json("C:\\Users\sasce\Downloads\\raw_data\\positive_examples",
#              "C:\\Users\sasce\Downloads\\raw_data\\positive_examples.json")


MAIN_FOLDER = "C:\\Users\sasce\Downloads\\all_splits\\"
OUT_FOLDER = "C:\\Users\sasce\Downloads\\all_splits_json\\"

SUBFOLDERS = ["entity_split", "relation_slit", "template_split"]

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
