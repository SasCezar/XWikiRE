import csv
import io
import json


def load_fastText_vocab(fname):
    fin = io.open(fname, 'r', encoding='utf-8', newline='\n', errors='ignore')
    vocab = set()
    vocab_lower = set()
    for line in fin:
        tokens = line.rstrip().split(' ')
        vocab.add(tokens[0])
        vocab_lower.add(tokens[0].lower())

    return vocab


def load_BERT_vocab(fname):
    with open(fname, 'rt', encoding='utf-8') as fin:
        vocab = set()
        vocab_lower = set()
        for line in fin:
            vocab.add(line.strip())
            vocab_lower.add(line.strip().lower())

    return vocab


def load_examples_vocab(fname):
    doc_vocab = set()
    question_vocab = set()
    with open(fname, "rt", encoding="utf-8") as inf:
        for line in inf:
            obj = json.loads(line)
            doc_tokens = obj['document']
            doc_vocab.update(doc_tokens)
            question_tokens = obj['question']
            question_vocab.update(question_tokens)

    return doc_vocab, question_vocab


def load_examples_vocab_lower(fname):
    doc_vocab = set()
    question_vocab = set()
    with open(fname, "rt", encoding="utf-8") as inf:
        for line in inf:
            obj = json.loads(line)
            doc_tokens = [x.lower() for x in obj['document']]
            doc_vocab.update(doc_tokens)
            question_tokens = [x.lower() for x in obj['question']]
            question_vocab.update(question_tokens)

    return doc_vocab, question_vocab


def get_intersection(document_vocab, model_vocab):
    return document_vocab.intersection(model_vocab)


if __name__ == '__main__':
    BERT_path = "/image/nlp-letre/QA/language_models/multi_cased_L-12_H-768_A-12/vocab.txt"
    fastText_base = "/image/nlp-letre/QA/language_models/wiki.multi.{lang}.vec"
    example_base = "/image/nlp-letre/QA/preprocessed_data/test/qas_{lang}_parallel_{lang}-{pair}_test_set.json.configd.prepend_nil-processed-{tool}.txt"
    en_example = "/image/nlp-letre/QA/preprocessed_data/test/qas_en_parallel_es-en_test_set.json.configd.prepend_nil-processed-corenlp.txt"

    languages = [('it', 'en', 'spacy'), ('fr', 'en', 'spacy'), ('es', 'en', 'spacy'), ('de', 'en', 'spacy'),
                 ('en', 'es', 'corenlp')]

    BERT_vocab, BERT_lower = load_BERT_vocab(BERT_path)
    with open("vocab_intersection_stats.tsv", "wt", encoding="utf8") as outf, \
            open("vocab_intersection_normalized.tsv", "wt", encoding="utf8") as outn, \
            open("vocab_intersection_stats_lower.tsv", "wt", encoding="utf8") as outl:
        writer = csv.writer(outf, delimiter="\t")
        writernormalized = csv.writer(outf, delimiter="\t")
        writerlower = csv.writer(outl, delimiter="\t")
        writer.writerow(
            ["lang", "bert_document_percent", "bert_question_percent", "bert_percent", "fastText_document_percent",
             "fastText_question_percent", "fastText_percent_percent"])
        writerlower.writerow(
            ["lang", "bert_document_percent", "bert_question_percent", "bert_percent", "fastText_document_percent",
             "fastText_question_percent", "fastText_percent_percent"])
        writernormalized.writerow(
            ["lang", "bert_document_percent", "bert_question_percent", "bert_percent", "fastText_document_percent",
             "fastText_question_percent", "fastText_percent_percent"])
        for language, pair, tool in languages:
            fastText_path = fastText_base.format(lang=language)
            fastText_vocab, fastText_vocab_lower = load_fastText_vocab(fastText_path)

            example_path = example_base.format(lang=language, pair=pair, tool=tool)

            if language == "en":
                example_path = en_example

            document_vocab, question_vocab = load_examples_vocab(example_path)

            bert_document_intersection = get_intersection(document_vocab, BERT_vocab)
            bert_document_percent = len(bert_document_intersection) / len(document_vocab)
            bert_question_intersection = get_intersection(question_vocab, BERT_vocab)
            bert_question_percent = len(bert_question_intersection) / len(question_vocab)
            language_vocab = question_vocab.union(document_vocab)
            bert_intersection = get_intersection(language_vocab, BERT_vocab)
            bert_percent = len(bert_intersection) / len(language_vocab)

            fastText_document_intersection = get_intersection(document_vocab, fastText_vocab)
            fastText_document_percent = len(fastText_document_intersection) / len(document_vocab)
            fastText_question_intersection = get_intersection(question_vocab, fastText_vocab)
            fastText_question_percent = len(fastText_question_intersection) / len(question_vocab)
            fastText_intersection = get_intersection(question_vocab.union(document_vocab), fastText_vocab)
            fastText_percent = len(fastText_intersection) / len(language_vocab)

            writer.writerow([language, bert_document_percent, bert_question_percent, bert_percent,
                             fastText_document_percent, fastText_question_percent, fastText_percent])

            document_vocab, question_vocab = load_examples_vocab_lower(example_path)

            bert_document_intersection = get_intersection(document_vocab, BERT_vocab)
            bert_document_percent = len(bert_document_intersection) / len(document_vocab)
            bert_question_intersection = get_intersection(question_vocab, BERT_vocab)
            bert_question_percent = len(bert_question_intersection) / len(question_vocab)
            language_vocab = question_vocab.union(document_vocab)
            bert_intersection = get_intersection(language_vocab, BERT_vocab)
            bert_percent = len(bert_intersection) / len(language_vocab)

            fastText_document_intersection = get_intersection(document_vocab, fastText_vocab)
            fastText_document_percent = len(fastText_document_intersection) / len(document_vocab)
            fastText_question_intersection = get_intersection(question_vocab, fastText_vocab)
            fastText_question_percent = len(fastText_question_intersection) / len(question_vocab)
            fastText_intersection = get_intersection(question_vocab.union(document_vocab), fastText_vocab)
            fastText_percent = len(fastText_intersection) / len(language_vocab)

            writerlower.writerow([language, bert_document_percent, bert_question_percent, bert_percent,
                                  fastText_document_percent, fastText_question_percent, fastText_percent])

            bert_document_intersection = get_intersection(document_vocab, BERT_lower)
            bert_document_percent = len(bert_document_intersection) / len(document_vocab)
            bert_question_intersection = get_intersection(question_vocab, BERT_lower)
            bert_question_percent = len(bert_question_intersection) / len(question_vocab)
            language_vocab = question_vocab.union(document_vocab)
            bert_intersection = get_intersection(language_vocab, BERT_lower)
            bert_percent = len(bert_intersection) / len(language_vocab)

            fastText_document_intersection = get_intersection(document_vocab, fastText_vocab_lower)
            fastText_document_percent = len(fastText_document_intersection) / len(document_vocab)
            fastText_question_intersection = get_intersection(question_vocab, fastText_vocab_lower)
            fastText_question_percent = len(fastText_question_intersection) / len(question_vocab)
            fastText_intersection = get_intersection(question_vocab.union(document_vocab), fastText_vocab_lower)
            fastText_percent = len(fastText_intersection) / len(language_vocab)

            writernormalized.writerow([language, bert_document_percent, bert_question_percent, bert_percent,
                                  fastText_document_percent, fastText_question_percent, fastText_percent])
