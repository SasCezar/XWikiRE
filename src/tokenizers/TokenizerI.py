from abc import ABC

SENTENCE_BREAKS = {'.', '!', '?', '…', '...'}


class TokenizerI(ABC):

    def __init__(self):
        self.BREAK_LEVEL_TOKENS = {
            " ": 1,
            "\n": 2,
            "SENTENCE_BREAK": 3,
            "\n\n": 4,
        }

        self.SENTENCE_BREAKS = SENTENCE_BREAKS

    def tokenize(self, text):
        raise NotImplemented

    # ToDo Add unique logic for all tokenizers
    def _get_break_levels(self, tokens):
        raise NotImplemented