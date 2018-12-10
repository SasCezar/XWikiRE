"""
Taken from NLTK library
"""


def align_tokens(tokens, sentence):
    """
    This module attempt to find the offsets of the tokens in *s*, as a sequence
    of ``(start, end)`` tuples, given the tokens and also the source string.

    :param tokens: The list of strings that are the result of tokenization
    :type tokens: list(str)
    :param sentence: The original string
    :type sentence: str
    :rtype: list(tuple(int,int))
    """
    point = 0
    offsets = []
    for token in tokens:
        try:
            start = sentence.index(token, point)
        except ValueError:
            raise ValueError('substring "{}" not found in "{}"'.format(token, sentence))
        point = start + len(token)
        offsets.append((start, point))
    return offsets
