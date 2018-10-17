def find_full_matches(sequence, answer):
    return find_sub_list(answer, sequence)


def find_matches(sequence, answer):
    elements = set(answer)
    return [index for index, value in enumerate(sequence) if value in elements]


def find_sub_list(sublist, list):
    results = []
    sll = len(sublist)
    for ind in (i for i, e in enumerate(list) if e == sublist[0]):
        if list[ind:ind + sll] == sublist:
            results.append(range(ind, ind + sll))

    return results