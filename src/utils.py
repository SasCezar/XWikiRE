def get_chunks(sequence, chunk_size):
    """
    Computes the lower limit and the upper limit of a collection of documents
    :param sequence:
    :param chunk_size:
    :return: The doc id for the lower and upper limits
    """
    for j in range(0, len(sequence), chunk_size):
        chunck = sequence[j:j + chunk_size]
        yield chunck
