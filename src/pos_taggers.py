from abc import ABC


class POSTagger(ABC):
    def tag(self, text):
        raise NotImplemented