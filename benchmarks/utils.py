from contextlib import contextmanager
from time import time
import pandas as pd


class DiagnosticTimer:
    def __init__(self):
        self.diagnostics = []

    @contextmanager
    def time(self, **kwargs):
        tic = time()
        yield
        toc = time()
        kwargs['runtime'] = toc - tic
        self.diagnostics.append(kwargs)

    def dataframe(self):
        return pd.DataFrame(self.diagnostics)
