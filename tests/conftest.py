import os
import sys
basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(basedir)

import pytest
import htcondor_dag
import StringIO
import __builtin__

@pytest.fixture
def dag():
    return htcondor_dag.Dag("test")

class MockFS(object):
    def __init__(self):
        self.files = {}

    def open(self, filename, mode="r"):
        if "w" in mode:
            nf = StringIO.StringIO()
            c = nf.close
            def close():
                self.files[filename] = nf.getvalue()
                c()
            def __enter__(): return nf
            def __exit__(*args): close()
            nf.close = close
            nf.__enter__ = __enter__
            nf.__exit__ = __exit__
            self.files[filename] = nf
            return nf
        return StringIO.StringIO(self.files[filename])

    def __getitem__(self, key):
        return self.files[key]

@pytest.fixture
def mockfs(monkeypatch):
    fs = MockFS()
    monkeypatch.setattr(__builtin__, "open", fs.open)
    return fs
