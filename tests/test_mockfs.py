import pytest

def test_read_nonexistent(mockfs):
    with pytest.raises(KeyError):
        open("/abc/foo")
    assert "/abc/foo" not in mockfs.files

def test_write(mockfs):
    f = open("/abc/foo", "w")
    f.write("hello world")
    f.close()
    assert mockfs["/abc/foo"] == "hello world"
    assert "/abc/foo" in mockfs.files

def test_write_with(mockfs):
    with open("/abc/foo", "w") as f:
        f.write("hello again")
    assert mockfs["/abc/foo"] == "hello again"
