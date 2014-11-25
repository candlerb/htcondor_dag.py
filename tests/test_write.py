import cPickle

def foo(a): pass

def test_write_simple_dag(dag, mockfs):
    dag.job("foo", "foo.sub")
    dag.job("bar", "xyz.sub", input="wibble.txt", request_memory=123, retry=2)
    dag.write()

    assert mockfs["test.dag"] == """
JOB foo foo.sub

JOB bar xyz.sub
RETRY bar 2
VARS bar input="wibble.txt" request_memory="123"
"""

    # Neither of these jobs needed to write the dag input file
    assert "test.in" not in mockfs.files
    assert "test.sub" not in mockfs.files

def test_write_dag_with_default_submit(dag, mockfs):
    """
    If you don't specify a submit file, then the dag's default
    submit file is used
    """
    dag.job("qux")
    dag.write()

    assert mockfs["test.dag"] == """
JOB qux test.sub
"""

    assert "test.in" not in mockfs.files
    assert "test.sub" in mockfs.files

def test_write_processes(dag, mockfs):
    dag.job("foo", "foo.sub", output="test.foo.out", processes=5)
    dag.job("bar", "xyz.sub", input="wibble.txt", error="test.bar.err", processes=10)
    dag.job("qux", "qux.sub", input="test.qux.in", processes=15)
    dag.write()

    assert mockfs["test.dag"] == """
JOB foo foo.sub
VARS foo output="test.foo.out.$(process)" processes="5"

JOB bar xyz.sub
VARS bar error="test.bar.err.$(process)" input="wibble.txt" processes="10"

JOB qux qux.sub
VARS qux input="test.qux.in" processes="15"
"""

def test_defer(dag, mockfs):

    dag.defer(foo)(100)
    dag.defer(foo, request_memory=123, retry=2)(a=200)
    dag.write()

    assert mockfs["test.dag"] == """
JOB foo_0 test.sub
VARS foo_0 error="test.foo_0.err" input="test.in" output="test.foo_0.out"

JOB foo_1 test.sub
RETRY foo_1 2
VARS foo_1 error="test.foo_1.err" input="test.in" output="test.foo_1.out" request_memory="123"
"""

    args = cPickle.loads(mockfs["test.in"])
    assert args == {
        "foo_0": (foo, (100,), {}),
        "foo_1": (foo, (), {"a":200}),
    }

    assert "executable =" in mockfs["test.sub"]
    assert "transfer_input_files =" in mockfs["test.sub"]
    assert "universe = vanilla" in mockfs["test.sub"]
    assert "queue $(processes)" in mockfs["test.sub"]

def test_defer_input_output_error_none(dag, mockfs):

    # Note: setting input=None forces a separate input file to be written,
    # named after the DAG node (the function+args must go somewhere!)
    dag.defer(foo, input=None)(100)
    dag.defer(foo, output=None)(200)
    dag.defer(foo, error=None)(300)
    dag.write()

    assert mockfs["test.dag"] == """
JOB foo_0 test.sub
VARS foo_0 error="test.foo_0.err" input="test.foo_0.in" output="test.foo_0.out"

JOB foo_1 test.sub
VARS foo_1 error="test.foo_1.err" input="test.in"

JOB foo_2 test.sub
VARS foo_2 input="test.in" output="test.foo_2.out"
"""

def test_dir(dag, mockfs):

    dag.defer(foo, noop=True)(100)
    dag.defer(foo, dir='wibble')(200)
    dag.defer(foo, noop=True, dir='bibble')(300)
    dag.write()

    assert mockfs["test.dag"] == """
JOB foo_0 test.sub NOOP
VARS foo_0 error="test.foo_0.err" input="test.in" output="test.foo_0.out"

JOB foo_1 test.sub DIR wibble
VARS foo_1 error="test.foo_1.err" input="test.in" output="test.foo_1.out"

JOB foo_2 test.sub DIR bibble NOOP
VARS foo_2 error="test.foo_2.err" input="test.in" output="test.foo_2.out"
"""
