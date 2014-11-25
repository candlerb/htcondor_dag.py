import htcondor_dag

def test_empty(dag):
    assert len(dag.nodes) == 0
    assert dag.filename == "test.dag"
    assert dag.input.filename == "test.in"
    assert dag.submit.filename == "test.sub"

def test_str(dag):
    assert str(dag) == dag.filename
    assert str(dag.input) == dag.input.filename
    assert str(dag.submit) == dag.submit.filename

    job = htcondor_dag.Job("j0")
    assert str(job) == "j0"

def test_simple_jobs(dag):
    dag.job("foo", submit="foo.sub")
    dag.job("bar", submit="xyz.sub", request_memory=123)

    assert [x.id for x in dag.nodes] == ['foo', 'bar']
    assert [x.submit for x in dag.nodes] == ['foo.sub', 'xyz.sub']
    assert 'request_memory' not in dag.nodes[0].vars
    assert dag.nodes[1].vars['request_memory'] == 123
