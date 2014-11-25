import htcondor_dag

dag = htcondor_dag.Dag("dec")

@dag.defer
def foo(a):
    return a*3

j1 = foo(10)
j2 = foo(20)

def test_decorate_plain(mockfs):
    assert dag.nodes == [j1, j2]

    din = dag.input.data["foo_0"]
    assert din[0](100) == 300
    assert din[1] == (10,)
    assert din[2] == {}

    din = dag.input.data["foo_1"]
    assert din[0](100) == 300
    assert din[1] == (20,)
    assert din[2] == {}

    #OOPS: this doesn't work because the real (inner) foo function is
    #now anonymous, and we can't pickle it.

    #dag.write()
