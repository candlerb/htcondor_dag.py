try:
    import cPickle as pickle
except:
    import pickle
import htcondor_dag

def adder(a,b): return a+b
def print_sum(a,b): print a+b

def test_chain(dag, mockfs):
    fn = dag.defer(adder, request_memory=100)
    j1 = fn(1,2)
    j2 = fn(3,4)
    j3 = dag.defer(print_sum, request_memory=200)(j1, j2)

    assert (j3 in j1.children or j1 in j3.parents)
    assert (j3 in j2.children or j2 in j3.parents)

    dag.write()

    # Note that print_sum_0 has its own private input file
    # (print_sum_0.in) because it can't be unpickled until
    # adder_0 and adder_1 have written their output files;
    # and also input_files lists the input files required

    assert mockfs["test.dag"] == """
JOB adder_0 test.sub
VARS adder_0 error="test.adder_0.err" input="test.in" output="test.adder_0.out" request_memory="100"

JOB adder_1 test.sub
VARS adder_1 error="test.adder_1.err" input="test.in" output="test.adder_1.out" request_memory="100"

JOB print_sum_0 test.sub
VARS print_sum_0 error="test.print_sum_0.err" input="test.print_sum_0.in" input_files="test.adder_0.out,test.adder_1.out" output="test.print_sum_0.out" request_memory="200"
PARENT adder_0 adder_1 CHILD print_sum_0
"""

    args = pickle.loads(mockfs["test.in"])
    assert args == {
        "adder_0": (adder, (1,2), {}),
        "adder_1": (adder, (3,4), {}),
    }

    args = pickle.loads(mockfs["test.print_sum_0.in"])
    assert args["print_sum_0"][0] == print_sum
    assert isinstance(args["print_sum_0"][1][0], htcondor_dag.Job)
    assert isinstance(args["print_sum_0"][1][1], htcondor_dag.Job)
    assert args["print_sum_0"][1][0].id == "adder_0"
    assert args["print_sum_0"][1][1].id == "adder_1"
    assert args["print_sum_0"][2] == {}
