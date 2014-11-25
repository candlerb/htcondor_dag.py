import htcondor_dag
import pytest

def test_job_getitem():
    """
    job[attr] gets the variable from the job, or if unset falls back to the
    variable in the submit file
    """
    foo = htcondor_dag.Submit("foo.sub", request_memory=123)
    j0 = htcondor_dag.Job("j0")
    j1 = htcondor_dag.Job("j1", submit="xyz.sub", output="j1.out")
    j2 = htcondor_dag.Job("j2", submit=foo, output="j2.out", processes=5)
    j3 = htcondor_dag.Job("j3", submit="xyz.sub", request_memory=456)
    j4 = htcondor_dag.Job("j4", submit=foo, request_memory=789)

    # (Question: should submit be an attribute or just one of vars?)
    assert j0.submit == "j0.sub"
    assert j1.submit == "xyz.sub"
    assert j2.submit == foo
    assert j3.submit == "xyz.sub"
    assert j4.submit == foo

    with pytest.raises(KeyError):
        j0["request_memory"]
    with pytest.raises(KeyError):
        j1["request_memory"]
    assert j2["request_memory"] == 123
    assert j3["request_memory"] == 456
    assert j4["request_memory"] == 789

    with pytest.raises(KeyError):
        j0["output"]
    assert j1["output"] == "j1.out"
    assert j2["output"] == "j2.out.$(process)"

    # (Question: submit file has output=True ---> $(job).out ?)
