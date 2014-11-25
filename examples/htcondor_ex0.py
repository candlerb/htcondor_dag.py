#!/usr/bin/env python
from htcondor_dag import Dag

# This is the simple diamond-shaped DAG example
dag = Dag('htcondor_ex0')
a = dag.job('A','A.condor',comment="This is node A")
b = dag.job('B','B.condor')
c = dag.job('C','C.condor')
d = dag.job('D','D.condor')
a.child(b,c)
d.parent(b,c)
dag.write()
