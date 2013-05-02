#!/usr/bin/env python
from htcondor import dag

# This is the simple diamond-shaped DAG example
a = dag.new_job('A','A.condor',comment="This is node A")
b = dag.new_job('B','B.condor')
c = dag.new_job('C','C.condor')
d = dag.new_job('D','D.condor')
a.child(b,c)
d.parent(b,c)
dag.write()
