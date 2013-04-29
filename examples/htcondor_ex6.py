#!/usr/bin/env python
import subprocess
from htcondor import job_with, autorun, Dag, dag

@job_with(output=None)
def bash(cmd):
    subprocess.check_call(["/bin/bash","-c","set -o pipefail; " + cmd])

autorun()

# http://research.cs.wisc.edu/htcondor/manual/v7.8/2_10DAGMan_Applications.html#SECTION003107900000000000000

diamond = Dag(id="DIAMOND", filename="diamond.dag")
a = diamond.create_job(id="A",func=bash).queue("echo A")
b = diamond.create_job(id="B",func=bash).queue("echo B")
c = diamond.create_job(id="C",func=bash).queue("echo C")
d = diamond.create_job(id="D",func=bash).queue("echo D")
a.child(b,c)
d.parent(b,c)

# splice into the normal top-level dag
x = dag.create_job(id="X",func=bash).queue("echo X")
y = dag.create_job(id="Y",func=bash).queue("echo Y")
dag.add_node(diamond)

x.child(diamond)
diamond.child(y)
