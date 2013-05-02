#!/usr/bin/env python
import subprocess
from htcondor import job, autorun, dag

@job(output=None)
def bash(cmd):
    subprocess.check_call(["/bin/bash","-c","set -o pipefail; " + cmd])

diamond = dag.new_dag(id="DIAMOND", filename="diamond.dag")
@diamond.job(output=None)
def bash2(cmd):
    subprocess.check_call(["/bin/bash","-c","set -o pipefail; " + cmd])

autorun()

# http://research.cs.wisc.edu/htcondor/manual/v7.8/2_10DAGMan_Applications.html#SECTION003107900000000000000

a = bash2.queue("echo A")
b = bash2.queue("echo B")
c = bash2.queue("echo C")
d = bash2.queue("echo D")
a.child(b,c)
d.parent(b,c)

# splice into the normal top-level dag
x = bash.queue("echo X")
y = bash.queue("echo Y")

x.child(diamond)
diamond.child(y)
