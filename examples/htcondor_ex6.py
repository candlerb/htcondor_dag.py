#!/usr/bin/env python
import subprocess
from htcondor_dag import Dag, autorun

def bash(cmd):
    subprocess.check_call(["/bin/bash","-c","set -o pipefail; " + cmd])

def bash2(cmd):
    subprocess.check_call(["/bin/bash","-c","set -o pipefail; " + cmd])

autorun()

dag = Dag("htcondor_ex6")
diamond = dag.dag(id="DIAMOND", filename="diamond.dag")

d_bash = dag.defer(bash, output=None, retry=1)
d_bash2 = diamond.defer(bash2, output=None, retry=1)

# http://research.cs.wisc.edu/htcondor/manual/v7.8/2_10DAGMan_Applications.html#SECTION003107900000000000000

a = d_bash2("echo A")
b = d_bash2("echo B")
c = d_bash2("echo C")
d = d_bash2("echo D")
a.child(b,c)
d.parent(b,c)

# splice into the normal top-level dag
x = d_bash("echo X")
y = d_bash("echo Y")

x.child(diamond)
y.parent(diamond)

dag.write()
