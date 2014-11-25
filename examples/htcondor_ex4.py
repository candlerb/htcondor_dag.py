#!/usr/bin/env python
from htcondor_dag import Dag, autorun

# Limit the number of concurrent jobs which run in a particular category,
# in this case only 3 at a time.
   
def adder(a, b):
    return a + b

autorun()
dag = Dag("htcondor_ex4")
dag.maxjobs["adder"] = 3

d_adder = dag.defer(adder, category="adder")
d_adder(1, 1)
d_adder(2, 2)
d_adder(3, 3)
d_adder(4, 4)
d_adder(5, 5)
d_adder(6, 6)
dag.write()
