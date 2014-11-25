#!/usr/bin/env python
from htcondor_dag import Dag, autorun, procid

# First we run a cluster of jobs, each of which returns a value.
# Then we run another job which prints all the results from the cluster.
   
def adder(a, b):
    return a + b

def printer(v):
    print repr(v)

autorun()

dag = Dag("htcondor_ex3")

j1 = dag.defer(adder, processes=10)(procid, 5)
dag.defer(printer, output="result.txt")(j1)

dag.write()
