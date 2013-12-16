#!/usr/bin/env python
from htcondor_dag import job, autorun, procid

# First we run a cluster of jobs, each of which returns a value.
# Then we run another job which prints all the results from the cluster.
   
@job
def adder(a, b):
    return a + b

@job(output="result.txt")
def printer(v):
    print repr(v)

autorun()

j1 = adder.queue(procid, 5).processes(10)
printer.queue(j1)
