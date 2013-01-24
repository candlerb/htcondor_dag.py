#!/usr/bin/env python
from htcondor import job, autorun, procid

# First we run a cluster of jobs, each of which returns a value.
# Then we run another job which prints all the results from the cluster.
   
@job
def adder(a, b):
    return a + b

@job
def printer(v):
    print repr(v)

autorun()

j1 = adder.job(processes=10).queue(procid, 5)
printer.job(output="result.txt").queue(j1)
