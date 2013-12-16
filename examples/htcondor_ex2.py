#!/usr/bin/env python
from htcondor_dag import job, autorun

# Two jobs write a python value to their output file; the
# third job waits for these jobs to complete, reads their values
# and writes text output.
   
@job(request_memory=200,output="result.txt")
def print_sum(a, b):
    print a + b

@job(request_memory=100)
def adder(a, b):
    return a + b
    
autorun()
   
j1 = adder.queue(1, 2) 
j2 = adder.queue(3, 4)
j3 = print_sum.queue(j1, j2)
