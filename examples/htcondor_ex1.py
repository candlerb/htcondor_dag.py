#!/usr/bin/env python
from htcondor import job, autorun, dag

# Two jobs, each writes text to its output file
   
@job
def print_sum(a, b):
    print a + b

autorun()
   
print_sum.queue(1, 2) 
print_sum.queue(3, 4)
