#!/usr/bin/env python
from htcondor import job, autorun, dag

# Two jobs, each writes text to its output file
   
@job
def print_sum(a, b):
    print a + b

autorun(report_hostname=True)
   
print_sum.queue(1, 2).var(output="res1.txt")
print_sum.queue(3, 4).var(output="res2.txt")
