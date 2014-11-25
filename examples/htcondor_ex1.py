#!/usr/bin/env python
from htcondor_dag import Dag, autorun

# Two jobs, each writes text to its output file
   
def print_sum(a, b):
    print a + b

autorun(report_hostname=True)

dag = Dag('htcondor_ex1')
dag.defer(print_sum, output="res1.txt")(1, 2)
dag.defer(print_sum, output="res2.txt")(3, 4)
dag.write()
