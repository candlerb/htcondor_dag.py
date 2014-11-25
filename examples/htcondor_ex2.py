#!/usr/bin/env python
from htcondor_dag import Dag, autorun

# Two jobs write a python value to their output file; the
# third job waits for these jobs to complete, reads their values
# and writes text output.


def print_sum(a, b):
    print a + b

def adder(a, b):
    return a + b

autorun()

dag = Dag('htcondor_ex2')
d_print_sum = dag.defer(print_sum, request_memory=200, output="result.txt")
d_adder = dag.defer(adder, request_memory=100)

j1 = d_adder(1, 2)
j2 = d_adder(3, 4)
j3 = d_print_sum(j1, j2)
dag.write()
