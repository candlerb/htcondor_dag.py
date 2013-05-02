#!/usr/bin/env python
from htcondor import job, autorun, dag

# Limit the number of concurrent jobs which run in a particular category,
# in this case only 3 at a time.
   
@job(category="adder")
def adder(a, b):
    return a + b

autorun()
dag.maxjobs["adder"] = 3

adder.queue(1, 1)
adder.queue(2, 2)
adder.queue(3, 3)
adder.queue(4, 4)
adder.queue(5, 5)
adder.queue(6, 6)
