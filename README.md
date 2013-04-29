Overview
========

htcondor.py turns python functions into
[HTCondor](http://research.cs.wisc.edu/htcondor/) jobs.  It writes out a
DAG (Directed Acyclic Graph) defining the individual jobs and their dependencies, ready for
[submission] (http://research.cs.wisc.edu/htcondor/manual/current/condor_submit_dag.html)
to [dagman](http://research.cs.wisc.edu/htcondor/manual/v7.8/2_10DAGMan_Applications.html)
which schedules their execution across a cluster of compute nodes.

Basic operation
===============

This example runs two instances of a job in parallel, with different
arguments.

~~~
@job
def print_sum(a, b):
    print a + b

autorun()

print_sum.queue(1, 2)
print_sum.queue(3, 4)
~~~

Make the script executable, and run it to create the DAG (this also creates
the input files to each job):

~~~
./mytest.py
~~~

You will need to create a minimal `htcondor.sub` like this:

~~~
universe = vanilla
transfer_input_files = $(job_files)
queue $(processes)
~~~

Finally, run the DAG:

~~~
condor_submit_dag mytest.dag
~~~

Monitor progress using `tail -f mytest.dag.dagman.out`.  The output will be
written to files `mytest.job.0.out` and `mytest.job.1.out`.

Environment
-----------

`htcondor.py` (or at least the bits used by htcondor.autorun) needs to be
available when the job runs.  You could install it on all the target nodes,
but a simpler approach is to get htcondor to copy it for you, by changing
the htcondor.sub file:

~~~
transfer_input_files = htcondor.py,$(job_files)
~~~

If your python app is split into modules then you can transfer them all
together in a zipfile:

~~~
transfer_input_files = htcondor.py,mylib.zip,$(job_files)
environment = "PYTHONPATH=mylib.zip"
~~~

Examining file contents
-----------------------

Set the magic environment variable UNPICKLE to examine any of the htcondor
job input files, or output files from jobs which generate pickled output.

~~~
UNPICKLE="mytest.job.0.in" ./myprog.py
~~~

Shell jobs
----------

Although the examples so far have shown trivial computation, htcondor.py
also makes it very convenient to marshal collections of shell jobs and
define their dependencies.

~~~
#!/usr/bin/env python
import subprocess
from htcondor import job, autorun

@job
def bash(cmd):
    subprocess.check_call(["/bin/bash","-c","set -o pipefail; " + cmd])

autorun()

j1 = bash.queue("foo </nfs/i1 | bar >/nfs/o1")
j2 = bash.queue("foo </nfs/i2 | bar >/nfs/o2")
j3 = bash.queue("baz /nfs/o1 /nfs/o2 >/nfs/out").parent(j1, j2)
~~~

DAG features
============

Parent/child
------------

This is the [diamond-shaped DAG example](http://research.cs.wisc.edu/htcondor/manual/current/2_10DAGMan_Applications.html#SECTION003102000000000000000)

~~~
job_a = a.queue(...)
job_b = b.queue(...)
job_c = c.queue(...)
job_d = d.queue(...)
job_a.child(job_b,job_c)
job_d.parent(job_b,job_c)
~~~

Macros (VARS)
-------------

~~~
job_a.job(state="Wisconsin",country="US").queue(...)
~~~

There is some support for converting dicts or lists to macro values:

~~~
job_a.job(environment={"PATH":"/usr/bin","TERMCAP":"vt100"}).queue(...)
job_b.job(arguments=["foo","bar","baz"]).queue(...)
~~~

However, for htcondor up to at least v7.8.7, dagman [does not allow the use
of single quotes within VARS](https://lists.cs.wisc.edu/archive/htcondor-users/2013-January/msg00067.shtml),
which means that values containing spaces cannot be quoted properly.

Categories
----------

Jobs can be assigned to categories, and you can limit the number of
jobs which will run concurrently in a particular category.

~~~
from htcondor import job_with, autorun, dag

@job_with(category="adder")
def adder(a, b):
    return a + b

autorun()
dag.maxjobs["adder"] = 3

adder.queue(1, 2)
... etc
~~~

Job options
===========

Per-job options
---------------

On all instances of a job:

~~~
@job_with(request_memory=100, submit='foobar.sub')
def myjob(...):
    ....

myjob.queue(...)
~~~

To suppress generation of output from a job (e.g. one which writes
all its output to a shared filesystem)

~~~
@job_with(output=None)
def myjob(...):
    ....
~~~

Options can also be set on an individual instance:

~~~
myjob.job(request_memory=100,output="result.txt").queue(...)
~~~

Defaults for all jobs
---------------------

To set default VARS for every job:

    from htcondor import defaults
    defaults['request_memory'] = 200
    defaults['submit'] = 'myjob.sub'

(alternatively, you can edit the .sub file to include these parameters)

To write the DAG to a different file:

    from htcondor import dag
    dag.filename = "foo.dag"

Returning python values (experimental)
======================================

~~~
@job
def adder(a, b):
    return a + b
~~~

In this case, the output file will contain the pickled value. If the return
value is None then normally no output will be written.  You can force a
value of None to be written using `autorun(output_none=True)`

The values which are output from one job can be used as the input to another
job:

~~~
j1 = adder.queue(1, 2)
j2 = adder.queue(3, 4)
j3 = print_sum.queue(j1, j2)
~~~

Parent/child dependencies and job_files are set automatically, and at
runtime the arguments are expanded to the values written by the previous
jobs.

If a job is a cluster, the value is a list containing all the generated
job values in sequential order.

Job clusters
============

An HTCondor DAG node can submit a "cluster" of identical jobs:

~~~
print_sum.job(processes=10).queue(1, 2)
~~~

You can pass the sentinel value `htcondor.procid` as an argument, and this
is expanded at run-time to the process number, between 0 and N-1.

~~~
from htcondor import procid

print_sum.job(processes=10).queue(procid, 5)   # outputs 5 to 14 inclusive
~~~

However, you should note that if any one job in a cluster fails, htcondor
dagman will kill all the other jobs in that cluster - and so when you
resubmit the DAG, all the jobs in that cluster will restart from the
beginning.

If you want to be able to restart individual failed jobs, you need to submit
them as separate jobs, and if necessary declare the dependencies explicitly.

~~~
for i in range(10):
    print_sum.queue(i, 5)
~~~

TODO
====

* Try to show the DAG node name or the function name instead of the script
  name in condor_q output
* ? Collect all job arguments into a single file, to minimise the number
  of .in files we generate (then we have to select the job name at
  runtime, e.g. as a classAd attr or in arguments). But we will still end
  up with separate .err files per job
* Pre and Post scripts
* Test suite
