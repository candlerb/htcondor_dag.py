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

~~~{.python}
from htcondor import job, autorun

@job
def print_sum(a, b):
    print a + b

autorun()   # at point where all functions have been defined

print_sum.queue(1, 2)
print_sum.queue(3, 4)
~~~

Make the script executable, and run it to create the DAG (this also creates
the input file(s) for the jobs):

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

Monitor progress using `tail -f mytest.dag.dagman.out`, and
`condor_q -run -dag`

The output will be written to files `mytest.print_sum_0.out` and
`mytest.print_sum_1.out`.

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
You need to re-run the same script which generates the dag (to ensure all
the relevant classes are defined) but with this environment variable set.

~~~
UNPICKLE="mytest.in" ./mytest.py
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

Pre-existing functions
----------------------

If the function you want to queue is defined in a different file, you can
decorate it without modifying the source file by calling `job(func, **opts)`

~~~
from htcondor import job, autorun
import foo

job(foo.bar, request_memory=1024)
autorun()

foo.bar.queue('/tmp/foo','/tmp/bar')
~~~

DAG features
============

The return value of queue(...) is a Job object instance. You can call
methods on this instance to alter the attributes of the job.

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

These can be set either as defaults on a function:

~~~
@job(state="Wisconsin",country="US")
def a(...):
   ...
~~~

Or for individual job instances, which can either add to or override the
defaults.

~~~
a.queue(...).var(state="Wisconsin",country="US")
~~~

There is some support for converting dicts or lists to macro values:

~~~
job_a.var(environment={"PATH":"/usr/bin","TERMCAP":"vt100"})
job_b.var(arguments=["foo","bar","baz"])
~~~

However, for htcondor up to at least v7.8.7, dagman [does not allow the use
of single quotes within VARS](https://lists.cs.wisc.edu/archive/htcondor-users/2013-January/msg00067.shtml),
which means that values containing spaces cannot be quoted properly.

Categories
----------

Jobs can be assigned to categories, and you can limit the number of
jobs which will run concurrently in a particular category.

~~~
from htcondor import job, autorun, dag

@job(category="adder")
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
@job(request_memory=100)
def myjob(...):
    ....

myjob.queue(...)
~~~

To suppress generation of output from a job (e.g. one which writes
all its output to a shared filesystem)

~~~
@job(output=None)
def myjob(...):
    ....
~~~

Options can also be set on an individual instance:

~~~
myjob.queue(...).var(request_memory=100,output="result.txt")
~~~

Defaults for all jobs
---------------------

To set default VARS for every job, edit the .sub file to contain them.

You can point any job to another submit file:

~~~
@job(submit='foo.sub')
def foo(...):
    ....
~~~

There is also a helper object which can write submit files for you.

~~~
s = Submit(filename="foo.sub", request_memory=1024)
@job(submit=s)
def foo(...):
    ....
~~~

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
print_sum.queue(1,2).var(processes=10)
~~~

You can pass the sentinel value `htcondor.procid` as an argument, and this
is expanded at run-time to the process number, between 0 and N-1.

~~~
from htcondor import procid

print_sum.queue(procid, 5).var(processes=10)   # outputs 5 to 14 inclusive
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

* Pre and Post scripts
* Other job and subdag attributes
* Test suite
* Make a local execution environment using multiprocess.Pool
* We could simplify DAG if the submit file had
    input=htcondor.in
    output=htcondor.$(jobname).out
    error=htcondor.$(jobname).err
  but that would mean having to parse the existing submit file
