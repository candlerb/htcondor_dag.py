Overview
========

htcondor_dag.py turns python functions into
[HTCondor](http://research.cs.wisc.edu/htcondor/) jobs.  It writes out a
DAG (Directed Acyclic Graph) defining the individual jobs and their dependencies, ready for
[submission] (http://research.cs.wisc.edu/htcondor/manual/current/condor_submit_dag.html)
to [dagman](http://research.cs.wisc.edu/htcondor/manual/current/2_10DAGMan_Applications.html)
which schedules their execution across a cluster of compute nodes.

Basic operation
===============

This example runs two instances of a job in parallel, with different
arguments.

~~~{.python}
from htcondor_dag import Dag, autorun

def print_sum(a, b):
    print a + b

autorun()   # at point where all functions have been defined

dag = Dag("mytest")
dag.defer(print_sum)(1, 2)
dag.defer(print_sum)(3, 4)
dag.write()
~~~

Make the script executable, and run it to create the DAG (this also creates
the input file(s) for the jobs and the submit file):

~~~{.bash}
./mytest.py
~~~

Finally, run the DAG:

~~~{.bash}
condor_submit_dag mytest.dag
~~~

Monitor progress using `tail -f mytest.dag.dagman.out`, and
`condor_q -run -dag`

The output will be written to files `mytest.print_sum_0.out` and
`mytest.print_sum_1.out`.

Environment
-----------

`htcondor_dag.py` (or at least the bits used by htcondor_dag.autorun) needs to be
available when the job runs.  You could install it on all the target nodes,
but the default approach is to get htcondor to copy it for you, since the
submit file includes:

~~~
transfer_input_files = /path/to/htcondor_dag.py,$(input_files)
~~~

If your python app is split into modules then you can transfer them all
together in a zipfile:

~~~
transfer_input_files = htcondor_dag.py,mylib.zip,$(input_files)
environment = "PYTHONPATH=mylib.zip"
~~~

which you can adjust programatically:

~~~
dag.submit.var(transfer_input_files=..., environment=...)
~~~

Examining file contents
-----------------------

Set the magic environment variable UNPICKLE to examine any of the htcondor
job input files, or output files from jobs which generate pickled output.
You need to re-run the same script which generates the dag (to ensure all
the relevant classes are defined) but with this environment variable set.

~~~{.bash}
UNPICKLE="mytest.in" ./mytest.py [jobid]
~~~

Shell jobs
----------

Although the examples so far have shown trivial computation, htcondor_dag.py
also makes it very convenient to marshal collections of shell jobs and
define their dependencies.

~~~{.python}
#!/usr/bin/env python
import subprocess
from htcondor_dag import Dag, autorun

def bash(cmd):
    subprocess.check_call(["/bin/bash","-c","set -o pipefail; " + cmd])

autorun()

dag = Dag("mytest")
j1 = dag.defer(bash)("foo </nfs/i1 | bar >/nfs/o1")
j2 = dag.defer(bash)("foo </nfs/i2 | bar >/nfs/o2")
j3 = dag.defer(bash)("baz /nfs/o1 /nfs/o2 >/nfs/out").parent(j1, j2)
dag.write()
~~~

DAG features
============

The return value of defer(func)(args) is a Job object instance. You can call
methods on this instance to alter the attributes of the job.

Parent/child
------------

This is the [diamond-shaped DAG example](http://research.cs.wisc.edu/htcondor/manual/current/2_10DAGMan_Applications.html#SECTION003102000000000000000)

~~~{.python}
job_a = dag.defer(a)(...)
job_b = dag.defer(b)(...)
job_c = dag.defer(c)(...)
job_d = dag.defer(d)(...)
job_a.child(job_b,job_c)
job_d.parent(job_b,job_c)
~~~

Macros (VARS)
-------------

These can be set either as defaults on a function:

~~~{.python}
def a(...):
   ...

defer_a = dag.defer(a, state="Wisconsin", country="US")
j1 = defer_a(...)
j2 = defer_a(...)
~~~

Or for individual job instances, which can either add to or override the
defaults.

~~~{.python}
j1 = defer_a(...).var(state="Wisconsin",country="US")
~~~

There is some support for converting dicts or lists to macro values:

~~~{.python}
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

~~~{.python}
from htcondor_dag import Dag, autorun

def adder(a, b):
    return a + b

autorun()
dag = Dag("mytest")
dag.maxjobs["adder"] = 3

dag.defer(adder, category="adder")(1, 2)
... etc
dag.write()
~~~

Job options
===========

Per-job options
---------------

To suppress generation of output from a job (e.g. one which writes
all its output to a shared filesystem)

~~~{.python}
dag.defer(myjob, output=None)(...)
~~~

Options can also be set on a job after it has been created:

~~~{.python}
dag.defer(myjob)(...).var(request_memory=100,output="result.txt")
~~~

Defaults for all jobs
---------------------

To set default VARS for every job, modify the .sub file to contain them.

~~~{.python}
dag.submit.var(request_memory=1000)
~~~

You can point any job to another submit file:

~~~{.python}
dag.defer(myjob, submit='foo.sub')(...)
~~~

There is also a helper object which can write submit files for you.

~~~{.python}
s = Submit(filename="foo.sub", request_memory=1024)
dag.defer(myjob, submit=s)(...)
~~~

Returning python values (experimental)
======================================

~~~{.python}
def adder(a, b):
    return a + b

dag.defer(adder)(1, 2)
~~~

In this case, the output file will contain the pickled value. If the return
value is None then normally no output will be written.  You can force a
value of None to be written using `autorun(output_none=True)`

The values which are output from one job can be used as the input to another
job:

~~~{.python}
j1 = dag.defer(adder)(1, 2)
j2 = dag.defer(adder)(3, 4)
j3 = dag.defer(print_sum)(j1, j2)
~~~

Parent/child dependencies and input_files are set automatically, and at
runtime the arguments are expanded to the values written by the previous
jobs.

If a job is a cluster, the value is a list containing all the generated
job values in sequential order.

Job clusters
============

An HTCondor DAG node can submit a "cluster" of identical jobs:

~~~{.python}
dag.defer(print_sum,processes=10)(1,2)
~~~

You can pass the sentinel value `htcondor_dag.procid` as an argument, and this
is expanded at run-time to the process number, between 0 and N-1.

~~~{.python}
from htcondor_dag import procid

dag.defer(print_sum, processes=10)(procid, 5)   # outputs 5 to 14 inclusive
~~~

However, you should note that if any one job in a cluster fails, htcondor
dagman will kill all the other jobs in that cluster - and so when you
resubmit the DAG, all the jobs in that cluster will restart from the
beginning.

If you want to be able to restart individual failed jobs, you need to submit
them as separate jobs, and if necessary declare the dependencies explicitly.

~~~{.python}
for i in range(10):
    dag.defer(print_sum)(i, 5)
~~~

TODO
====

* dag-level options (e.g. DOT)
* Make a local execution environment using multiprocess.Pool
* We could simplify DAG if the submit file had
    input=dagname.in
    output=dagname.$(jobname).out
    error=dagname.$(jobname).err
  but that would mean having to parse the existing submit file
