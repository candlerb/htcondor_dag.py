#!/usr/bin/env python

# htcondor.py: distributed python using a HTCondor DAG
# Copyright (C) 2013 Brian Candler
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# version 2 as published by the Free Software Foundation.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# Heavily borrows from http://flask.pocoo.org/snippets/73/ which is
# in the public domain
#
# At minimum you will need a htcondor.sub which looks like this:
#
#   universe = vanilla
#   transfer_input_files = $(job_files)
#   queue $(processes)

from __future__ import print_function
import sys
import os
import re
import sets
import pickle

pickle_protocol = 0 # (or for binary) pickle.HIGHEST_PROTOCOL

############################################################
#
# Tools for writing a DAG file of queued function calls
#
############################################################

_curr_job_parents = sets.Set()

def output_files(filename, processes=None):
    return [re.sub(r'\$\(process\)',str(p),filename,flags=re.IGNORECASE)
            for p in range(processes or 1)]

class Node(object):
    def __init__(self, id, filename):
        self.id = id
        self.filename = filename
        self.parents = []
        self.children = []
    
    def __str__(self):
        return self.id

    def parent(self, *other):
        self.parents.extend(other)
        return self
    
    def child(self, *other):
        self.children.extend(other)
        return self

    def write_dag_entry(self, dag):
        if self.parents:
            print("PARENT %s CHILD %s" %
                  (" ".join([str(o) for o in self.parents]), self), file=dag)
        if self.children:
            print("PARENT %s CHILD %s" %
                  (self, " ".join([str(o) for o in self.children])), file=dag)
        print("", file=dag)
    
class Job(Node):
    def __init__(self, id, submit="htcondor.sub", func=None,
                 category=None, priority=None, retry=None,
                 input=True, output=True, error=True, processes=None,
                 filebase="tmp", **vars):
        super(Job, self).__init__(id=id, filename=submit)
        self.func = func
        self.category = category
        self.priority = priority
        self.retry = retry
        self.input = input
        self.output = output
        self.error = error
        self.processes = processes
        if input is True:
            self.input = "%s.job.%s.in" % (filebase, id)
        if output is True:
            if self.processes is None:
                self.output = "%s.job.%s.out" % (filebase, id)
            else:
                self.output = "%s.job.%s.$(process).out" % (filebase, id)
        if error is True:
            if self.processes is None:
                self.error = "%s.job.%s.err" % (filebase, id)
            else:
                self.error = "%s.job.%s.$(process).err" % (filebase, id)
        self.vars = vars
    
    def __repr__(self):
        return "Job %s >%s" % (self.id, self.output)
    
    def write_dag_entry(self, dag):
        print("JOB %s %s" % (self, self.filename), file=dag)
        if self.category is not None:
            print("CATEGORY %s %s" % (self, self.category), file=dag)
        if self.priority is not None:
            print("PRIORITY %s %d" % (self, self.priority), file=dag)
        if self.retry is not None:
            print("RETRY %s %d" % (self, self.retry), file=dag)
        self.write_vars(dag, input=self.input, output=self.output, error=self.error,
                        processes=self.processes, **self.vars)
        super(Job, self).write_dag_entry(dag)
        if self.input and not os.path.exists(self.input):
            print("WARNING: %s: input file %s does not exist" % (self, self.input))

    def set_vars(self, **v):
        self.vars.update(v)
    
    def write_vars(self, dag, **kwargs):
        """
        Output macros which are to be passed to this job submission: e.g.
            myjob.set_vars(foo="bar", bar="qux")
        """
        res = ''
        for (k,v) in sorted(kwargs.iteritems()):
            if re.match('queue', k, flags=re.IGNORECASE):
                raise ValueError('macroname must not start with "queue"')
            elif v is None:
                continue
            elif hasattr(v, 'iteritems'):
                # e.g. environment={"PATH":"/usr/bin","HOME":"/home/job"}
                v = " ".join(["%s=%s" % (x,re.sub("[ ']", '_', y))
                              for (x,y) in v.iteritems()])
                # Not yet permitted by DAGMAN:
                #v = " ".join(["%s='%s'" % (x,y.replace("'","''")
                #              for (x,y) in v.iteritems()])
                #v = '"%s"' % v
            elif type(v) is list:
                # e.g. arguments=["foo", "bar", "baz"]
                v = " ".join([re.sub("[ ']", '_', y) for y in v])
                # Not yet permitted by DAGMAN:
                #v = " ".join(["'%s'" % y.replace("'","''") for y in v])
                #v = '"%s"' % v
            v = str(v).replace('\\','\\\\').replace('"','\\"')
            res += ' %s="%s"' % (k, v)
        if res:
            print('VARS %s%s' % (self, res), file=dag)
        return self

    def queue(self, *args, **kwargs):
        """
        The primary function: write out a JOB to be executed by htcondor.
        If its arguments are other jobs, add the dependencies.
        """
        _curr_job_parents.clear()
        with open(self.input, 'wb') as f:
            pickle.dump({'func':self.func, 'args':args, 'kwargs':kwargs}, f, pickle_protocol)
        if _curr_job_parents:
            job_files = []
            for j in _curr_job_parents:
                job_files.extend(output_files(j.output, j.processes))
            self.set_vars(job_files=",".join(job_files))
            self.parent(*_curr_job_parents)
        return self

    def __reduce__(self):
        """
        If this job is used as an argument to another job, then at depickle
        time we need to read the file(s) created by this job. Also add
        parents to the job currently being written.
        """
        _curr_job_parents.add(self)
        return (read_job_output, (self.id, self.output, self.processes))

class Dag(Node):
    """
    A Dag is a collection of jobs. It also allocates job ids, and holds
    default options common to every job (i.e. acts as a job factory).
    """
    def __init__(self, id, filename, dir=None, **defaults):
        super(Dag, self).__init__(id=id, filename=filename)
        self.dir = dir
        self.defaults = defaults
        self.nodes = []
        self.last_id = {}           # id_prefix => sequence number
        self.maxjobs = {}           # category => limit
        self.job = self.job_with()  # simple decorator

    def write(self):
        with open(self.filename, "w") as f:
            for node in self.nodes:
                node.write_dag_entry(f)
            for (k,v) in self.maxjobs.iteritems():
                print("MAXJOBS %s %d" % (k,v), file=f)

    def write_dag_entry(self, dag):
        self.write()
        if self.dir:
            print("SPLICE %s %s DIR %s" % (self.id, self.filename, self.dir), file=dag)
        else:
            print("SPLICE %s %s" % (self.id, self.filename), file=dag)
        super(Dag, self).write_dag_entry(dag)

    def add_node(self, node):
        """
        Add a node - could be a Job or a sub-Dag
        """
        self.nodes.append(node)

    def create_job(self, id=None, id_prefix="", **options):
        if id is None:
            if id_prefix in self.last_id:
                self.last_id[id_prefix] += 1
            else:
                self.last_id[id_prefix] = 0
            id = "%s%d" % (id_prefix, self.last_id[id_prefix])
        opt = self.defaults.copy()
        opt.update(options)
        job = Job(id=id, **opt)
        self.add_node(job)
        return job
    
    def job_with(self, **options):
        """
        This is a parameterised decorator.
        
        @dag.job_with(request_memory=100)
        def myjob(args):
            ....
        
        myjob.queue(...)
        """
        def decorate(func):
            def queue(*args, **kwargs):
                return self.create_job(func=func, **options).queue(*args, **kwargs)
            def job(**opt):
                o = options.copy()
                o.update(opt)
                return self.create_job(func=func, **o)
            func.queue = queue
            func.job = job
            return func
        return decorate

# Most users need only a single DAG
base = re.sub(r'\.pyc?$', '', os.path.basename(sys.argv[0]))
dag = Dag(id=base, filename="%s.dag" % base, filebase=base, executable=sys.argv[0])
job = dag.job
job_with = dag.job_with
defaults = dag.defaults

def write_dag():
    """
    Write out the default dag
    """
    if dag.nodes:
        dag.write()
        print("Written DAG file to %s" % dag.filename, file=sys.stderr)

class Ad(object):
    """An object which represents a run-time value of a classAd attribute.
       It is replaced with the actual value when the job is unpickled"""
    def __init__(self, attr, env='_CONDOR_JOB_AD'):
        self.attr = attr
        self.env = env
    
    def __repr__(self):
        return "%s[%s]" % (self.env, self.attr)

    def __reduce__(self):
        return (ad_attr, (self.attr, self.env))

def MachineAd(attr):
    return Ad(attr, '_CONDOR_MACHINE_AD')

procid = Ad('ProcId')

############################################################
#
# Tools for invoking a function when htcondor job runs
#
############################################################

def running():
    """Return true if running as a htcondor job"""
    return '_CONDOR_JOB_AD' in os.environ

def parse_ad(filename):
    """Read a classAd-formatted file and return a dict of {attr:val}"""
    ad = {}
    with open(filename) as f:
        for line in f:
            m = re.match('^(\w+)\s*=\s*"(.*)"$', line)
            if m:
                ad[m.group(1)] = m.group(2)  # TODO: dequote internal \" ?
                continue
            m = re.match('^(\w+)\s*=\s*(\d+)$', line)
            if m:
                ad[m.group(1)] = int(m.group(2))
                continue
            m = re.match('^(\w+)\s*=\s*(.*)$', line)
            if m:
                ad[m.group(1)] = m.group(2)
                continue
    return ad

ads = {}

def ad_attr(attr, env):
    """Return a single classAd value"""
    if running():
        if env not in ads:
            ads[env] = parse_ad(os.environ[env])
        return ads[env][attr]
    else:
        return Ad(attr, env)

def read_job_output(id, filename, processes=None):
    """
    If job B uses the value of job A in its arguments, we have to read that
    value at runtime. This is done when job B's arguments are unpickled.
    If the job was a cluster, return all the values as a list.
    """
    if running():
        if filename is None:
            return None
        elif processes is None:
            with open(output_files(filename)[0], 'rb') as f:
                return pickle.load(f)
        else:
            res = []
            for fn in output_files(filename, processes):
                with open(fn, 'rb') as f:
                    res.append(pickle.load(f))
            return res
    else:
        return Job(id=id, output=filename, processes=processes)

def invoke(job_data):
    """
    Run a job, passing in the de-pickled argument set.
    If an exception is raised then the default python behaviour is to
    write a backtrace to stderr and exit with a non-zero code, which is
    what we want for htcondor.
    """
    func = job_data['func']
    args = job_data['args']
    kwargs = job_data['kwargs']

    return func(*args, **kwargs)

def run(src=sys.stdin, dst=sys.stdout, output_none=False):
    if src.isatty():
        print('%s is non-interactive, requires a pickled argument set' % sys.argv[0], file=sys.stderr)
        sys.exit(1)
    else:
        res = invoke(pickle.load(src))
        if res is not None or output_none:
            pickle.dump(res, dst, pickle_protocol)

def autorun(*args, **kwargs):
    """
    Call this in your application after you have defined your functions,
    but before deciding what functions to queue. Then if the script is called
    as a htcondor job, it will just invoke the desired function.
    """
    if running():
        run(*args, **kwargs)
        sys.exit(0)
    elif 'UNPICKLE' in os.environ:
        print(repr(pickle.load(open(os.environ['UNPICKLE'], 'rb'))))
        sys.exit(0)
    import atexit
    atexit.register(write_dag)

# Another option is to set Executable = htcondor.py in submit file.
# For this to work, when you write the dag file the functions must have
# been imported from a different module, not implicitly __main__
if __name__ == '__main__':
    run()
    sys.exit(0)

