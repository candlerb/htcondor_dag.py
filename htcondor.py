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

class Job(object):
    def __init__(self, id, func, submit="htcondor.sub", filebase="tmp", dag=sys.stdout,
                 input=True, output=True, error=True,
                 processes=None, category=None, **vars):
        self.id = id
        self.func = func
        self.submit = submit
        self.dag = dag
        self.processes = processes
        if input is True:
            input = "%s.%s.in" % (filebase, id)
        if output is True:
            if processes is None:
                output = "%s.%s.out" % (filebase, id)
            else:
                output = "%s.%s.$(process).out" % (filebase, id)
        if error is True:
            if processes is None:
                error = "%s.%s.err" % (filebase, id)
            else:
                error = "%s.%s.$(process).err" % (filebase, id)
        self.input = input
        self.output = output
        self.error = error
        print("\nJOB %s %s" % (self, self.submit), file=self.dag)
        self.category(category)
        self.vars(input=input, output=output, error=error,
                  processes=processes, **vars)
    
    def __str__(self):
        return self.id
    
    def __repr__(self):
        return "Job %s >%s" % (self.id, self.output)
    
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
            self.vars(job_files=",".join(job_files))
            self.parent(*_curr_job_parents)
        return self

    def vars(self, **kwargs):
        """
        Output macros which are to be passed to this job submission: e.g.
            myjob.vars(foo="bar", bar="qux")
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
            res = '%s %s="%s"' % (res, k, v)
        if res:
            print('VARS %s%s' % (self, res), file=self.dag)
        return self

    def category(self, category):
        """
        Set the category for this job, so you can configure MAXJOBS
        instances of each job category to run concurrently
        """
        if category is not None:
            print("CATEGORY %s %s" % (self, category), file=self.dag)
        return self

    def parent(self, *other):
        """
        List other jobs which are parents (predecessors) of this one.
        You may pass either Job instances or strings containing jobids.
        """
        if other:
            print("PARENT %s CHILD %s" %
                  (" ".join([str(o) for o in other]), self), file=self.dag)
        return self
    
    def child(self, *other):
        """
        List other jobs which are children of this one,
        You may pass either Job instances or strings containing jobids.
        """
        if other:
            print("PARENT %s CHILD %s" %
                  (self, " ".join([str(o) for o in other])), file=self.dag)
        return self

    def __reduce__(self):
        """
        If this job is used as an argument to another job, then at depickle
        time we need to read the file(s) created by this job. Also add
        parents to the job currently being written.
        """
        _curr_job_parents.add(self)
        return (read_job_output, (self.id, self.output, self.processes))

class Dag(object):
    """
    A Dag is a collection of jobs. It also allocates job ids, and holds
    default options common to every job (i.e. acts as a job factory).
    """
    def __init__(self, **defaults):
        self.defaults = defaults
        self.jobs = []
        self.last_id = {}           # id_prefix => sequence number
        self.job = self.job_vars()  # simple decorator

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
        self.jobs.append(job)
        return job
    
    def job_vars(self, **options):
        """
        This is a parameterised decorator.
        
        @dag.job_vars(request_memory=100)
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

    def maxjobs(self, category, n):
        print("MAXJOBS %s %d" % (category, n),
              file=self.defaults.get('dag', sys.stdout))

# Most users need only a single DAG
dag = Dag(filebase=re.sub(r'\.pyc?$', '', os.path.basename(sys.argv[0])) + '.job',
          executable=sys.argv[0])
job = dag.job
job_vars = dag.job_vars
defaults = dag.defaults

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

# Another option is to set Executable = htcondor.py in submit file.
# For this to work, when you write the dag file the functions must have
# been imported from a different module, not implicitly __main__
if __name__ == '__main__':
    run()
    sys.exit(0)

