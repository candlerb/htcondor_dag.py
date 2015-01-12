#!/usr/bin/env python

# htcondor_dag.py: distributed python using a HTCondor DAG
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

from __future__ import print_function, absolute_import
import sys
import os
import re
import cPickle

pickle_protocol = cPickle.HIGHEST_PROTOCOL

def pypath(src):
    return re.sub(r'\.pyc$', '.py', os.path.abspath(src))

# These values are used for the default input file
DEFAULT_SUBMIT_VARS = {
    'universe': 'vanilla',
    'transfer_input_files': pypath(__file__)+',$(input_files)',
    'executable': pypath(sys.argv[0]),
}

############################################################
#
# Tools for writing a DAG file of queued function calls
#
############################################################

class Input(object):
    """
    An object which stores input arguments for one or more deferred calls
    """
    def __init__(self, filename):
        self.filename = filename
        self.data = {}           # {"jobname":(func,args,kwargs)}
        self.written = False

    def __repr__(self):
        return "Input(filename=%s,data=%s)" % (repr(self.filename),repr(self.data))

    def __str__(self):
        return self.filename

    def __unicode__(self):
        return self.filename

    def write(self):
        if self.data and not self.written:
            self.written = True
            with open(self.filename, "wb") as f:
                cPickle.dump(self.data, f, pickle_protocol)  # TODO: gzip

class Submit(object):
    """
    An object which writes out a submit file
    """

    def __init__(self, filename, **vars):
        self.filename = filename
        self.vars = vars
        self.written = False

    def __repr__(self):
        return "Submit(filename=%s,...)" % repr(self.filename)

    def __str__(self):
        return self.filename

    def __unicode__(self):
        return self.filename

    def var(self, **v):
        """Update one or more variables"""
        self.vars.update(v)
        return self

    def write(self):
        if not self.written:
            self.written = True
            if 'input' in self.vars and hasattr(self.vars['input'], 'write'):
                self.vars['input'].write()
            with open(self.filename, "w") as f:
                for (k,v) in sorted(self.vars.iteritems()):
                    if v is None:
                        continue
                    elif hasattr(v, 'iteritems'):
                        # e.g. environment={"PATH":"/usr/bin","HOME":"/home/job"}
                        v = " ".join(["%s='%s'" % (x,str(y).replace("'","''"))
                                      for (x,y) in v.iteritems()])
                        v = '"%s"' % v.replace('"','""')
                    elif isinstance(v, list):
                        # e.g. arguments=["foo", "bar", "baz"]
                        v = " ".join(["'%s'" % str(y).replace("'","''") for y in v])
                        v = '"%s"' % v.replace('"','""')
                    print("%s = %s" % (k,v), file=f)
                print("queue $(processes)", file=f)

class Node(object):
    """
    Parent class for nodes within a DAG (jobs and sub-DAGs)
    """
    def __init__(self, id, comment=None, dir=None):
        self.id = id
        self.comment = comment
        self.dir = dir
        self.parents = set()
        self.children = set()

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, repr(self.id))

    def __str__(self):
        return self.id

    def __unicode__(self):
        return self.id

    def parent(self, *other):
        self.parents.update(other)
        return self

    def child(self, *other):
        self.children.update(other)
        return self

    def write_dag_header(self, file):
        print("", file=file)
        if self.comment:
            print(re.sub(r'^', '# ', self.comment, flags=re.MULTILINE), file=file)

    def write_dag_footer(self, file):
        if self.parents:
            print("PARENT %s CHILD %s" %
                  (" ".join(sorted([str(o) for o in self.parents])), self), file=file)
        if self.children:
            print("PARENT %s CHILD %s" %
                  (self, " ".join(sorted([str(o) for o in self.children]))), file=file)

class Job(Node):
    """
    An instance of a job within a DAG
    """

    OPTIONS = {
        "data":		"DATA",
        "script_pre":	"SCRIPT PRE",
        "script_post":	"SCRIPT POST",
        "retry":	"RETRY",
        "abort_dag_on":	"ABORT-DAG-ON",
        "priority":	"PRIORITY",
        "category":	"CATEGORY",
    }
    _parent_jobs = set()

    def __init__(self, id, submit=None, comment=None, dir=None, noop=False, **vars):
        super(Job, self).__init__(id=id, comment=comment, dir=dir)
        self.submit = submit or id+".sub"
        self.noop = noop
        self.vars = vars

    def write(self):
        """
        Write out the job's submit and input files (if they are objects)
        """
        if hasattr(self.submit, 'write'):
            self.submit.write()
        if 'input' in self.vars and hasattr(self.vars['input'], 'write'):
            self.vars['input'].write()

    def write_dag_entry(self, file):
        """
        Write this job's entry in the containing DAG
        """
        self.write_dag_header(file)
        line = "JOB %s %s" % (self, self.submit)
        if self.dir:
            line += " DIR %s" % self.dir
        if self.noop:
            line += " NOOP"
        print(line, file=file)
        self.write_vars(file=file)
        self.write_dag_footer(file)

    def var(self, **v):
        """Update one or more DAG variables"""
        self.vars.update(v)
        return self

    def processes(self, n):
        """Mark a job as running a cluster of multiple processes"""
        self.var(processes=n)
        return self

    def __getitem__(self, varname):
        if varname in self.vars:
            v = self.vars[varname]
        elif self.submit and hasattr(self.submit,'vars') and varname in self.submit.vars:
            v = self.submit.vars[varname]
        else:
            raise KeyError("'%s' not present in job %s" % (varname, self))

        if varname in ["output", "error"]:
            if self.vars.get('processes', 1) > 1 and v.find("$(process)") < 0:
                v += '.$(process)'

        return v

    def write_vars(self, file):
        """
        Output macros which are to be passed to this job submission: e.g.
            myjob.var(foo="bar", bar="qux")
        """
        res = ''
        for k in sorted(self.vars.keys()):
            v = self[k]
            if v is None:
                continue
            elif k in Job.OPTIONS:
                if isinstance(v, list):
                    for vv in v:
                        print('%s %s %s' % (Job.OPTIONS[k], self, str(vv)),
                              file=file)
                else:
                    print('%s %s %s' % (Job.OPTIONS[k], self, str(v)),
                          file=file)
                continue
            elif re.match('queue', k, flags=re.IGNORECASE):
                raise ValueError('macroname must not start with "queue"')
            elif hasattr(v, 'iteritems'):
                # e.g. environment={"PATH":"/usr/bin","HOME":"/home/job"}
                v = " ".join(["%s=%s" % (x,re.sub("[ ']", '_', str(y)))
                              for (x,y) in v.iteritems()])
                # Not yet permitted by DAGMAN:
                #v = " ".join(["%s='%s'" % (x,y.replace("'","''"))
                #              for (x,y) in v.iteritems()])
            elif isinstance(v, list):
                # e.g. arguments=["foo", "bar", "baz"]
                v = " ".join([re.sub("[ ']", '_', str(y)) for y in v])
                # Not yet permitted by DAGMAN:
                #v = " ".join(["'%s'" % y.replace("'","''") for y in v])
            v = str(v).replace('\\','\\\\').replace('"','\\"')
            res += ' %s="%s"' % (k, v)
        if res:
            print('VARS %s%s' % (self, res), file=file)
        return self

    def set_function_data(self, func, args, kwargs, dag):
        # Does this job have any other Jobs in its args or kwargs?
        Job._parent_jobs.clear()
        cPickle.dumps((args, kwargs), protocol=pickle_protocol)
        # Add dependencies
        if Job._parent_jobs:
            self.parent(*Job._parent_jobs)
            input_files = []
            for j in Job._parent_jobs:
                input_files.extend(output_files(j.id, j['output'], j.vars.get('processes')))
            self.var(input_files=",".join(sorted(input_files)))
        # We also need a separate input file for this job
        if Job._parent_jobs or 'input' not in self.vars or not hasattr(self.vars['input'],'data'):
            self.vars['input'] = Input(filename="%s.%s.in" % (dag.id, self))
        # Finally store the function and args
        self.vars['input'].data[str(self)] = (func, args, kwargs)
        return self

    def __reduce__(self):
        """
        If this job is used as an argument to another job, then at depickle
        time we need to read the file(s) created by this job. Also add
        parents to the job currently being written.
        """
        Job._parent_jobs.add(self)
        return (read_job_output,
                (self.id, self['output'], self.vars.get('processes')))

class Dag(Node):
    """
    A Dag is a collection of nodes (jobs or sub-dags). It also allocates
    node ids.
    """
    def __init__(self, id, filename=None, comment=None, dir=None, maxjobs=None,
                 submit=None, input=None, config={}):
        super(Dag, self).__init__(id=id, comment=comment, dir=dir)
        self.filename = filename or (id + '.dag')
        self.maxjobs = maxjobs or {} # category => limit
        self.submit = submit or Submit(filename=id+".sub", **DEFAULT_SUBMIT_VARS)
        self.input = input or Input(filename=id+".in")
        self.config = config
        self.nodes = []              # (list, not set: must preserve order)
        self.last_id = {}            # id_prefix => sequence number
        self.written = False

    def __str__(self):
        return self.filename

    def next_id(self, id_prefix=""):
        """
        Allocate the next id for a given prefix
        """
        if id_prefix in self.last_id:
            self.last_id[id_prefix] += 1
        else:
            self.last_id[id_prefix] = 0
        return "%s%d" % (id_prefix, self.last_id[id_prefix])

    def write(self):
        """
        Write out the DAG. Will recursively write out all its jobs
        and sub-DAGs; each job also writes its input/submit files.
        """
        if not self.written:
            self.written = True
            with open(self.filename, "w") as f:
                if self.config:
                    print("CONFIG %s.config" % self.id, file=f)
                    with open("%s.config" % self.id, "w") as cf:
                        for (k,v) in self.config.iteritems():
                            print("%s = %s" % (k,v), file=cf)
                for node in self.nodes:
                    node.write()
                    node.write_dag_entry(file=f)
                for (k,v) in self.maxjobs.iteritems():
                    print("MAXJOBS %s %d" % (k,v), file=f)

    def write_dag_entry(self, file):
        self.write_dag_header(file)
        line = "SPLICE %s %s" % (self.id, self.filename)
        if self.dir:
            line += " DIR %s" % self.dir
        print(line, file=file)
        self.write_dag_footer(file)

    def node(self, cls, id=None, id_prefix="", **node_options):
        if id is None:
            id = self.next_id(id_prefix)
        node = cls(id=id, **node_options)
        self.nodes.append(node)
        return node

    def job(self, id=None, submit=None, **options):
        """
        Create a job object and add it to this DAG. Pass either
        id or id_prefix; the latter will allocate an id sequentially.
        """
        return self.node(Job, id=id,
                         submit=(submit or self.submit),
                         **options)

    def dag(self, id, **options):
        """
        Create a sub-DAG within this DAG (experimental)
        """
        return self.node(Dag, id=id, **options)

    def defer(self, func=None, id_prefix=None, **vars):
        """
        Return a function so that defer(settings)(args) creates a condor job.
        This is the core functionality of this library.

        def myfunc(args):
            ...
        mydag.defer(myfunc, request_memory=1024)(args)

        Note: unless you pass input=<obj> or submit=<obj>, the dag's
        input file and submit file respectively will be used. Hence these
        will be shared between instances if you queue the same function
        multiple times.

        Pass output=None or error=None if you wish to suppress generation
        of the stdout and stderr files.
        """
        dag = self
        def deferred(*args, **kwargs):
            job = dag.job(
                id_prefix=id_prefix or func.__name__+'_',
                **vars
            )
            if 'input' not in job.vars:
                job.var(input=dag.input) # default to dag's shared input file
            if 'output' not in job.vars:
                job.var(output='%s.%s.out' % (dag.id, job.id))
            if 'error' not in job.vars:
                job.var(error='%s.%s.err' % (dag.id, job.id))
            job.set_function_data(func, args, kwargs, dag)
            return job

        if func is not None:
            return deferred

        return lambda func: self.defer(func=func, id_prefix=id_prefix, **vars)

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

def ad_attr(attr, env='_CONDOR_JOB_AD'):
    """Return a single classAd value"""
    if running():
        if env not in ads:
            ads[env] = parse_ad(os.environ[env])
        return ads[env][attr]
    else:
        return Ad(attr, env)

def output_files(id, filename, processes=None):
    """
    Return a list of filenames of all the outputs for a job (cluster)
    """
    base = re.sub(r'\$\(jobname\)',str(id),filename,flags=re.IGNORECASE)
    return [re.sub(r'\$\(process\)',str(p),base,flags=re.IGNORECASE)
            for p in range(processes or 1)]

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
            with open(output_files(id, filename, None)[0], 'rb') as f:
                return cPickle.load(f)
        else:
            res = []
            for fn in output_files(id, filename, processes):
                with open(fn, 'rb') as f:
                    res.append(cPickle.load(f))
            return res
    else:
        return Job(id=id, submit=None, output=filename, processes=processes)

def invoke(job_data):
    """
    Run a job, passing in the de-pickled argument set.
    If an exception is raised then the default python behaviour is to
    write a backtrace to stderr and exit with a non-zero code, which is
    what we want for htcondor.
    """
    (func, args, kwargs) = job_data
    return func(*args, **kwargs)     # apply(*job_data) is deprecated

def run(src=sys.stdin, dst=sys.stdout, output_none=False):
    if src.isatty():
        print('%s is non-interactive, requires a pickled argument set' % sys.argv[0], file=sys.stderr)
        sys.exit(1)
    else:
        job_name = re.sub(r'^.*\+','',ad_attr('DAGNodeName'))  # FIXME: use a command-line argument?
        data = cPickle.load(src)
        if job_name not in data:
            raise KeyError("Job name '%s' not found in job input" % job_name)
        res = invoke(data[job_name])
        if res is not None or output_none:
            cPickle.dump(res, dst, pickle_protocol)

def autorun(report_hostname=True, *args, **kwargs):
    """
    Call this in your application after you have defined your functions,
    but before deciding what functions to queue. Then if the script is called
    as a htcondor job, it will just invoke the desired function.

    It also causes the top-level DAG to be written out when your program
    terminates, unless you use autorun(write_dag=False)

    If you pass report_hostname=True then a line is written to stderr saying
    the name of the host where the job is run. This can be useful to pin
    down problems with a particular server.
    """
    if running():
        if report_hostname:
            import socket
            print("HTCONDOR: Running on %s" % socket.gethostname(), file=sys.stderr)
        run(*args, **kwargs)
        sys.exit(0)
    elif 'UNPICKLE' in os.environ:
        import pprint
        data = cPickle.load(open(os.environ['UNPICKLE'], 'rb'))
        if len(sys.argv) > 1:
            jobid = sys.argv[1]
            if jobid not in data:
                print("Job '%s' not in input" % jobid, file=sys.stderr)
                sys.exit(1)
            data = data[jobid]
        pprint.pprint(data)
        sys.exit(0)

# Another option is to set Executable = htcondor_dag.py in submit file.
# For this to work, when you write the dag file the functions must have
# been imported from a different module, not implicitly __main__
if __name__ == '__main__':
    run()
    sys.exit(0)

