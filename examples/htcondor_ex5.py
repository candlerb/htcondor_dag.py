#!/usr/bin/env python
import subprocess
from htcondor_dag import Dag, autorun

# Note: you can't set input=None because this is where htcondor_dag.py
# stores the picked arguments to call the function
def bash(cmd):
    subprocess.check_call(["/bin/bash","-c","set -o pipefail; " + cmd])

autorun()

dag = Dag("htcondor_ex5")
d_bash = dag.defer(bash, output=None, arguments=["one","\"two\"","spacey 'quoted' argument"],
                   environment={"one":1,"two":'"2"',"three":"spacey 'quoted' value"})

j1 = d_bash("tr 'a-z' 'A-Z' </etc/passwd >tmp1")
j2 = d_bash("tr 'a-z' 'A-Z' </etc/hosts >tmp2")
j3 = d_bash("cat tmp1 tmp2 >tmp.out").parent(j1, j2).var(job_files="tmp1,tmp2")
dag.write()
