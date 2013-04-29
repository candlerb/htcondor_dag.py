#!/usr/bin/env python
import subprocess
from htcondor import job_with, autorun

# Note: you can't set input=None because this is where htcondor.py
# stores the picked arguments to call the function
@job_with(output=None)
def bash(cmd):
    subprocess.check_call(["/bin/bash","-c","set -o pipefail; " + cmd])

autorun()

j1 = bash.queue("tr 'a-z' 'A-Z' </etc/passwd >tmp1")
j2 = bash.queue("tr 'a-z' 'A-Z' </etc/hosts >tmp2")
j3 = bash.job(job_files="tmp1,tmp2").queue("cat tmp1 tmp2 >tmp.out").parent(j1, j2)
