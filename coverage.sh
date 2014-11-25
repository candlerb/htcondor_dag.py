#!/bin/sh
#
# Run the application test suite *and* generate coverage statistics
# and annotated source files (*,cover) showing untested code
#
exec ./t.sh --cov-report term --cov-report annotate --no-cov-on-fail \
  --cov htcondor_dag "$@"
