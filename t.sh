#!/bin/sh
#
# Run the application test suite
#
# Pass flag -s if you want to see stdout/stderr generated during the test
#
py.test tests "$@"
