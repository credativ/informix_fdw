#!/usr/bin/env bash

set -e

# Prepare informix regression databases
tar -xzf regression.tar.gz
tar -xzf regression_dml.tar.gz
dbimport regression
dbimport -l buffered regression_dml
