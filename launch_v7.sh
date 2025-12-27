#!/bin/bash
nohup ./scripts/run_benchmarking_suite.sh > suite_results_high_vol_v7.log 2>&1 &
echo $! > suite_pid.txt
