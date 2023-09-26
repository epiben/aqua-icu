#! /usr/bin/env bash

spec=("qsub -l nodes=1:ppn={threads},"
	  "mem={resources.vmem}mb,"
	  "walltime={resources.tmin}:00"
	  " -j eo -e ~/.qsub_logs/")

call=$(printf "%s" "${spec[@]}")

snakemake $@ -p --jobs 80 --notemp --verbose --latency-wait 120 \
	--cluster "$call" #--cluster-status "python qsub-status.py" 
