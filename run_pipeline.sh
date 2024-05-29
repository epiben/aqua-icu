#!/usr/bin/bash

module reload # to handle cases when pipeline invoked inside screen session

# first and only parameter (unnamed): number of workers
Rscript run_targets_with_future.R $1
