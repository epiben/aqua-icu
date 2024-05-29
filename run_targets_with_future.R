args <- commandArgs(trailingOnly = TRUE)
n_workers <- if (length(args) == 0) {
    999
} else {
    readr::parse_integer(args[1])
}

cat(sprintf("\033[32mRunning the pipeline on max. %i cores\033[39m\n", n_workers))
targets::tar_make_future(workers = n_workers)
