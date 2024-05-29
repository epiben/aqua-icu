library(future.batchtools)
library(tarchetypes)
library(targets)
library(hrqolr)
library(dplyr)
library(tidyr)

scenarios <- expand_grid(
	n_patients_per_arm = c(100L, 500L, 1000L, 2000L),
	relative_mortality_reduction = c(0.0, 0.025, 0.05, 0.10, 0.20),
	acceleration_hrqol = c(0.0, 0.02, 0.05, 0.10),
	relative_improvement_final_hrqol = c(0.0, 0.1, 0.2),
	mortality_dampening = c(0.0, 0.10, 0.20, 0.50),
	prop_mortality_benefitters = c(0.0, 0.05, 0.10, 0.15),
	mortality_trajectory_shape = c("exp_decay", "constant", "linear", "reflected_exp_decay")
) 

scenarios <- scenarios[1:4, ]



# # For benchmarking of memory requirement
# scenarios <- data.frame(
# 	n_patients_per_arm = 2000L,
# 	relative_mortality_reduction = 0.10,
# 	acceleration_hrqol = 0.1,
# 	relative_improvement_final_hrqol = 0.2,
# 	mortality_dampening = 0.0,
# 	prop_mortality_benefitters = 0.15,
# 	mortality_trajectory_shape = "exp_decay"
# )

# Add metadata to the scenarios data frame
scenarios$scenario_id <- seq_len(nrow(scenarios))
scenarios$scenario_name <- lapply(
	paste0("scenario__", apply(scenarios, 1, paste, collapse = "__")), 
	as.symbol
) # see Examples in ?tar_eval
scenarios$scenario_priority <- with(scenarios, 1 - scenario_id/max(scenario_id))
scenarios$run_hash <- substr(openssl::sha1(paste(Sys.time())), 1, 7)
	# inspired by short git commit hashes

cat(
	sprintf("\033[32m▶ simulating for %i scenarios\033[39m\n", nrow(scenarios))
)

worker_fun <- function(
		scenario_id,
		n_patients_per_arm,
		relative_mortality_reduction,
		acceleration_hrqol,
		relative_improvement_final_hrqol,
		mortality_dampening,
		prop_mortality_benefitters,
		mortality_trajectory_shape,
		run_hash,
		last_scenario_id = max(scenarios$scenario_id),
		...
) {

	cache_hrqolr(2 * 1024^3)
	data.table::setDTthreads(1) # don't circumvent slurm

	scenario <- setup_scenario(
		arms = c("intv", "ctrl"),
		n_patients = n_patients_per_arm,
		index_hrqol = 0.0,
		first_hrqol = 0.1,
		final_hrqol = 0.75 * c(intv = 1 + relative_improvement_final_hrqol, ctrl = 1),
		acceleration_hrqol = c(intv = 1 + acceleration_hrqol, ctrl = 1),
		mortality = 0.4 * c(intv = 1 - relative_mortality_reduction, ctrl = 1),
		mortality_dampening = mortality_dampening,
		mortality_trajectory_shape = mortality_trajectory_shape,
		prop_mortality_benefitters = c(intv = prop_mortality_benefitters, ctrl = 0),
		sampling_frequency = 14,
		verbose = FALSE # avoid cluttering targets output
	)

	sims <- simulate_trials(
		scenario,
		n_trials = 100,
		# n_trials = 1e5,
		n_patients_ground_truth = 1e6,
		n_example_trajectories_per_arm = 0,
		max_batch_size = 10e6,
		verbose = TRUE
	)

	print(sims$resource_use)

	if (scenario_id %% 25 == 0) {
		pushover_silent(
			sprintf("%s: Finished scenario #%i", run_hash, scenario_id),
			user = "u25z2harmu27c5py37dce77fuqy3xd",
			app = "a1n8jsyk8arj59cinmwwm9x2qz6yrv"
		)
	}

	if (scenario_id == last_scenario_id) {
		pushover_high(
			sprintf("%s: Finished all scenarios", run_hash),
			user = "u25z2harmu27c5py37dce77fuqy3xd",
			app = "a1n8jsyk8arj59cinmwwm9x2qz6yrv"
		)
	}

	return(sims)
}

plan(
    batchtools_slurm, 
    template = "slurm.tmpl",
    resources = list(
        walltime = 60, # minutes
        memory = 1024 * 20, # MBs
        ncpus = 1,
		partition = "standard"
    )
)

tar_option_set(
    packages = c("hrqolr", "pushoverr", "data.table"),
    format = "qs", 
    repository = "local",
    memory = "transient"
)

tar_config_set(
	seconds_meta_append = 15,
	seconds_meta_upload = 15,
	seconds_reporter = 1
)

simulations <- tar_eval(
	expr = tar_target(
		scenario_name,
		worker_fun(
			scenario_id,
			n_patients_per_arm,
			relative_mortality_reduction,
			acceleration_hrqol,
			relative_improvement_final_hrqol,
			mortality_dampening,
			prop_mortality_benefitters,
			mortality_trajectory_shape,
			run_hash
		),
		priority = scenario_priority
	),
	values = scenarios
)

figure1 <- tar_combine(
	figure1_target,
	simulations,
	command = list(!!!.x)
)

list(
	simulations,
	figure1
)
