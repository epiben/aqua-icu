# NLPANDMED PIPELINE
#  
# Authors: [ Benjamin Skov Kaas-Hansen | Davide Placido | Cristina Leal Rodríguez ]  
#
# 
# ========== SETUP ==========

configfile: "config.json"
import getpass
config["user"] = getpass.getuser()

shell.prefix("""
    module purge
    module load tools anaconda3/5.3.0 
    module load intel/redist/2017.2.174 intel/perflibs/64/2017_update3
    module unload R
    module load R/3.6.2
""")

# ========== 00 TARGET RULE ==========

rule target:
    input: 
        "data/fasttext_embedding.bin", # keep here to start early in pipeline (long runtime)
        "data/compute_label_frequencies.tstamp", # idem
        dynamic("data/keras_models/{target_label}.tstamp"),
        "data/create_predicted_profiles.tstamp",
        "output/table_top50_signals.tsv",
        "output/figure_2_frequencies_top_target_labels.pdf",
        "output/figure_3_fingerprints.pdf",
        "output/figure_4_heatmap_congruence_signals.pdf",
        "output/figure_S1_discrimination_calibration.pdf"
    output: "data/pipeline_rulegraph.pdf"
    resources: vmem = 1024*5, tmin = 10
    shell: "snakemake --rulegraph | dot -Tpdf > {output}"

# ========== EMBEDDING ==========

rule prepare_notes_for_fasttext:
    input: "code/prepare_notes_for_fasttext.sql"
    output: protected("data/full_corpus__wo_spec_chars__lowercase.tsv")
    resources: vmem = 1024*5, tmin = 60*6
    benchmark: "benchmarks/prepare_notes_for_fasttext.tsv"
    log: "logs/prepare_notes_for_fasttext.log"
    shell: """
        psql -h dbserver -U {config[user]} -d bth -f {input} \
            -v schema={config[schema]} > {log}
    """

rule train_fasttext_embeddings:
    input: rules.prepare_notes_for_fasttext.output
    output: "data/fasttext_embedding.bin"
    params: dim = 100, minn = 3, maxn = 6, wordNgrams = 3, lr = 0.1
    resources: vmem = 1024*200, tmin = 60*36
    threads: 100
    benchmark: "benchmarks/train_fasttext_embeddings.tsv"
    log: "logs/train_fasttext_embeddings.log"
    shell: """
        module load fasttext/0.8.22
        fasttext skipgram \
            -input {input} \
            -output $(dirname {output})/$(basename {output} .bin) \
            -minn {params.minn} -maxn {params.maxn} -dim {params.dim} \
                -lr {params.lr} -thread {threads} > {log}
    """

# ========== KERAS + RELATED PREPROCESSING ==========

rule prepare_notes_for_keras:
    input: names = "data/00-raw-symlinks/names.tsv"
    output: protected("data/prepare_notes_for_keras.tstamp")
    params: negation_window = 5, min_token_length = 4
    threads: 100
    resources: vmem = 1024*10, tmin = 60*3
    benchmark: "benchmarks/prepare_notes_for_keras.tsv"
    log: "logs/prepare_notes_for_keras.log"
    script: "code/prepare_notes_for_keras.py"

rule compute_tf:
    input: rules.prepare_notes_for_keras.output
    output: protected("data/compute_tf.tstamp")
    params: window = 48 # hours
    threads: 100
    resources: vmem = 1024*80, tmin = 60*18
    benchmark: "benchmarks/compute_tf.tsv"
    log: "logs/compute_tf.log"
    script: "code/compute_tf.py"

rule compute_df_idf:
    input: rules.compute_tf.output
    output: protected("data/compute_df_idf.tstamp")
    params: min_df = 5
    threads: 100
    resources: vmem = 1024*200, tmin = 60*3
    benchmark: "benchmarks/compute_df_idf.tsv"
    log: "logs/compute_df_idf.log"
    script: "code/compute_df_idf.py"

rule create_keras_data:
    input: rules.compute_df_idf.output
    output: protected("data/create_keras_data.tstamp")
    params: n_tokens_per_visit = 50, min_df = 10, max_df = 50000 
    resources: vmem = 1024*200, tmin = 60*2
    threads: 100
    benchmark: "benchmarks/create_keras_data.tsv"
    log: "logs/create_keras_data.log"
    script: "code/create_keras_data.py"

rule create_label_files: 
    input: rules.create_keras_data.output
    output: dynamic("data/target_labels/{target_label}")
    params: output_dir = "data/target_labels/", min_label_count = 1000
    resources: vmem = 2048, tmin = 60
    script: "code/create_label_files.py"

rule train_keras_models:
    input:
        "data/target_labels/{target_label}",
        data = rules.create_keras_data.output[0], # index to extract string
        embedding_model = rules.train_fasttext_embeddings.output[0] # idem
    output: 
        model_file = "data/keras_models/{target_label}.hdf5",
        tstamp = "data/keras_models/{target_label}.tstamp"
    # wildcard_constraints: target_label = r"\w\d\d\w\w\d\d"
    params: 
        n_layers = 2, n_nodes = 256, activation_function = "tanh",
        optimizer = "Adam", learning_rate = 5e-4, reduce_lr_factor = 0.5,
        batch_size = 1024, n_epochs = 100,
        l2_penalty = 0, dropout_rate = 0.1,
        balance_epoch = False,
        metric_monitor = "val_auc", metric_mode = "max", patience = 10, 
        early_stopping = True, min_delta = 0.0025,
        model_name = "mlp", kernel_size = 3,
        verbose = True,
        calibration_n_bins = 10
    resources: vmem = 1024*80, tmin = 60*24
    threads: 2 # checkjob confirms this be a good speed_per_job/n_jobs ratio
    benchmark: "benchmarks/train_keras_model_{target_label}.tsv"
    log: "logs/train_keras_model_{target_label}.log"
    script: "code/train_keras_model.py"

rule compute_label_frequencies:
    output: "data/compute_label_frequencies.tstamp"
    resources: vmem = 1024*2, tmin = 60
    benchmark: "benchmarks/compute_label_frequencies.tsv"
    log: "logs/compute_label_frequencies.log"
    script: "code/compute_label_frequencies.py"

# ========== OUTPUTS ==========

rule create_predicted_profiles: # serves as a "join node" for subsequent rules
    input: 
        dummy_text = "data/lorem_ipsum_5_paragraphs.txt",
        terms = "data/terms.yaml",
        keras_models = dynamic("data/keras_models/{target_label}.hdf5"),
        embedding_model = rules.train_fasttext_embeddings.output[0] # index yields path
    output: "data/create_predicted_profiles.tstamp"
    params: n_signals = 25, min_auroc = 0.7, min_intercept = -0.05, 
        max_intercept = 0.05, min_slope = 0.95, max_slope = 1.05
    resources: vmem = 1024*100, tmin = 60
    threads: 10
    benchmark: "benchmarks/create_predicted_profiles.tsv"
    log: "logs/create_predicted_profiles.log"
    script: "code/create_predicted_profiles.py"

# === TABLES
rule table_top_signals:
    input: rules.create_predicted_profiles.output
    output: "output/table_top50_signals.tsv"
    params: max_rank = 50
    resources: vmem = 1024*5, tmin = 15
    log: "logs/table_signals.log"
    shell: """
        psql -h dbserver -U {config[user]} -d bth -c \
            "\copy (
            	WITH cte_signals AS (
					SELECT 
						*
						, DENSE_RANK() OVER(
							PARTITION BY domain, main_term, term, target_label ~ '_'
							ORDER BY signal_rank ASC
						    ) AS grouped_rank
					FROM {config[signals_table]}
					WHERE odds_ratio > 1
                    	AND main_term = term
				)
				SELECT *
				FROM cte_signals
				WHERE grouped_rank <= {params[max_rank]}
            ) TO '{output}' HEADER DELIMITER E'\t' CSV;" > {log}
    """

# ========== FIGURES ==========

rule figure_2_frequencies_top_target_labels:
    input: rules.create_predicted_profiles.output
    output: "output/figure_2_frequencies_top_target_labels.pdf"
    params: width = 17, height = 25
    resources: vmem = 1024*5, tmin = 15
    log: "logs/figure_2_frequencies_top_target_labels.log"
    script: "code/figure_2_frequencies_top_target_labels.R"

rule figure_3_fingerprints:
    input: rules.create_predicted_profiles.output
    output: "output/figure_3_fingerprints.pdf"
    params: width = 17, height = 25
    resources: vmem = 1024*5, tmin = 15
    log: "logs/figure_3_fingerprints.log"
    script: "code/figure_3_fingerprints.R"

rule figure_4_heatmap_congruence_signals:
    input: rules.create_predicted_profiles.output
    output: "output/figure_4_heatmap_congruence_signals.pdf"
    params: max_rank = 50, width = 40, height = 40
    resources: vmem = 1024*5, tmin = 15
    log: "logs/figure_4_heatmap_congruence_signals.log"
    script: "code/figure_4_heatmap_congruence_signals.R"

rule figure_S1_discrimination_calibration:
    input: rules.create_predicted_profiles.output
    output: "output/figure_S1_discrimination_calibration.pdf"
    params: max_rank = 50
    resources: vmem = 1024*5, tmin = 10
    log: "logs/figure_S1_discrimination_calibration.log"
    script: "code/figure_S1_discrimination_calibration.R"

# ========== MISC ==========

rule misc_signals_for_eval:
    input: 
        signals = rules.table_top_signals.output,
        atc_map = "00-raw-symlinks/atc_classification.tsv",
        danish_interaction_database = "data/danish_interaction_database.tsv"
    output: "output/misc_signals_for_eval.tsv"
    resources: vmem = 1024*5, tmin = 10
    log: "logs/misc_signals_for_eval.log"
    script: "code/misc_signals_for_eval.R"
    
