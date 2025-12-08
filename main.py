# import statements
import os
import sys
import glob

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.config_validation import load_config
from user_level_src.UserData import UserData
from user_level_src.Dataset import Dataset
from user_level_src.Clipper import Clipper
from user_level_src.ClippedDPMechanism import ClippedDPMechanism
from user_level_src.UnclippedDPMechanism import UnclippedDPMechanism
import scripts.Pipeline as pipe
import scripts.spatioTemporalPipeline as stpipe
import scripts.utilities as utils
import json

json_files = [f for f in os.listdir("config") if f.endswith(".json")]
if not json_files:
    raise FileNotFoundError("No JSON config file found in /config directory!")

config_file_name = os.path.join("config", json_files[0])
#print("Config file path =", os.path.abspath(config_file_name))

config = load_config(config_file_name)

#config = utils.read_config(config_file_name)
dataset = config.data_type
operations = config.operations

#fileList = [file for file in os.popen('ls data/*.json').read().split('\n') if file]
fileList = glob.glob("data/*.json")

if not fileList:
    print("No JSON config files found in 'data/' — continuing with defaults or provided config.")

# selecting appropriate pipeline

if "user-level-dp" in operations:
        print("\n=============================")
        print("Running User-Level Differential Privacy")
        print("=============================\n")
        user_level = config.medical.user_level_dp

        dataset_config = user_level.dataset
        dp_config = user_level.dp

        dataset_name = dataset_config.name
        total_users = dataset_config.total_users
        total_records = dataset_config.total_records
        min_contrib = dataset_config.min_contribution
        max_contrib = dataset_config.max_contribution
        attribute = dataset_config.attribute
        U = dataset_config.U
        V = dataset_config.V

        epsilon = dp_config.epsilon

        print(f"Dataset: {dataset_name}, Users: {total_users}, Epsilon: {epsilon}")

        dataset_file = dataset_name + ".csv"
        dataset = Dataset.from_csv(dataset_file)
        if (len(dataset.users) == 0):
            print("Dataset is empty. Skipping User Level DP. \n")
            sys.exit()
            
        dataset.sort_by_contribution()

        total_users = len(dataset.users)
        total_records = sum(user.num_records() for user in dataset.users)

        contrib_counts = [user.num_records() for user in dataset.users]
        print(f"Top 5 users by contribution: {contrib_counts[:5]}")
        print(f"Min contributions: {min(contrib_counts)}, Max contributions: {max(contrib_counts)}")

        index_i = dataset.compute_index_i(epsilon)
        T_epsilon_clipped = dataset.compute_T_epsilon_clipped(index_i, U, V, attribute)
        T_epsilon_unclipped = dataset.compute_T_epsilon_unclipped(U, V, attribute)

        clipper = Clipper()
        clipped_dp = ClippedDPMechanism(epsilon=epsilon)
        unclipped_dp = UnclippedDPMechanism(epsilon=epsilon)

        total_sum = 0
        count = 0
        for user in dataset.users:
            for record in user.records:
                total_sum += float(record[attribute])
                count += 1
        true_mean = total_sum / count if count else 0
        print(f"True mean of {attribute}: {true_mean}\n")

        sensitivity_clipped = clipped_dp.compute_clipped_sensitivity(T_epsilon_clipped, dataset)
        sensitivity_unclipped = unclipped_dp.compute_unclipped_sensitivity(T_epsilon_unclipped, dataset)

        b_clipped = clipped_dp.compute_clipped_b(sensitivity_clipped)
        b_unclipped = unclipped_dp.compute_unclipped_b(sensitivity_unclipped)

        noise_clipped = clipped_dp.add_laplace_noise_clipped(sensitivity_clipped)
        noise_unclipped = unclipped_dp.add_laplace_noise_unclipped(sensitivity_unclipped)

        dp_mean_clipped = true_mean + noise_clipped
        dp_mean_unclipped = true_mean + noise_unclipped

        print(f"DP-protected mean (clipped) = {dp_mean_clipped}")
        print(f"DP-protected mean (unclipped) = {dp_mean_unclipped}")
        sys.exit()
else:
    if dataset == "spatioTemporal":
        if config["differential_privacy"]["dp_query"] == 'mean':
            data, bVector = stpipe.spatioTemporalPipeline(config, operations, fileList) 
            data = utils.post_processing(data, config)
            mean_absolute_error = utils.mean_absolute_error(bVector)
            formatted_error, formatted_averaged_error = utils.output_handler_spatioTemp_mae(mean_absolute_error, config)          
            formatted_data = utils.output_handler_spatioTemp_dp_data(data, config)
            concat_output = utils.output_concatenator(anonymised_output = formatted_data, epsilon_vs_error_per_hat = formatted_error, epsilon_vs_averaged_error = formatted_averaged_error)
        if config["differential_privacy"]["dp_query"] == 'count':
            data, bVector = stpipe.spatioTemporalPipeline(config, operations, fileList) 
            data = utils.post_processing(data, config)
            mean_absolute_error = utils.mean_absolute_error(bVector)
            formatted_error = utils.output_handler_spatioTemp_mae(mean_absolute_error, config)
            formatted_data = utils.output_handler_spatioTemp_dp_data(data, config)
            concat_output = utils.output_concatenator(anonymised_output = formatted_data, epsilon_vs_error = formatted_error)
    else:
        data, mean_absolute_error, noisy_query_output_for_epsilon_vector = pipe.generalPipelineDP(config, operations, fileList)
        data = utils.post_processing(data, config)
        formatted_error = utils.output_handler_general_mae(mean_absolute_error, config)
        formatted_data = utils.output_handler_general_dp_data(data, config)
        formatted_noise_vector = utils.output_handler_general_noise_vector(noisy_query_output_for_epsilon_vector)
        concat_output = utils.output_concatenator(anonymised_output = formatted_data, epsilon_vs_error = formatted_error, noise_vector = formatted_noise_vector)