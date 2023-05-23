import json
from quantilemean.algo_utils import process_data, calc_user_array_length
from quantilemean.groupping import get_user_arrays
from quantilemean.algo import private_estimation

import numpy as np

def give_me_private_mean(data, epsilon):
    config = None
    with open("../config/DPConfigITMS.json", "r") as jsonfile:
        config = json.load(jsonfile) 
        # print("Configurations loaded from config.json")
        jsonfile.close()
    
    
    data = process_data(data)
    
    L = calc_user_array_length(data)
    # print("L: ", L)
    user_arrays, K = get_user_arrays(data, L)
    # print("K:", K)
    actual_mean = np.mean(data["mean"].values)
    user_group_means = [np.mean(x) for x in user_arrays]
    upper_bound = config["spatio-temporal"]["globalMaxValue"]
    lower_bound = config["spatio-temporal"]["globalMinValue"]
    # print("Actual mean: ", actual_mean)
    
    final_estimate, clipped_signal, noise, var = private_estimation(
            user_group_means,
            L,
            K,
            upper_bound,
            lower_bound,
            epsilon,
        )
    # print(final_estimate)
    return final_estimate, actual_mean, noise, var

# if __name__ == "__main__":
#     give_me_private_mean(None, 0.1)
    # ers = 0
    # for i in range(50):
    #     est = give_me_private_mean(None, 0.1)
    #     err = abs(est - 20.66769)
    #     ers += err
    # print(ers/50)