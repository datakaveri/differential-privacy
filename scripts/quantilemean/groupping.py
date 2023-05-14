import numpy as np


def get_user_arrays(data, L):
    user_arrays = None
    K = None
    users = np.unique(data["User"])
    user_arrays = [[]]
    for u in users:
        counter = 0
        user_data = [data[data["User"]==u]["mean"].values[0] for i in range(data[data["User"]==u]["count"].values[0])]
        stop_cond = np.minimum(len(user_data), L)
        remaining_spaces = [L - len(user_arrays[i]) - stop_cond for i in range(len(user_arrays))]
        remaining_spaces = np.array(remaining_spaces)
        remaining_spaces = np.where(remaining_spaces < 0, L, remaining_spaces)
        array_idx_to_fill = None
        if np.min(remaining_spaces) >= L:
            user_arrays.append([])
            array_idx_to_fill = -1
        else:
            array_idx_to_fill = np.argmin(remaining_spaces)   
        
        while counter < stop_cond:
            user_arrays[array_idx_to_fill].append(user_data[counter])
            counter += 1
                
        K = len(user_arrays)
        
    return user_arrays, K