import math
import os
import numpy as np

def get_left_right_counts(bins, values):
    left_counts = []
    right_counts = []
    
    for v in bins:
        left_counts.append(len([x for x in values if x < v]))
        right_counts.append(len([x for x in values if x > v]))
        
    return left_counts, right_counts

def get_probs(bins, left_counts, right_counts, epsilon, factor):
    c = np.maximum(left_counts, right_counts)
    probs = [math.exp(- (epsilon*c[i]) / (2 * factor) ) for i in range(len(bins))]
    probs = probs / np.sum(probs)
    # print("Probability assigned to quantized means: ", probs)
    return probs

def get_probs_quantiles(vals, alpha, epsilon, factor):
    k = len(vals) - 1
    probs = [(vals[i+1] - vals[i])*(math.exp(- (epsilon/ (2 * factor))  *   abs(i - (alpha*k))))for i in range(len(vals)-1)]
    probs = probs / np.sum(probs)
    
    return probs

def private_quantile(vals, q, epsilon, ub, lb, factor):
    vals_c = [lb if v < lb else ub if v > ub else v for v in vals]
    vals_sorted = np.sort(vals_c)
    new_s_vals = [lb]
    new_s_vals = np.append(new_s_vals, vals_sorted)
    new_s_vals = np.append(new_s_vals, ub)
    probs = get_probs_quantiles(new_s_vals, q, epsilon, factor)
    indices = np.arange(0, len(new_s_vals)-1)
    selected_interval = np.random.choice(indices, p=probs)
    selected_quantile = np.random.uniform(new_s_vals[selected_interval], new_s_vals[selected_interval+1])
    return selected_quantile

def private_estimation(user_group_means, L, K, ub, lb, epsilon):
    
    q = 0.1
    user_group_means = np.sort(user_group_means)
    factor = 1
    quantile_1 = q
    quantile_2 = 1 - q
    q1_t = private_quantile(user_group_means, quantile_1, epsilon/4, ub, lb, factor)
    q2_t = private_quantile(user_group_means, quantile_2, epsilon/4, ub, lb, factor)
    q1 = np.minimum(q1_t, q2_t)
    q2 = np.maximum(q1_t, q2_t)
    projected_vals = np.clip(user_group_means, q1, q2)
    mean_of_projected_vals = np.mean(projected_vals)
    noise_projected_vals = np.random.laplace(0, ( ((q2-q1)*factor) / (K * (epsilon/2))  ))
    final_estimate = mean_of_projected_vals + noise_projected_vals
    return final_estimate, mean_of_projected_vals, noise_projected_vals
        
    
def baseline_estimation(data, ub, lb, epsilons, num_exp):
    data_grouped = data.groupby(["User"]).agg({"Value":"count"}).reset_index()
    max_contrib = np.max(data_grouped["Value"])
    sum_contrib = np.sum(data_grouped["Value"])
    print("Max contribution: ", max_contrib)
    print("Sum of contributions: ", sum_contrib)
    f_base = './results/baseline/epsilon_{}/'
    for e in epsilons:
        b = ((ub - lb) * max_contrib) / (sum_contrib * e)
        noise = np.random.laplace(0, b, num_exp)
        os.makedirs(f_base.format(e), exist_ok=True)
        np.save(f_base.format(e) + 'losses.npy', np.abs(noise))

    