from .UserData import UserData
from .Dataset import Dataset
from .Clipper import Clipper
import numpy as np

class ClippedDPMechanism:
    def __init__(self, epsilon):
        self.epsilon = epsilon
        self.b = None

    def compute_clipped_sensitivity(self, T_epsilon_clipped, dataset):
        total_contributions = sum(user.num_records() for user in dataset.users)
        sensitivity = T_epsilon_clipped / total_contributions
        #print(f"clipped sensitivity: ", sensitivity)
        return sensitivity
    
    def compute_clipped_b(self, sensitivity):
        self.b = sensitivity/self.epsilon
        return self.b
    
    def add_laplace_noise_clipped(self, sensitivity):
        b = self.compute_clipped_b(sensitivity)
        #print(f"clipped b: ", b)
        noise = np.random.laplace(0, b)
        return noise
    
    def release_clipped_dp_mean(self, dataset, attr_name, index_i, U,V, T_epsilon, clipper: Clipper):
        """
        Compute and release DP-protected mean of an attribute.
        """
        clipped_mean = clipper.clipped_mean(dataset, attr_name, index_i, U,V, T_epsilon)

        all_values = []
        for user in dataset.users:
            for record in user.records:
                all_values.append(record[attr_name])  
        
        true_mean = np.mean(all_values)
        #print(f"True mean, Clipped mean:", true_mean, clipped_mean)

        clipped_error = true_mean - clipped_mean

        #print("clipped error: ", clipped_error)

        sensitivity = self.compute_clipped_sensitivity(T_epsilon, dataset)
        noise = self.add_laplace_noise_clipped(sensitivity)
        dp_mean = clipped_mean + noise
        return dp_mean, clipped_error