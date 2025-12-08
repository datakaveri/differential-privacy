from .UserData import UserData
from .Dataset import Dataset
from .Clipper import Clipper
import numpy as np

class UnclippedDPMechanism:
    def __init__(self, epsilon):
        self.epsilon = epsilon
        self.b = None

    def compute_unclipped_sensitivity(self, T_epsilon_unclipped, dataset):
        total_contributions = sum(user.num_records() for user in dataset.users)
        sensitivity = T_epsilon_unclipped / total_contributions
        #print(f"unclipped sensitivity: ", sensitivity)
        return sensitivity
    
    def compute_unclipped_b(self, sensitivity):
        self.b = sensitivity/self.epsilon
        #print(f"unclipped b: ", self.b)
        return self.b
    
    def add_laplace_noise_unclipped(self, sensitivity):
        b = self.compute_unclipped_b(sensitivity)
        noise = np.random.laplace(0, b)
        return noise
    
    def release_dp_mean_unclipped(self, dataset, attr_name, T_epsilon_unclipped):
        """
        Compute and release DP-protected mean of an attribute without clipping.
        """
        all_values = []
        for user in dataset.users:
            for record in user.records:
                all_values.append(record[attr_name])  
        
        true_mean = np.mean(all_values)

        sensitivity = self.compute_unclipped_sensitivity(T_epsilon_unclipped, dataset)
        noise = self.add_laplace_noise_unclipped(sensitivity)

        dp_mean = true_mean + noise
        return dp_mean
