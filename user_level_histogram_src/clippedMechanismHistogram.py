import numpy as np
import math
from user_level_histogram_src.datasetHistogram import DatasetHistogram

class ClippedMechanismHistogram:
    def __init__(self, epsilon):
            self.epsilon = epsilon

    def build_histogram_per_user(self, user, bins, attribute, k, bin_width, U, V):

        user_counts = [0] * k

        for sample in user.records:
            value = sample[attribute]

            if value < U or value > V:
                continue

            bin_index = int((value - U) // bin_width)

            if value == V:
                bin_index = k - 1

            if bin_index >= k:
                bin_index = k - 1

            user_counts[bin_index] += 1

        return user_counts

    def clip_histogram(self, C, m_i, user_counts, epsilon, k):
        
        if (C >= m_i):
            # No clipping required
            return user_counts[:]

        else:
            scale = C / m_i
            clipped_counts = [scale * u for u in user_counts]

        return clipped_counts
    
    def merge_clipped_user_histograms(self, histograms, k):
        "Merging all user histograms to get new counts"
        merged = [0.0] * k

        for h in histograms:
            for j in range(k):
                merged[j] += h[j]

        return merged

    def compute_clipped_sensitivity_histogram(self, C):
        clipped_sensitivity = 2*C

        return clipped_sensitivity

    def compute_clipped_b_histogram(self, clipped_sensitivity):
        clipped_b = clipped_sensitivity/self.epsilon
        return clipped_b
    
    def laplace_sample_clipped(self, sensitivity):
        b = self.compute_clipped_b_histogram(sensitivity)
        noise = np.random.laplace(0, b)
        return noise
    
    def compute_clipped_histogram(self, f, k, sensitivity):
         
        A = [0] * k


        for bin_index in range(k):
            Z = self.laplace_sample_clipped(sensitivity)
            A[bin_index] = f[bin_index] + Z

        #print(f"A: {A}\n")

        return A