import numpy as np
from user_level_histogram_src.datasetHistogram import DatasetHistogram

class UnclippedMechanismHistogram:
    def __init__(self, epsilon):
        self.epsilon = epsilon

    def compute_unclipped_sensitivity_histogram(self, m_star):
        """
        Sensitivity of the unclipped histogram = 2 * m*
        """
        return 2 * m_star
    
    def compute_unclipped_b_histogram(self, unclipped_sens):
        """
        Scale parameter for the Laplace mechanism.
        b = sensitivity / epsilon
        """
        return unclipped_sens / self.epsilon

    def laplace_sample_unclipped(self, sensitivity):
        """
        Generates Laplace(0, b) noise using the unclipped sensitivity.
        """
        b = self.compute_unclipped_b_histogram(sensitivity)
        noise = np.random.laplace(0, b)
        return noise

    def compute_unclipped_histogram(self, f, k, sensitivity):
        """
        Produces an unclipped noisy histogram.

        f : true histogram array (length k)
        k : number of bins
        sensitivity : L1 sensitivity for Laplace noise

        Returns:
            A : array of length k containing f[i] + Laplace(0, b)
        """

        A = [0] * k

        for bin_index in range(k):
            Z = self.laplace_sample_unclipped(sensitivity)
            A[bin_index] = f[bin_index] + Z

        return A
