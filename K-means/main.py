import os
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"


import time
import argparse
import sys
import warnings

import numpy as np
from scipy.special import erfcinv
from scipy.spatial.distance import cdist
from scipy.optimize import root_scalar

from sklearn.cluster import KMeans
from sklearn.exceptions import ConvergenceWarning

warnings.filterwarnings("ignore", category=ConvergenceWarning)


"""
Implementation of DP K-means clustering in IBM's diffprivlib
Refs: 
    N. Holohan, S. Braghin, P. Mac Aonghusa, and K. Levacher, “Diffprivlib: The IBM differential privacy library,” 2019, arXiv:1907.02444.
"""
from diffprivlib.models import KMeans as IBMKMeans


"""
Implementation of Google's Coreset-based Clustering
Refs: 
    https://research.google/blog/practical-differentially-private-clustering/
    https://github.com/google/differential-privacy/blob/main/learning/clustering/README.md
"""
if not os.path.exists("differential-privacy"):
    os.system("git clone https://github.com/google/differential-privacy.git")
sys.path.append("differential-privacy/learning")

from clustering import clustering_algorithm
from clustering import clustering_params


# ============================================================
# Utility functions
# ============================================================


def laprnd(shape, a=0.0, b=1.0):
    """
    Laplace(a, b) samples using inverse transform
    """
    u = np.random.rand(*shape) - 0.5
    return a - b * np.sign(u) * np.log(1 - 2 * np.abs(u))


def qfuncinv(p):
    return np.sqrt(2) * erfcinv(2 * p)


def generate_clustered_data(d, r, K, N, alpha=1e-4, max_trials=20):
    flag = False
    N_trials = 0

    while not flag:
        if N_trials >= max_trials:
            raise RuntimeError("Data could not be generated within max_trials")

        mu0, a = get_cluster_centroids(d, r, K)

        labels = np.random.randint(0, K, size=N)

        X = mu0[:, labels] + a / qfuncinv(0.5 * alpha) * np.random.randn(d, N)

        flag = np.max(np.abs(X)) <= r
        N_trials += 1

    mu = np.array([X[:, labels == k].mean(axis=1) for k in range(K)]).T
    return X, labels, mu


def get_cluster_centroids(d, r, K, max_trials=None, tol=None, max_bisections=1000):

    if tol is None:
        tol = r / 50
    if max_trials is None:
        max_trials = 10 * K

    a_l = 0
    a_u = r * np.sqrt(d)

    for _ in range(max_bisections):

        if a_u - a_l <= tol:
            break

        a_m = (a_l + a_u) / 2

        N_trials, C_tmp = try_generate_centroids(d, r, K, a_m, max_trials)

        if N_trials <= max_trials:
            a_l = a_m
            C = C_tmp
        else:
            a_u = a_m

    return C, a_l


def try_generate_centroids(d, r, K, a, max_trials):

    C = np.full((d, K), np.nan)

    N_trials = 0

    for kk in range(K):

        flag = False

        while not flag:

            N_trials += 1

            if N_trials > max_trials:
                return N_trials, C

            c_k = r * (2 * np.random.rand(d, 1) - 1)

            cond1 = np.max(np.abs(c_k)) <= r - a

            cond2 = True
            if kk > 0:
                cond2 = np.all(np.linalg.norm(c_k - C[:, :kk], axis=0) >= 2 * a)

            if cond1 and cond2:
                C[:, kk] = c_k.ravel()
                flag = True

    return N_trials, C


def grid_points(s, d):
    """
    s: (m,) grid centers per coordinate
    d: dimension
    returns: (d, m^d) array
    """
    grids = np.meshgrid(*([s] * d), indexing='ij')
    S = np.stack(grids, axis=0).reshape(d, -1) # d × m^d
    return S


def hist_counts(Y, m):
    """
    Y: (d, N) in [0,1]^d
    m: bins per dimension
    returns: flattened histogram (m^d,)
    """
    d, N = Y.shape
    m = int(m)

    # Bin indices
    idx = np.minimum((Y * m).astype(int), m - 1)

    # Multi-dimensional histogram
    T = np.zeros([m]*d, dtype=np.int64)

    # Safe accumulation
    np.add.at(T, tuple(idx), 1)

    return T.reshape(-1)


def discretize(X, r, m):
    """
    X: (d, N) in [-r, r]^d
    m: bins per dimension
    returns: S (d, m^d), T (m^d,)
    """
    d, N = X.shape
    Y = 0.5 * (X / r + 1) # Normalize to [0,1]^d
    m=int(m)
    
    T = hist_counts(Y, m) # Histogram: size m^d

    # Grid centers in [-r, r]^d
    edges = np.linspace(-r, r, m + 1)
    s = edges[:-1] + (r / m)

    S = grid_points(s, d) # d × m^d

    return S, T


def kmeans_quantized(S, T, K, init_centroids=None, Max_iter_kMeans=100, tol=1e-6, seed=None):
    """
    Weighted K-means on quantized data

    Parameters:
    S : (d, M) array of bin centers
    T : (M,) weights (counts)
    K : number of clusters
    init_centroids : (d, K) initial centroids (optional)
    Max_iter_kMeans : max iterations
    tol : convergence tolerance
    seed : random seed (optional)

    Returns:
    C : (d, K) centroids
    """

    if seed is not None:
        np.random.seed(seed)

    d, M = S.shape
    T = T.reshape(-1).astype(np.float64)   # ensure (M,) and float

    # --- Initialization ---
    if init_centroids is None:
        perm = np.random.permutation(M)
        C = S[:, perm[:K]].copy()
    else:
        C = init_centroids.copy()

    labels = np.zeros(M, dtype=np.int64)

    for _ in range(Max_iter_kMeans):

        # --- Step 1: Assignment ---
        # Compute squared distances: (K, M)
        # Efficient vectorized form
        # ||s - c||^2 = ||s||^2 + ||c||^2 - 2 c^T s
        S_sq = np.sum(S**2, axis=0, keepdims=True)        # (1, M)
        C_sq = np.sum(C**2, axis=0, keepdims=True).T      # (K, 1)
        D = C_sq + S_sq - 2 * (C.T @ S)                   # (K, M)

        labels = np.argmin(D, axis=0)  # (M,)

        # --- Step 2: Update ---
        C_new = np.zeros((d, K))

        for k in range(K):
            idx = (labels == k)

            if np.any(idx):
                w = T[idx]                         # weights
                S_k = S[:, idx]                    # points

                # weighted mean
                C_new[:, k] = (S_k * w).sum(axis=1) / w.sum()
            else:
                # reinitialize empty cluster
                rand_idx = np.random.randint(M)
                C_new[:, k] = S[:, rand_idx]

        # --- Convergence check ---
        if np.linalg.norm(C_new - C, ord='fro') < tol:
            C = C_new
            break

        C = C_new

    return C


# ============================================================
# Main
# ============================================================


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        description='Performance comparison of differentially private K-means clustering schemes')
    parser.add_argument('--d', default=2, type=int, 
                        help='dimensions')
    parser.add_argument('--N', default=100, type=int, 
                        help='dataset size')
    parser.add_argument('--K', default=2, type=int, 
                        help='Number of clusters')
    parser.add_argument('--r', default=1.0, type=float, 
                        help='Bound on the data')
    parser.add_argument('--N_init_kMeans', default=10, type=int, 
                        help='Number of initializations for K-means (EUGkM, RUGNIK, and non-private)')
    parser.add_argument('--Max_iter_kMeans', default=30, type=int, 
                        help='Maximun number of iterations for Lloyd K-means algorithm (EUGkM, RUGNIK, and non-private)')
    parser.add_argument('--N_dset', default=50, type=int, 
                        help='Number of dataset loops')
    parser.add_argument('--N_noise', default=1000, type=int, 
                        help='Number of noise loops for each dataset')
    parser.add_argument('--seed', default=100, type=int, 
                        help='random seed')
    
    args = parser.parse_args()
    
    d = args.d
    N = args.N
    K = args.K
    r = args.r
    N_dset = args.N_dset
    N_noise = args.N_noise
    N_init_kMeans = args.N_init_kMeans
    Max_iter_kMeans = args.Max_iter_kMeans

    # privacy parameters
    epsilon_all = np.array([0.1, 0.15, 0.25, 0.4, 0.6, 1])    
    N_eps = len(epsilon_all)

    delta = 1 / N**2 # Used for Google's Coreset-based Clustering alone

    wcss0_IBM = np.zeros((N_eps, N_dset, N_noise))
    wcss0_GCC = np.zeros((N_eps, N_dset, N_noise))
    wcss0_EUGkM = np.zeros((N_eps, N_dset, N_noise))
    wcss0_RUGNIK = np.zeros((N_eps, N_dset, N_noise))
    wcss0_nopriv = np.zeros((1, N_dset, N_noise))

    m_EUGkM = np.ones(N_eps)
    m_RUGNIK = np.ones(N_eps)

    start_time = time.time()

    # Initial centroids for (EUGkM, RUGNIK and non-private)
    C_init = np.full((d, K, N_init_kMeans), np.nan)
    for i_init_kMeans in range(N_init_kMeans):
        C_init[:, :, i_init_kMeans], _ = get_cluster_centroids(d, r, K)

    # Determining the number of grids for EUGkM and RUGNIK (ours)
    for eps_loop, epsilon in enumerate(epsilon_all):

        # EUGkM
        m_EUGkM[eps_loop] = max(round((N * epsilon / 10) ** (2 / (2 + d))), 1)

        # RUGNIK
        gam = 1 / (1 + np.sqrt(N / 3))
        eta = (epsilon / d) * np.sqrt(8 * N / 3) / gam
        obj = lambda m: 1/3*N/m**2 + 2*np.sqrt(N/3)/(m*K**(1/d)) + np.sqrt(2)*m**(d/2)/(epsilon*K**(2/d))
        xi = lambda m: m**(d/2 + 2) - eta * gam * K**(1/d) * m - eta * (1 - gam) * K**(2/d)

        m_l = 0
        m_u = 1e4 * m_EUGkM
        if epsilon >= d * np.sqrt(3 * K / (8 * N)) / (1 + np.sqrt(N / 3)):
            m_l = np.floor((eta * K**(2/d)) ** (2 / (4 + d)))
            m_u = np.ceil((eta * K**(1/d)) ** (2 / (2 + d)))
        if epsilon <= (d / 3) * np.sqrt(2 * K) * ((4 / (3 * N)) ** (d / 4)):
            m_u = np.ceil((gam * np.sqrt(3 * N) * eta * K**(2/d)) ** (2 / (4 + d)))

        m_opt0 = root_scalar(xi, bracket=[m_l, m_u], method='brentq').root
        m_opt = np.array([np.floor(m_opt0), np.ceil(m_opt0)])
        m_RUGNIK[eps_loop] = max(m_opt[np.argmin(obj(m_opt))], 1)

    for dset_loop in range(N_dset):

        np.random.seed(args.seed + dset_loop)

        X, _, C0 = generate_clustered_data(d, r, K, N)
        X_T = X.T # transposed data

        data = clustering_params.Data(X_T, r * np.sqrt(d)) # Used for Google's Coreset-based Clustering

        for eps_loop, epsilon in enumerate(epsilon_all):

            privacy_param = clustering_params.DifferentialPrivacyParam(epsilon, delta) # Used for Google's Coreset-based Clustering

            # Obtain the grid centres and histogram 
            S_EUGkM, T_EUGkM = discretize(X, r, m_EUGkM[eps_loop])
            S_RUGNIK, T_RUGNIK = discretize(X, r, m_RUGNIK[eps_loop])
            
            
            for noise_loop in range(N_noise):
                # Run DP clustering once for each noise seed

                np.random.seed(args.seed + 10000 * dset_loop + noise_loop)

                # ----------------------------------------------------
                # IBM diffprivlib K-means implementation
                # ----------------------------------------------------

                model = IBMKMeans(n_clusters=K, epsilon=epsilon, bounds=(-r, r))
                model.fit(X_T)
                C_IBM = model.cluster_centers_.T

                # WCSS
                dist_sq = cdist(X_T, C_IBM.T, 'sqeuclidean')
                wcss0_IBM[eps_loop, dset_loop, noise_loop] = np.sum(np.min(dist_sq, axis=1))/K

                # ----------------------------------------------------
                # Google's Coreset-based Clustering
                # ----------------------------------------------------

                result = clustering_algorithm.private_lsh_clustering(K, data, privacy_param)
                C_GCC = result.centers.T
                if C_GCC.shape[1] < K:
                    C_GCC = np.hstack([C_GCC, np.zeros((d, K - C_GCC.shape[1]))])
                elif C_GCC.shape[1] > K:
                    C_GCC = C_GCC[:, :K]

                # WCSS
                dist_sq = cdist(X_T, C_GCC.T, 'sqeuclidean')
                wcss0_GCC[eps_loop, dset_loop, noise_loop] = np.sum(np.min(dist_sq, axis=1))/K

                # ----------------------------------------------------
                # EUGkM (Ref: D. Su et al., “Differentially Private K-Means Clustering and a Hybrid Approach to Private Optimization,” ACM Trans. Priv. Secur., vol. 20, no. 4, pp. 1–33, 2017.)
                # ----------------------------------------------------

                T_EUGkM_noisy = T_EUGkM + laprnd(T_EUGkM.shape, 0, 1/epsilon)

                best_wcss_EUGkM = np.inf
                for i_init_kMeans in range(N_init_kMeans):
                    C_EUGkM = kmeans_quantized(S_EUGkM, T_EUGkM_noisy, K, init_centroids=C_init[:, :, i_init_kMeans], Max_iter_kMeans=Max_iter_kMeans)
                    
                    # WCSS
                    dist_sq = cdist(X_T, C_EUGkM.T, 'sqeuclidean')
                    wcss_tmp = np.sum(np.min(dist_sq, axis=1))/K
                    if wcss_tmp < best_wcss_EUGkM:
                        best_wcss_EUGkM = wcss_tmp

                wcss0_EUGkM[eps_loop, dset_loop, noise_loop] = best_wcss_EUGkM

                # ----------------------------------------------------
                # RUGNIK (Ours, Ref: G. Muthukrishnan and A. Tandon, “On the Optimal Number of Grids for Differentially Private Non-Interactive K-Means Clustering,” 2026.)
                # ----------------------------------------------------

                T_RUGNIK_noisy = T_RUGNIK + laprnd(T_RUGNIK.shape, 0, 1/epsilon)

                best_wcss_RUGNIK = np.inf
                for i_init_kMeans in range(N_init_kMeans):
                    C_RUGNIK = kmeans_quantized(S_RUGNIK, T_RUGNIK_noisy, K, init_centroids=C_init[:, :, i_init_kMeans], Max_iter_kMeans=Max_iter_kMeans)
                    
                    # WCSS
                    dist_sq = cdist(X_T, C_RUGNIK.T, 'sqeuclidean')
                    wcss_tmp = np.sum(np.min(dist_sq, axis=1))/K
                    if wcss_tmp < best_wcss_RUGNIK:
                        best_wcss_RUGNIK = wcss_tmp

                wcss0_RUGNIK[eps_loop, dset_loop, noise_loop] = best_wcss_RUGNIK

            print(f"Epsilon loop: {100*(eps_loop+1)/N_eps:.2f}% completed")

        # ----------------------------------------------------
        # Non-private K-means clustering
        # ----------------------------------------------------

        best_wcss_no_priv = np.inf
        for i_init_kMeans in range(N_init_kMeans):
            kmeans = KMeans(n_clusters=K, init=C_init[:, :, i_init_kMeans].T, n_init=1, max_iter=Max_iter_kMeans, algorithm='lloyd')
            kmeans.fit(X.T)

            # WCSS
            wcss_tmp = kmeans.inertia_ / K
            if wcss_tmp < best_wcss_no_priv:
                best_wcss_no_priv = wcss_tmp

        wcss0_nopriv[0, dset_loop, :] = best_wcss_no_priv

        print(f"Dataset loop {dset_loop+1}/{N_dset} completed")
        print("=" * 60)
    
    run_time = time.time() - start_time

    wcss_IBM=np.mean(wcss0_IBM, axis=(1, 2))
    wcss_GCC=np.mean(wcss0_GCC, axis=(1, 2))
    wcss_EUGkM=np.mean(wcss0_EUGkM, axis=(1, 2))
    wcss_RUGNIK=np.mean(wcss0_RUGNIK, axis=(1, 2))
    wcss_nopriv=np.mean(wcss0_nopriv, axis=(1, 2))

    print("Total runtime:", run_time)

    np.set_printoptions(precision=3, suppress=True)

    with open(f"results_d_{d}_N_{N}_K_{K}.txt", 'w') as f:
        sys.stdout = f # All print statements below now go to the file
    
        print("=" * 60)
        print("Connifiguration: d =", d, "N =", N, "K =", K)
        print("=" * 60)

        print("\nMean WCSS Values")

        print("IBM:")
        print(wcss_IBM)
        print("GCC:")
        print(wcss_GCC)
        print("EUGkM:")
        print(wcss_EUGkM)
        print("RUGNIK:")
        print(wcss_RUGNIK)
        print("No privacy:")
        print(wcss_nopriv)

        print("\nNumber of grids")
        print("EUGkM:")
        print((m_EUGkM ** d).astype(int))
        print("RUGNIK:")
        print((m_RUGNIK ** d).astype(int))
