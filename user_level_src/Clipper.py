from .UserData import UserData
from .Dataset import Dataset
import csv

class Clipper:
    def __init__(self):
        self.lower_bound = None
        self.upper_bound = None
        self.debug_info = []

    def compute_bounds(self, user_l, user_index, index_i, U,V, T_epsilon, attr_name):
        """
        Compute clipping bounds for a single user.
        """
        Y_l = user_l.sum_attribute(attr_name)

        if user_index +1 >= index_i:
            m_l = user_l.num_records()
            #no clipping for users at or after index i
            #print(f"User {user_index} -> no clipping (after i)")
            A = Y_l - m_l * V
            B = Y_l - m_l * V
            #print(f"T_epsilon: ", T_epsilon)
            #print("A, B : ",A,  B)

        else:
            #print(f"User {user_index} -> clipped (before i)")
           # print(f"Index i: ", index_i)
            m_l = user_l.num_records()
            #print(f"m_l: ", m_l)

            d = m_l * (U-V) - T_epsilon
            #print(f"T_epsilon: ", T_epsilon)
            #print(f"d: ", d)
            #print (f"d: ", {d})
            A = d/2
            B = m_l * (U-V) - (d/2)
            #print("A, B : ",A,  B)

        self.lower_bound = A
        self.upper_bound = B

        return A, B

    def clip_values(self, user_l, attr_name, A, B, V):
        """
        Clip all attribute values of user_l using stored bounds.
        """
        Y_l = user_l.sum_attribute(attr_name)
        m_l = user_l.num_records()
        Y_l_star = Y_l - m_l * V
        clipped_value_star = max(A, min(Y_l_star, B))
        clipped_value = clipped_value_star + m_l*V
        #clipped_value = max(A, min(Y_l, B))
        #print(f"Y_l and clipped value: ", Y_l, clipped_value)
        return Y_l, clipped_value
    
    def clipped_mean(self, dataset, attr_name, index_i, U,V, T_epsilon):
        """
        Compute the clipped mean of an attribute across all users.
        """
        clipped_sums = []
        total_contributions = 0
        self.debug_info = []

        for user_index, user in enumerate(dataset.users):
            A, B = self.compute_bounds(user, user_index, index_i, U,V, T_epsilon, attr_name)
            
            Y_l, clipped_sum = self.clip_values(user, attr_name, A, B, V)
            clipped_sums.append(clipped_sum)
            
            total_contributions += user.num_records()

            self.debug_info.append({
                "user_id": user.user_id,
                "A": A,
                "B": B,
                "Y_l (true_sum)": Y_l,
                "clipped_sum": clipped_sum,
                "num_records": user.num_records()
            })
            
        mean_clipped = sum(clipped_sums) / total_contributions if total_contributions > 0 else 0
        return mean_clipped

    def export_debug_info(self, file_path):
            """
            Export debug info (A, B, true sum, clipped sum) for each user to a CSV file.
            """
            if not self.debug_info:
                print("⚠️ No debug info found. Run clipped_mean() first.")
                return

            fieldnames = ["user_id", "num_records", "A", "B", "Y_l (true_sum)", "clipped_sum"]
            with open(file_path, mode="w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.debug_info)
            #print(f"✅ Debug info written to {file_path}")