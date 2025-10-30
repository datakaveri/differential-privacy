import csv
import math
from .UserData import UserData

class Dataset:
    def __init__(self, users):
        self.users = users

    def sort_by_contribution(self):
        """
        Sort users in-place by number of records (descending).
        Most contributions first.
        """
        self.users.sort(key=lambda user: user.num_records(), reverse=True)

    def add_user(self, user):
        """Add a UserData object to the dataset."""
        self.users.append(user)

    def compute_index_i(self, epsilon):
        """
        Compute index i using epsilon and total number of users.
        """
        n_users = len(self.users)
        index_i = min(math.ceil(2/epsilon), n_users)
        return index_i
    
    def compute_U(self, attr_name):
        """
        Compute the global upper bound U for the attribute.
        U = max(X_i) over all contributions of all users.
        """
        all_values = []
        for user in self.users:
            all_values.extend([float(v) for v in user.get_attribute_values(attr_name)])
        U = max(all_values) if all_values else 0
        return U
    
    def compute_V(self, attr_name):
        """
        Compute the global lower bound V for the attribute.
        V = min(X_i) over all contributions of all users.
        """
        all_values = []
        for user in self.users:
            all_values.extend([float(v) for v in user.get_attribute_values(attr_name)])
        V = min(all_values) if all_values else 0
        return V

    
    def compute_T_epsilon_clipped(self, index_i, U, V, attr_name):
        """
        Compute T_epsilon = m_i * (U-V)
        where:
        - m_i = number of contributions of user at index_i
        - U = max(X_i) over all contributions of all users.
        - V = min(X_i) over all contributions of all users.
        """
        user_i = self.users[index_i-1]
        m_i = user_i.num_records()
        #print(f"m_i: ", m_i)

        T_epsilon_clipped = m_i * (U-V)
        #print(f"T_epsilon_clipped: ", T_epsilon_clipped)

        return T_epsilon_clipped
    
    def compute_T_epsilon_unclipped(self, U, V, attr_name):
        """
        Compute T_epsilon = m_i * (U - V)
        where:
        - m_L = number of contributions of user with maximum number of contributions
        - U = max(X_i) over all contributions of all users.
        - V = min(X_i) over all contributions of all users.
        """
        m_L = self.users[0].num_records()
        #print(f"m_L: ", m_L)

        T_epsilon_unclipped = m_L * (U-V)

        return T_epsilon_unclipped
    
    @classmethod
    def from_dataframe(cls, df):
        dataset = cls([])
        for user_id, group in df.groupby("user_id"):
            user = UserData(user_id)  # only user_id
            for record in group.to_dict(orient="records"):
                user.add_record(record)  # add each record manually
            dataset.add_user(user)
        return dataset

    @classmethod
    def from_csv(cls, csv_file_path):
        """
        Load dataset from CSV.
        Each row becomes a dictionary in UserData.records.
        """
        from collections import defaultdict
        import csv

        user_dict = defaultdict(lambda: UserData(None))

        with open(csv_file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                user_id = row["user_id"]
                if user_dict[user_id].user_id is None:
                    user_dict[user_id].user_id = user_id

                user_dict[user_id].add_record(row)

        users = list(user_dict.values())
        return cls(users)
    
    def to_csv(self, output_file_path, rank_column="user_rank"):
        """
        Write the dataset to a CSV file.
        Adds a separate column for user rank after sorting.
        Records within each user remain intact.
        """
        if not self.users:
            return

        # Get all column names from the first record
        fieldnames = list(self.users[0].records[0].keys())
        if rank_column not in fieldnames:
            fieldnames.append(rank_column)

        with open(output_file_path, mode='w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for rank, user in enumerate(self.users, start=1):
                for record in user.records:
                    record[rank_column] = rank
                    writer.writerow(record)