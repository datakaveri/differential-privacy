import csv
import math
from user_level_histogram_src.userDataHistogram import UserDataHistogram

class DatasetHistogram:
    def __init__(self, users):
        self.users = users

    def add_user(self, user):
        """Add a UserData object to the dataset."""
        self.users.append(user)

    def sort_by_contribution(self):
        """
        Sort users in-place by number of records (descending).
        Most contributions first.
        """
        self.users.sort(key=lambda user: user.num_records(), reverse=True)

    def create_bins(self, k, bin_width, U, V):
        """
        Create k fixed-width non-overlapping contiguous bins:
        [U, U+bin_width), [U+bin_width, U+2*bin_width), ...
        Each bin defaults count to zero.
        """

        bins = []

        for i in range(k):
            lower = U + i * bin_width
            upper = U + (i + 1) * bin_width

            # Clip final bin at V
            if upper > V:
                upper = V

            bins.append([lower, upper, 0])

        return bins

    def sort_to_bins(self, attribute, bins, k, bin_width, U, V):
        for user in self.users:
            for sample in user.records:
                value = sample[attribute]

                if value < U or value > V:
                    continue

                # compute bin index directly
                bin_index = int((value - U) // bin_width)

                if value == V:
                    bin_index = k-1

                # clamp to last bin
                if bin_index >= k:
                    bin_index = k - 1

                bins[bin_index][2] += 1

        return bins
    
    def count_bin(self, bins, k):
        """
        Extract the true histogram counts f from the bins.

        bins : list of k bins in the format [lower, upper, count]
        k    : number of bins

        Returns:
            f : array of length k containing counts per bin
        """
        f = [0] * k

        for i in range(k):
            f[i] = bins[i][2]   # the count field

        return f
    
    # ONLY FOR UNCLIPPED
    def compute_m_star(self):
        """
        m* = number of samples from the user with the highest contribution.
        Assumes users are already sorted in descending order.
        """
        if not self.users:
            return 0
        return self.users[0].num_records()
    
    # ONLY FOR CLIPPED
    def compute_idx(self, k, epsilon):
            n_users = len(self.users)
            index = min(math.ceil((2*k) / epsilon), n_users)
            return index
    
    def compute_C(self, dataset, index):
        return len(dataset.users[index-1].records)
    
    @classmethod
    def from_dataframe(cls, df):
        dataset = cls([])
        for user_id, group in df.groupby("user_id"):
            user = UserDataHistogram(user_id)  # only user_id
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

        user_dict = defaultdict(lambda: UserDataHistogram(None))

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
