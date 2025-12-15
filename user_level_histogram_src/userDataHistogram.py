import csv
import math

class UserDataHistogram:
    def __init__(self, user_id):
        self.user_id = user_id
        self.records = []

    def add_record(self, record_dict):
        self.records.append(record_dict)

    def num_records(self):
        return len(self.records)