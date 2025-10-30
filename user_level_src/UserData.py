import csv
import math

class UserData:
    def __init__(self, user_id):
        self.user_id = user_id
        self.records = []

    def add_record(self, record_dict):
        self.records.append(record_dict)

    def num_records(self):
        return len(self.records)

    def get_attribute_values(self, attr_name):
        """
        Return a list of all values for the given attribute for this user.
        """
        return [record[attr_name] for record in self.records if attr_name in record]
    
    def sum_attribute(self, attr_name):
        """
        Return the sum of all values of the given attribute for this user.
        """
        return sum(float(v) for v in self.get_attribute_values(attr_name))