class CustomValueError(Exception):
    def __init__(self, message, code):
        self.code = code
        self.message = message

    def __str__(self):
        return f"Error {self.code}: {self.message}"

class ValidateDPConfig():
    def __init__(self, config):
        self.epsilon = config['differential_privacy']['dp_epsilon']
        

    def validate_epsilon(self):
        
        if self.epsilon < 0:
            raise CustomValueError("Epsilon must be non-negative", "1110")
    
    def validate_dp_config(self):
        self.validate_epsilon()
