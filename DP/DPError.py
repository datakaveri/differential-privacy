class DPError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message

    def to_dict(self):
        return {
            "code": self.code,
            "message": self.message
        }
