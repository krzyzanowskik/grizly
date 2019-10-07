class Config(dict):
    def __init__(self):
        dict.__init__(self)
        
    def from_dict(self, d):
        for key in d:
            self[key] = d[key]