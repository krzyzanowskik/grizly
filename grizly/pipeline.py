import dask

class Config(dict):
    def __init__(self):
        dict.__init__(self)
        
    def from_dict(self, d):
        for key in d:
            self[key] = d[key]

class Pipeline():
    def __init__(self):
        self.config = Config()
        self.tasks = []
    
    def register(self, *args):
        for func in args:
            self.tasks.append(dask.delayed(func))
        return self
    
    def compute(self):
        """
        some docs
        """
        output = dask.delayed()(self.tasks).compute()
        return output