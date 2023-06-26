class MultiMatchObject:
    def __init__(self,query):
        self.type = 'Phrase'
        self.query = str(query)
        self.lenient = True
        
    def get_multi_match_object(self,query):
        return {'multi_match':
                    {'type':self.type,
                    'query':query,
                    'lenient':self.lenient}}