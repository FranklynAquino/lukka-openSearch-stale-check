class OpenSearchResponseObject:
    def __init__(self,timestamp,label_app_name, pod_name,job_id=None):
        self.timestamp = timestamp
        self.label_app_name = label_app_name
        self.pod_name = pod_name
        self.job_id = job_id
        
    def to_string(self):
        return (self.timestamp, self.label_app_name, self.pod_name,self.job_id)