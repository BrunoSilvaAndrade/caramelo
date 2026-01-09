class Message:
    def __init__(self, topic, key=None, value=None, headers:list=None):
        self.topic = topic
        self.key = key
        self.value = value
        self.headers = headers
