
class FileHandlingException(Exception):

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class DBHandlingException(Exception):

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class SparkSessionException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class TransformationFailureException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class VisualizationFailureException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)       