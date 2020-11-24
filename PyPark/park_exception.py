

class ServiceException(Exception):
    pass


class NoServiceException(Exception):
    def __init__(self, *args, **kwargs):
        pass

