from PyPark.cons import StatusCode


class Result:

    def __init__(self, is_success, status_code, data, msg=""):
        self.status_code = status_code
        self.is_success = is_success
        self.data = data
        self.msg = msg

    @staticmethod
    def success(data=None, msg=""):
        return Result(True, status_code=StatusCode.SUCCESS, data=data, msg=msg)

    @staticmethod
    def error(code=StatusCode.ERROR, data=None, msg=""):
        return Result(False, status_code=code, data=data, msg=msg)
