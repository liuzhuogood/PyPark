import datetime
import os
import sys


def show_version():
    try:
        exc_file = os.path.realpath(sys.executable)
        ctime = os.stat(exc_file).st_ctime
        return datetime.datetime.fromtimestamp(ctime).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return "无法显示版本"


if __name__ == '__main__':
    print(show_version())