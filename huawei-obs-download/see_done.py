#!/usr/bin/env python3

"""查看已下载数量."""

import subprocess
import sys
import time

from conf import download_storage_path


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ["local", "online"]:
        print("参数错误")
        sys.exit(0)

    storage_path = download_storage_path[sys.argv[1]]
    while True:
        subprocess.run(["du", storage_path, "-sh"])
        time.sleep(60)


if __name__ == "__main__":
    main()
