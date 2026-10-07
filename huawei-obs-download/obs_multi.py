#!/usr/bin/env python3

import argparse
import hashlib
import logging
import os
import shutil
import sys
import time

import gevent
from obs import ObsClient

from conf import ACCESS_KEY, SECRET_KEY, OBS_SERVER, download_storage_path

logs_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")


class Utils:
    """封装函数库."""

    @staticmethod
    def md5sum(filename):
        md5 = hashlib.md5()
        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        # 因OBS返回的ETAG特殊, 故在此格式化处理.
        return f'"{md5.hexdigest()}"'

    @staticmethod
    def build_file_logs(logger_name, log_path=None, level=logging.INFO):
        """创建文件日志.

        :param logger_name: 日志名称
        :param log_path: 日志存放路径
        :param level: 日志级别
        """

        log_dir = logs_path if not log_path else log_path
        log_file = os.path.join(log_dir, f"{logger_name}.log")
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        formatter = logging.Formatter(
            "{asctime} - {name} - {levelname} - {message}", style="{"
        )
        fh = logging.FileHandler(log_file)
        fh.setFormatter(formatter)

        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        logger.addHandler(fh)

        return logger


class HuaWeiDownLoad:
    """华为资源下载."""

    def __init__(self, access_key, secret_key, logger=None):
        """下载器构建.

        :param access_key: 存取键
        :param secret_key: 密匙
        """

        self._connect = ObsClient(
            access_key_id=access_key, secret_access_key=secret_key, server=OBS_SERVER
        )
        self._download_bucket = os.path.join(logs_path, "download_bucket.txt")
        self._next_bucket = None  # 当前执行bucket位置

        self._set_progress_bucket()

        self._print = logger.info if logger else print

    def _set_progress_bucket(self):
        if os.path.exists(self._download_bucket):
            with open(self._download_bucket, "r") as f:
                bucket = f.readline().strip()
                if bucket:
                    self._next_bucket = bucket
                else:
                    self._next_bucket = None

    def get_all_bucket(self):
        """返回所有bucket."""

        response = self._connect.listBuckets()
        return [getattr(item, "name") for item in response.body.buckets]

    def get_all_bucket_info(self):
        """查看所有bucket信息, 用于核实数据及下载前磁盘空间规划."""

        buckets = self.get_all_bucket()
        total_info = dict(objectNumbers=0, sizes=0)

        # 1G = 1024**3 bytes
        for bucket in buckets:
            response = self._connect.getBucketStorageInfo(bucket)
            self._print(f"{bucket} : {vars(response.body)}")

            total_info["sizes"] += int(getattr(response.body, "size"))
            total_info["objectNumbers"] += int(getattr(response.body, "objectNumber"))

        total_info["human_sizes"] = f"{total_info['sizes'] / 1024 ** 3}G"
        self._print(f"Total : {total_info}")

    def download_all_buckets(self, env):
        """下载所有桶里数据."""

        buckets = self.get_all_bucket()

        if buckets:
            download_path = download_storage_path[env]

            if not self._next_bucket:
                self._print(f'{"*" * 40}All Bucket Is Starting.{"*" * 40}')

            for bucket in buckets:
                # 跳过已执行bucket.
                if self._next_bucket and bucket != self._next_bucket:
                    continue

                # 记录执行bucket位置.
                self._next_bucket = bucket
                with open(self._download_bucket, "w") as f:
                    f.write(f"{self._next_bucket}")

                hua_wei = HuaWeiBucket(
                    bucket, max_keys=500, download_path=download_path
                )
                hua_wei.download_all_object()

            self._print(f'{"*" * 40}All Bucket Is Finished.{"*" * 40}')


class HuaWeiBucket:
    def __init__(self, bucket_name, max_keys=0, download_path=None, debug=False):
        """下载桶构建.

        :param bucket_name: 桶名
        :param max_keys: 请求返回数量
        :param download_path: 下载存放目录
        :param debug: 是否调试模式, 开发者自用
        """

        self._connect = ObsClient(
            access_key_id=ACCESS_KEY, secret_access_key=SECRET_KEY, server=OBS_SERVER
        )
        self._download_path = download_path
        self._bucket_name = bucket_name

        self._max_keys = max_keys
        self._is_truncated = True
        self._next_marker = None  # 当前执行paging位置

        self._debug = debug

        bucket_logs = os.path.join(logs_path, bucket_name)
        if not os.path.exists(bucket_logs):
            os.makedirs(bucket_logs)

        self._print = Utils.build_file_logs("obs_hwc_s3", bucket_logs).info

        self._download_marker = os.path.join(bucket_logs, "download_marker.txt")
        self._download_failure = os.path.join(bucket_logs, "download_failure.txt")

        self._set_progress_marker()

    def _set_progress_marker(self):
        """对翻页和桶的进度设置."""

        if os.path.exists(self._download_marker):
            with open(self._download_marker, "r") as f:
                marker = f.readline().strip()
                if marker:
                    record = marker.split(":", 1)
                    if len(record) == 2:
                        self._is_truncated = record[0] == "True"
                        self._next_marker = record[1]
                    else:
                        self._is_truncated = True
                        self._next_marker = None
                else:
                    self._is_truncated = True
                    self._next_marker = None

    def _get_all_object(self):
        """返回所有object."""

        object_keys = None
        try:
            response = self._connect.listObjects(
                bucketName=self._bucket_name,
                marker=self._next_marker,
                max_keys=self._max_keys,
            )
        except Exception as ex:
            self._print(f"listObject ==> {ex}")
        else:
            self._is_truncated = getattr(response.body, "is_truncated")
            self._next_marker = getattr(response.body, "next_marker")

            if self._is_truncated:
                with open(self._download_marker, "w") as f:
                    f.write(f"{self._is_truncated}:{self._next_marker}")

            object_keys = [
                (getattr(item, "etag"), getattr(item, "key"))
                for item in response.body.contents
            ]

        return object_keys

    def _log_download_failure(self, info):
        """记录下载失败的对象.

        :param info: 信息(下载失败的key)
        """

        with open(self._download_failure, "a") as f:
            f.write(f"{info}\n")

    def _check_object_and_catalog(self, object_md5_key, bucket_path=None):
        """检测目录和对象是否存在.

        :param object_md5_key: (对象校验码, 对象键)
        :param bucket_path: 桶路径
        """

        if isinstance(object_md5_key, tuple):
            etag, object_key = object_md5_key
        else:
            etag, object_key = None, object_md5_key

        # 每桶成独立目录.
        bucket_base_path = bucket_path if bucket_path else self._download_path

        # 如key只是个目录 or 文件已存在则直接跳过.
        if not os.path.basename(object_key):
            return 2

        # 如目录则构建存放路径.
        if "/" in object_key:
            object_path = os.path.join(bucket_base_path, *object_key.split("/"))
            down_load_path = os.path.join(
                bucket_base_path, *os.path.dirname(object_key).split("/")
            )
        else:
            object_path = os.path.join(bucket_base_path, object_key)
            down_load_path = bucket_base_path

        if os.path.exists(object_path) and Utils.md5sum(object_path) == etag:
            return 1

        # 目录如不存在则创建.
        if not os.path.exists(down_load_path):
            os.makedirs(down_load_path)

        return down_load_path

    def download_object(self, object_md5_key, bucket_path=None):
        """获得对象保存到本地.

        :param object_md5_key: (对象校验码, 对象键)
        :param bucket_path: 桶路径
        """

        if isinstance(object_md5_key, tuple):
            etag, object_key = object_md5_key
        else:
            etag, object_key = None, object_md5_key

        down_load_path = self._check_object_and_catalog(object_md5_key, bucket_path)
        if down_load_path in [1, 2]:
            return 1

        try:
            self._connect.getObject(self._bucket_name, object_key, down_load_path)
        except Exception as ex:
            self._print(f"getObject ==> {ex}")
            if self._debug:
                import traceback

                traceback.print_exc()
            return 0
        else:
            # 校验文件下载正确性.
            base_name = os.path.basename(object_key)
            if etag:
                return int(
                    Utils.md5sum(os.path.join(down_load_path, base_name)) == etag
                )
            else:
                return 1

    def download_all_object(self):
        """获得所有对象保存到本地."""

        # 每桶每目录, 如不存在则创建.
        bucket_path = os.path.join(self._download_path, self._bucket_name)
        if not os.path.exists(bucket_path):
            os.makedirs(bucket_path)

        i = 0 if self._debug else None
        info = "Debug is done." if self._debug else "work is done."

        def pack_download(_item, _bucket_path):
            """封装下载为异步所用."""

            _result = self.download_object(_item, bucket_path=_bucket_path)
            if not _result:
                self._print(f"DownloadFailure ==> {_item}")
                self._log_download_failure(f"{_item[0]}:{_item[1]}")
            else:
                self._print(f"DownLoad ==> {_item}")

        while self._is_truncated:
            keys = self._get_all_object()
            if keys:
                tasks = [
                    gevent.spawn(pack_download, item, bucket_path) for item in keys
                ]
                gevent.joinall(tasks)

            if i is not None:
                i += 1
                if i == 5:
                    break

        self._print(f'{"=" * 40}Download Failure is starting{"=" * 40}')

        try:
            # RuntimeError: maximum recursion depth exceeded.
            self.download_failure(bucket_path=bucket_path)
        except Exception as ex:
            self._print(ex)

        self._print(f'{"=" * 40}Download Failure is Done.{"=" * 40}')

        self._print(info)

    def download_failure(self, bucket_path=None):
        """正常下载完成后, 再次下载曾经失败的对象.

        :param bucket_path: 桶存储路径
        """

        if os.path.exists(self._download_failure):
            with open(self._download_failure, "r") as f:
                lines = f.readlines()

            if lines:
                second_failure = []
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue

                    # 仅按首个':'拆分, 避免对象key含':'时数据损坏.
                    parts = line.split(":", 1)
                    if len(parts) == 2:
                        item = (parts[0], parts[1])
                    else:
                        item = (None, parts[0])

                    result = self.download_object(item, bucket_path=bucket_path)

                    # 失败的对象再次下载重试, 因校验码总是失败的情况.
                    if not result:
                        result = self.download_object(item[-1], bucket_path=bucket_path)

                    if not result:
                        self._print(f"DownloadFailure ==> {item}")
                        second_failure.append(item)
                    else:
                        self._print(f"DownLoad ==> {item[-1]}")

                os.remove(self._download_failure)
                if second_failure:
                    with open(self._download_failure, "a") as f:
                        for item in second_failure:
                            if item:
                                f.write(f"{item[0]}:{item[1]}\n")

                    # 静默1分钟再对失败对象尝试, 排除时间段内网络异常.
                    time.sleep(60)

                    # 递归再次处理失败的下载对象.
                    self.download_failure(bucket_path=bucket_path)

    def path_object_mov(self, object_md5_key, bucket_path=None):
        """纠正错误存放路径, 原该放入文件夹但放在桶里.

        :param object_md5_key: (对象校验码, 对象键)
        :param bucket_path: 桶路径
        """

        _, object_key = object_md5_key
        down_load_path = self._check_object_and_catalog(object_md5_key, bucket_path)

        # 如为文件夹直接跳过.
        if down_load_path == 2:
            return 1

        try:
            self._print(f"PathObjectMove ==> {object_key}")

            # 只对包含文件夹的做移动操作.
            if "/" in object_key:
                base_name = os.path.basename(object_key)

                if down_load_path == 1:
                    path_correct = True
                else:
                    path_correct = os.path.exists(
                        os.path.join(down_load_path, base_name)
                    )

                path_error = os.path.exists(os.path.join(bucket_path, base_name))

                # 如都存在删错误存放滴.
                if path_correct and path_error:
                    os.remove(os.path.join(bucket_path, base_name))

                # 如正确位置不存在错误位置存在则移动.
                if not path_correct and path_error:
                    shutil.move(
                        os.path.join(bucket_path, base_name),
                        os.path.join(down_load_path, base_name),
                    )
        except Exception as ex:
            self._print(f"path_object_mov ==> {ex}")
            return 0
        else:
            return 1

    def path_correction(self):
        """纠正错误存放路径, 原该放入文件夹但放在桶里."""

        # 每桶每目录, 如不存在则创建.
        bucket_path = os.path.join(self._download_path, self._bucket_name)
        if not os.path.exists(bucket_path):
            os.makedirs(bucket_path)

        log_bucket = os.path.join(logs_path, self._bucket_name)
        if not os.path.exists(log_bucket):
            os.makedirs(log_bucket)

        corr_failure_path = os.path.join(log_bucket, "correction_failure.log")
        if os.path.exists(corr_failure_path):
            os.remove(corr_failure_path)

        self._print(f'{"=" * 50} Path correction is start. {"=" * 50}')
        while self._is_truncated:
            keys = self._get_all_object()
            if keys:
                with open(corr_failure_path, "a") as f:
                    for item in keys:
                        result = self.path_object_mov(item, bucket_path=bucket_path)
                        if not result:
                            self._print(f"MoveObjectFailure ==> {item}")
                            f.write(f"{item}\n")

        self._print(f'{"=" * 50} Path correction is done. {"=" * 50}')

    def unicode_not_supported(self, object_md5_key):
        # 每桶每目录, 如不存在则创建.
        bucket_path = os.path.join(self._download_path, self._bucket_name)
        if not os.path.exists(bucket_path):
            os.makedirs(bucket_path)

        self.download_object(object_md5_key, bucket_path=bucket_path)

    def again_failure_supported(self):
        # 每桶每目录, 如不存在则创建.
        bucket_path = os.path.join(self._download_path, self._bucket_name)
        if not os.path.exists(bucket_path):
            os.makedirs(bucket_path)

        self._print(f'{"=" * 40}Download Failure is starting{"=" * 40}')

        try:
            # RuntimeError: maximum recursion depth exceeded.
            self.download_failure(bucket_path=bucket_path)
        except Exception as ex:
            self._print(ex)

        self._print(f'{"=" * 40}Download Failure is Done.{"=" * 40}')


def main():
    """任务执行."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-e",
        "--env",
        dest="env",
        help="which environment script is running In ['local', 'online'].",
    )
    parser.add_argument(
        "-b", "--bucket", dest="bucket", help="Which bucket will be downloaded."
    )
    parser.add_argument(
        "-a",
        "--action",
        dest="action",
        default="download",
        help="Which action will be done In ['download', 'verify', 'failure'].",
    )

    options = parser.parse_args()

    if (
        not options.env
        or options.env.lower() not in ["local", "online"]
        or not options.bucket
    ):
        print("参数错误")
        sys.exit(0)

    # 根据local or online 运行环境.
    download_path = download_storage_path[options.env.lower()]
    hua_wei = HuaWeiBucket(
        options.bucket, max_keys=500, download_path=download_path, debug=False
    )

    if options.action.lower() == "download":
        hua_wei.download_all_object()
    elif options.action.lower() == "verify":
        hua_wei.path_correction()
    elif options.action.lower() == "failure":
        hua_wei.again_failure_supported()
    else:
        print("^_^：你还想干嘛!?")


def minor(mode):
    """使用supervisor的情况.

    :param mode: 运行模式, ['supervisor', 'perfection']中其一.

    supervisor缺点：
    依赖于的设置, numprocs_start必须0, numprocs必须为桶的数量;
    获取到的buckets无法共享, 需要多次请求;
    """

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-e",
        "--env",
        dest="env",
        help="which environment script is running In ['local', 'online'].",
    )
    parser.add_argument(
        "index",
        type=int,
        nargs="?",
        default=0,
        help="bucket index for supervisor mode.",
    )

    options = parser.parse_args()
    if not options.env or options.env.lower() not in ["local", "online"]:
        print("参数错误")
        sys.exit(0)

    mode = mode.strip().lower()
    if mode not in ["supervisor", "perfection"]:
        print("模式错误")
        sys.exit(0)

    # 根据local or online 运行环境.
    download_path = download_storage_path[options.env.lower()]
    buckets = HuaWeiDownLoad(ACCESS_KEY, SECRET_KEY).get_all_bucket()

    if mode == "supervisor":
        print(
            f"Doing Bucket: {buckets[options.index]}; supervisor PID: {options.index}"
        )

        hua_wei = HuaWeiBucket(
            buckets[options.index], max_keys=500, download_path=download_path
        )
        hua_wei.download_all_object()

    elif mode == "perfection":
        for bucket in buckets:
            pid = os.fork()
            if pid == 0:
                print(f"Doing Bucket: {bucket}; SubProcess PID: {os.getpid()}")

                hua_wei = HuaWeiBucket(
                    bucket, max_keys=500, download_path=download_path
                )
                hua_wei.download_all_object()
                os._exit(0)


if __name__ == "__main__":
    main()
    # minor('supervisor')
    # minor('perfection')
