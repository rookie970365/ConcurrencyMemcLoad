#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pip install python-memcached
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto

import glob
import gzip
import logging
import os
import sys
from collections import defaultdict, namedtuple
from datetime import datetime
from itertools import islice
from multiprocessing import Process, Queue, current_process, cpu_count
from optparse import OptionParser

from pycparser.ply import cpp
from pymemcache.client import RetryingClient
from pymemcache.client.base import Client
from pymemcache.exceptions import MemcacheUnexpectedCloseError

import appsinstalled_pb2

PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION = cpp
NORMAL_ERR_RATE = 0.01
BATCH_SIZE = 10000
AppsInstalled = namedtuple(
    "AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"]
)


class MyRetryingClient:
    """Класс для создания подключений к memcached"""

    clients = {}

    @classmethod
    def get_client(cls, addr):
        """Метод получения клиента по адресу подключения к memcached"""
        if addr not in cls.clients:
            logging.info("%s подключился к %s", current_process(), addr)
            base_client = Client(addr)
            cls.clients[addr] = RetryingClient(
                base_client,
                attempts=3,
                retry_delay=0.01,
                retry_for=[MemcacheUnexpectedCloseError],
            )
        return cls.clients[addr]


def dot_rename(path):
    head, filename = os.path.split(path)
    os.rename(path, os.path.join(head, "." + filename))


def insert_appsinstalled(memc_addr, memc_client, apps_list, dry_run=False):
    packed_dict = {}
    for appsinstalled in apps_list:
        ua = appsinstalled_pb2.UserApps()
        ua.lat = appsinstalled.lat
        ua.lon = appsinstalled.lon
        key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
        ua.apps.extend(appsinstalled.apps)
        packed = ua.SerializeToString()

        if dry_run:
            logging.debug("%s - %s -> %s", (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            # MyRetryingClient().get_client(memc_addr).set(key, packed)
            packed_dict[key] = packed
    if not dry_run:
        try:
            failed_keys = len(memc_client.set_multi(packed_dict))
            return len(packed_dict) - failed_keys, failed_keys
        except Exception as ex:
            logging.exception("Cannot write to memc %s: %s", memc_addr, ex)
            return False, len(packed_dict)


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`", line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`", line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def producer_task(file, queue):
    """Функция подготовки очереди"""
    logging.info("Start processing file: %s", file.split("/")[-1])
    with gzip.open(file, "rt") as f:
        batch = list(islice(f, BATCH_SIZE))
        count = 1
        while batch:
            queue.put((file, batch))
            batch = list(islice(f, BATCH_SIZE))
            count += 1
        queue.put((file, ["EOF"]))
    logging.info(
        "Sent %s batches to the queue from file: %s", count, file.split("/")[-1]
    )


def consumer_task(queue, options):
    """Функция обработки очереди"""
    stats = {"processed": 0, "errors": 0}
    while True:
        file, batch = queue.get()
        if not batch:
            if file and not stats["processed"]:
                dot_rename(file)
            break
        elif batch == ["EOF"]:
            dot_rename(file)
        else:
            processed, errors = process_batch(batch, options)
            stats["processed"] += processed
            stats["errors"] += errors

    if not file and stats["processed"]:
        err_rate = float(stats["errors"]) / stats["processed"]
        if err_rate < NORMAL_ERR_RATE:
            logging.info(
                "%s Acceptable error rate (%s). Successfull load",
                current_process(),
                err_rate,
            )
        else:
            logging.error(
                "High error rate (%s > %s). Failed load", (err_rate, NORMAL_ERR_RATE)
            )
    else:
        logging.error("There are no files in the directory to process")


def process_batch(batch, options):
    """Функция обработки пакета"""
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    processed, errors = 0, 0
    logging.info("%s processing batch", current_process())
    apps_dict = defaultdict(list)
    for line in batch:
        line = line.strip()
        if not line:
            continue
        appsinstalled = parse_appsinstalled(line)
        if not appsinstalled:
            errors += 1
            continue
        memc_addr = device_memc.get(appsinstalled.dev_type)
        if not memc_addr:
            errors += 1
            logging.error("Unknow device type: %s", appsinstalled.dev_type)
            continue
        # ok = insert_appsinstalled(memc_addr, appsinstalled, False)
        # if ok:
        #     processed += 1
        # else:
        #     errors += 1

        apps_dict[memc_addr].append(appsinstalled)

    for key, value in apps_dict.items():
        ok, err = insert_appsinstalled(
            key, MyRetryingClient().get_client(key), value, False
        )
        processed += ok
        errors += err

    return processed, errors


def main(options):
    q = Queue()
    files = glob.iglob("./data/appsinstalled/*.tsv.gz")
    workers = range(cpu_count())

    start = datetime.now()

    processes = [Process(target=consumer_task, args=(q, options)) for _ in workers]
    for p in processes:
        p.start()
    for file in sorted(files):
        producer_task(file, q)
    for _ in workers:
        q.put(("", []))
    for p in processes:
        p.join()

    logging.info("Execution time: %s", datetime.now() - start)


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == "__main__":
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="./data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(
        filename=opts.log,
        level=logging.INFO if not opts.dry else logging.DEBUG,
        format="[%(asctime)s] %(levelname).1s %(message)s",
        datefmt="%Y.%m.%d %H:%M:%S",
    )
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s", opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s", e)
        sys.exit(1)
