#!/usr/bin/env python
# manage
# Management and admin script for rpctp results.
#
# Author:  Benjamin Bengfort <benjamin@bengfort.com>
# Created: Wed Sep 06 15:27:28 2017 -0400
#
# ID: manage.py [] benjamin@bengfort.com $

"""
Management and admin script for rpctp results.
"""

##########################################################################
## Imports
##########################################################################

import os
import csv
import glob
import json
import argparse

from collections import defaultdict

from fabfile import load_hosts
from fabfile import FIXTURES, HOSTS, RESULTS, SERVER


##########################################################################
## Helper Functions
##########################################################################

def load_jsonl(path):
    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def write_jsonl(rows, path):
    with open(path, 'w') as f:
        for idx, row in enumerate(rows):
            f.write(json.dumps(data)+"\n")

    print("wrote {} rows to {}".format(idx+1, path))


def write_csv(rows, path, fields=None):
    if fields is None:
        rows = list(rows)
        fields = list(rows[0].keys())

    with open(path, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for idx, row in enumerate(rows):
            writer.writerow(row)

    print("wrote {} rows to {}".format(idx+1, path))


##########################################################################
## Commands
##########################################################################

def combine(args):
    hosts = list(load_hosts(args.hosts))
    server = hosts.pop(args.server)
    flatten = not args.type == "json"

    if args.clients:
        results = combine_results(hosts, args.dir, flatten)
        outpath = "results"
    else:
        results = combine_metrics(server, args.dir)
        outpath = "metrics"

    if args.write is None:
        args.write = os.path.join(args.dir, "{}.{}".format(outpath, args.type))

    if args.type == "json":
        write_jsonl(results, args.write)
    elif args.type == "csv":
        write_csv(results, args.write)


def combine_metrics(server, root):
    path = os.path.join(root, server, "metrics*.json")
    for name in glob.glob(path):
        system = name.split("-")[1]
        for row in load_jsonl(name):
            row['system'] = system
            row['host'] = server
            yield row


def combine_results(hosts, root, flatten=False):

    def load_results(hosts, root):
        for host in hosts:
            path = os.path.join(root, host, "results*.json")
            for name in glob.glob(path):
                system = name.split("-")[1]
                for row in load_jsonl(name):
                    row['host'] = host
                    row['system'] = system
                    yield row

    if flatten:
        # Aggregate the results
        results = defaultdict(lambda: defaultdict(list))
        for row in load_results(hosts, root):
            results[row['system']][row['n_clients']].append(row)

        # Yield the aggregated results
        for system, nclients in results.items():
            for clients, rows in nclients.items():
                data = {
                    'system': system,
                    'clients': clients,
                    'duration': 0.0,
                    'messages': 0.0,
                    'maximum latency': None,
                    'minimum latency': None,
                    'throughput': 0.0,
                    'nrows': 0,
                }

                for row in rows:
                    data['nrows'] += 1
                    data['duration'] += float(row['latency (nsec)'])
                    data['messages'] += float(row['messages'])
                    data['throughput'] += float(row['throughput (msg/sec)'])

                    if data['maximum latency'] is None or data['maximum latency'] < row['latency distribution']['maximum']:
                        data['maximum latency'] = float(row['latency distribution']['maximum'])

                    if data['minimum latency'] is None or data['minimum latency'] > row['latency distribution']['minimum']:
                        data['minimum latency'] = float(row['latency distribution']['minimum'])

                data['mean latency'] = data['duration'] / data['messages']
                yield data
    else:
        for row in load_results(hosts, root):
            yield row


##########################################################################
## Main Method
##########################################################################

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="management script for rpctp results",
        epilog="part of the Bengfort toolkit",
    )

    subparsers = parser.add_subparsers(
        title="commands", description="rpctp utilities",
    )

    # Combine Results Parser
    cp = subparsers.add_parser(
        "combine", help="combine results from multiple results files",
    )
    cp.add_argument(
        "-t", "--type", choices=("json", "csv"), default="csv",
        help="specify how the file should be combined"
    )
    cp.add_argument(
        "-c", "--clients", action="store_true",
        help="combine the client-side results rather than the server"
    )
    cp.add_argument(
        "-H", "--hosts", default=HOSTS, type=str, metavar="PATH",
        help="specify the path to the hosts.txt file"
    )
    cp.add_argument(
        "-d", "--dir", default=RESULTS, type=str, metavar="PATH",
        help="specify the directory of the results"
    )
    cp.add_argument(
        "-s", "--server", default=SERVER, type=int, metavar="IDX",
        help="specify the index of the server in the hosts file",
    )
    cp.add_argument(
        "-w", "--write", default=None, metavar="PATH",
        help="specify the path to write the results file"
    )
    cp.set_defaults(func=combine)


    args = parser.parse_args()
    args.func(args)
