# fabfile
# Fabric command definitions for running RPC benchmarks
#
# Author:  Benjamin Bengfort <benjamin@bengfort.com>
# Created: Tue Sep 05 10:57:04 2017 -0400
#
# ID: fabfile.py [] benjamin@bengfort.com $

"""
Fabric command definitions for running RPC benchmarks
"""

##########################################################################
## Imports
##########################################################################

import os
import random

from copy import copy
from fabric.api import env, run, cd, parallel, get, put
from fabric.api import task, execute, settings

##########################################################################
## Environment
##########################################################################

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
RESULTS = os.path.join(FIXTURES, "results")
HOSTS = os.path.join(FIXTURES, "hosts.txt")
SERVER = 0

## Load Hosts
def load_hosts(path=HOSTS):
    with open(path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                yield line

## Paths
workspace = "/data/rpctp"
repos = [
    "~/workspace/go/src/github.com/bbengfort/rtreq",
    "~/workspace/go/src/github.com/bbengfort/echo",
]

# Fabric ENV
env.colorize_errors = True
env.hosts = list(load_hosts())
env.use_ssh_config = True
env.forward_agent = True

# For EC2
env.user="ubuntu"
env.key_filename="~/.ssh/bengfortaws.pem"


##########################################################################
## Helper Functions
##########################################################################

def pproc_command(commands):
    """
    Creates a pproc command from a list of command strings.
    """
    commands = " ".join([
        "\"{}\"".format(command) for command in commands
    ])
    return "pproc {}".format(commands)


def round_robin(n, host, hosts=env.hosts):
    """
    Returns a number n (of clients) for the specified host, by allocating the
    n clients evenly in a round robin fashion. For example, if hosts = 3 and
    n = 5; then this function returns 2 for host[0], 2 for host[1] and 1 for
    host[2].
    """
    # copy the hosts and remove server
    hosts = copy(hosts)
    hosts.pop(SERVER)

    num = n / len(hosts)
    idx = hosts.index(host)
    if n % len(hosts) > 0 and idx < (n % len(hosts)):
        num += 1
    return num


def add_suffix(path, suffix=None):
    if suffix:
        base, ext = os.path.splitext(path)
        path = "{}-{}{}".format(base, suffix, ext)
    return path


def unique_name(path, start=0, maxtries=1000):
    for idx in range(start+1, start+maxtries):
        ipath = add_suffix(path, idx)
        if not os.path.exists(ipath):
            return ipath

    raise ValueError(
        "could not get a unique path after {} tries".format(maxtries)
    )


def parse_bool(val):
    if isinstance(val, str):
        val = val.lower().strip()
        if val in {'yes', 'y', 'true', 't', '1'}:
            return True
        if val in {'no', 'n', 'false', 'f', '0'}:
            return False
    return bool(val)


##########################################################################
## Fabric commands
##########################################################################

@parallel
def update():
    """
    Update the go libraries by pulling the repository and running install
    """
    for repo in repos:
        with cd(repo):
            run("git pull")
            run("go install ./...")


@parallel
def cleanup():
    """
    Cleans up the results file so the experiments can be run again
    """
    for name in ("results.json", "metrics.json"):
        path = os.path.join(workspace, name)
        run("rm -f {}".format(path))


@parallel
def bench(clients=1,server=SERVER,cmd="rtreq",sync=False,addr=None):
    """
    Runs the server on the host specified by the server index, then runs
    clients in a round robin fashion on all other hosts.
    """
    # Parse arguments
    server = int(server)
    sync = parse_bool(sync)
    clients = int(clients)

    if cmd not in {"echgo", "rtreq"}:
        raise ValueError("command should be either echgo or rtreq")

    # Get the server
    server = env.hosts[server]
    
    if server == env.host:
        # Run the serve command
        cmd = "{} serve -u 45s".format(cmd)
        if sync: cmd += " -s"
        with cd(workspace):
            run(cmd)
    else:
        command = []
        addr = "{}:4157".format(addr or server)
        for idx in range(round_robin(clients, env.host)):
            seed = random.randint(1, 1000000)
            command.append(
                "{} bench -c {} -s {} -a {}".format(cmd, clients, seed, addr)
            )

        # Run the client commands
        with cd(workspace):
            run("sleep 3")
            if command:
                run(pproc_command(command))


@parallel
def getmerge(path=RESULTS, suffix=None, server=SERVER):
    """
    Get the results.json and save it with the specified suffix to localpath.
    """
    server = env.hosts[server]

    if server == env.host:
        name = "metrics.json"
    else:
        name = "results.json"

    remote = os.path.join(workspace, name)
    local = os.path.join(path, env.host, add_suffix(name, suffix))
    local  = unique_name(local)
    get(remote, local)
