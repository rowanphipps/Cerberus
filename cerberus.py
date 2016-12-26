#!/usr/bin/python
"""Module docstring."""
from __future__ import print_function, unicode_literals
import argparse
import json
import math
import multiprocessing
import os
import Queue
import subprocess
import sys
import threading
import time


def main():
    """Run the cerberus command from the provided command line args."""
    parser = argparse.ArgumentParser(prog="cerberus")

    subparser = parser.add_subparsers(
        dest="mode",
        title="modes",
        description="""Cerberus has a number of different subcommands that each
            do different things. For detailed help type `cerberus command
            -h`""")

    new_args(subparser)
    add_remote_args(subparser)
    add_file_args(subparser)
    list_args(subparser)
    remove_remote_args(subparser)
    remove_file_args(subparser)
    update_args(subparser)
    run_args(subparser)

    args = parser.parse_args()
    args.func(args)


def new_project(args):
    """Create a new project for the given args.

    Exits wih code 1 if a project already exists in this directory.
    """
    data = {}
    if os.path.isfile("cerberus.conf"):
        sys.exit("Existing project already found")
    data["name"] = args.name
    file, function = args.target.split(".")
    data["function"] = function.replace("()", "")
    data["file"] = file
    data["files"] = []
    data["remotes"] = []
    data["local"] = args.local
    close_project(data)


def add_remote(args):
    """Add a remote server to the current project."""
    data = open_project()
    remote = {
        "location": args.location,
        "cores": args.cores,
        "user": args.user}

    if args.alias is not None:
        name = args.alias
    else:
        name = args.location
    remote["name"] = name

    for server in data["remotes"]:
        if remote["location"] == server["location"]:
            sys.exit("Server already exists in this project.")
        if remote["name"] == server["name"]:
            sys.exit("Server with that name already exists in this project.")

    data["remotes"].append(remote)
    data["remotes"].sort(key=lambda x: x["name"])
    if args.upload:
        _upload_to(remote, data)
    close_project(data)


def add_file(args):
    """Add one or more files to the current project."""
    data = open_project()
    for name in args.file:
        data["files"].append(name)
    close_project(data)


def list_data(args):
    """List all servers and files associated with this project."""
    data = open_project()
    if len(data["remotes"]) > 0:
        print("Servers:")
        for server in data["remotes"]:
            if server["name"] == server["location"]:
                print(server["user"] + "@" + server["location"])
            else:
                print(
                    server["user"] + "@" + server["name"] + " ("
                    + server["location"] + ")")
    else:
        print("No servers added")

    print("Included files and directories:")
    print(data["file"] + ".py")
    print("\n".join(data["files"]))


def remove_remote(args):
    """Remove a server from the project.

    If there are files on the server from this project they will be
    removed as well.
    """
    data = open_project()
    for i in range(len(data["remotes"]) - 1, -1, -1):
        remote = data["remotes"][i]
        if remote["name"] == args.name:
            _remove_server(remote, data)
            data["remotes"].pop(i)

    close_project(data)


def remove_files(args):
    """Remove files from the project."""
    data = open_project()
    for fname in args.file:
        if fname in data["files"]:
            data["files"].remove(fname)
        else:
            print("File not found in project: " + fname)
    close_project(data)


def update_remote(args):
    """Upload project files to each of the servers."""
    # TODO: upload in parallel
    data = open_project()
    for server in data["remotes"]:
        _upload_to(server, data)


def run(args):
    """Run the project on local and remote machines."""
    data = open_project()
    if args.stop - args.start < 1:
        sys.exit("stop - start must be at least 1")

    results = multiprocessing.Queue()
    blocks, total_blocks = _create_blocks(args)

    end_event = threading.Event()
    end_event.clear()
    consumers = []
    try:
        if not args.local_only:
            for server in data["remotes"]:
                cons = multiprocessing.Process(
                    name=server["name"], target=remote_runner,
                    args=(server, blocks, results, data, end_event))
                cons.start()
                consumers.append(cons)

        if not args.remote_only:
            cons = multiprocessing.Process(
                name="local", target=local_runner,
                args=(blocks, results, data, end_event))
            cons.start()
            consumers.append(cons)

        full_results = {}
        complete_blocks = 0
        try:
            while complete_blocks < total_blocks:
                print(
                    "\r" + str(complete_blocks), "complete of",
                    str(total_blocks),
                    "(" + str(len(multiprocessing.active_children)),
                    "consumers running)",
                    sep=" ", end="")
                sys.stdout.flush()
                while not results.empty():
                    _, result = results.get(False)      # block_id
                    complete_blocks += 1
                    for i in result:
                        full_results[i[0]] = i[1]
                time.sleep(0.5)
        except Queue.Empty:
            pass
        print("\r" + str(complete_blocks), "complete of", str(total_blocks),
              sep=" ")
        sys.stdout.flush()

        end_event.set()
        time.sleep(0.01)
        for consumer in consumers:
            consumer.join()

        json.dump(full_results, open(args.output, "w"))
        print("Done")
    except KeyboardInterrupt:
        print("\nAborting")
        end_event.set()
        for consumer in consumers:
            consumer.join()


def _create_blocks(args):
    """Create the blocks that will be distrubuted over the network."""
    blocks = multiprocessing.JoinableQueue()

    if args.block_size == 0:
        block_size = 10 ** math.floor(math.log10(args.stop - args.start))
    else:
        block_size = args.block_size
    low = args.start
    high = low + block_size

    block_id = 0
    blocks.put((block_id, (low, min(high, args.stop))))

    while high < args.stop:
        low += block_size
        high += block_size
        block_id += 1
        blocks.put((block_id, (low, min(high, args.stop))))

    return blocks, block_id + 1


def _remove_server(server, data):
    """Remove files from a particular server."""
    rm_args = [
        "ssh",
        server["user"] + "@" + str(server["location"]),
        "rm", "-rf", ".cerberus/" + data["name"]]
    print("Deleting files from "
          + server["user"] + "@" + str(server["location"]) + " ", end="")
    sys.stdout.flush()
    proc = subprocess.Popen(
        rm_args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    proc.wait()
    if proc.returncode != 0:
        print("Error")
        print(proc.stderr.readline().strip())
    else:
        print(" ...Done")


def _upload_to(remote, data):
    """Upload project files to a single server."""
    _ensure_dir(".cerberus", remote)
    _ensure_dir(".cerberus/" + data["name"], remote)

    program_name = data["file"] + ".py"
    _upload_file(program_name, remote, data["name"])
    for filename in data["files"]:
        _upload_file(filename, remote, data["name"])

    if data["local"]:
        _upload_file("controller.py", remote, data["name"])
    else:
        _upload_file("~/.cerberus/controller.py", remote, data["name"])


def _upload_file(file, remote, name):
    """Upload a single file to a server."""
    rsync_args = [
        "rsync", "-rc", os.path.expanduser(file),
        remote["user"] + "@" + remote["location"]
        + ":~/.cerberus/" + name + "/"]
    print("Uploading " + file + " ", end="")
    sys.stdout.flush()
    proc = subprocess.Popen(
        rsync_args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    proc.wait()
    if proc.returncode != 0:
        print("Error")
        print(proc.stderr.readline().strip())
    else:
        print(" ...Done")


def _ensure_dir(name, remote):
    """Ensure that the specified directory is present on the server.

    If it is not present it is then created.
    """
    ssh_args = [
        "ssh", remote["user"] + "@" + str(remote["location"]), "mkdir", name]
    proc = subprocess.Popen(
        ssh_args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    proc.wait()
    err = proc.stderr.readline().strip()
    if not err.endswith("File exists"):
        sys.exit(err)


def open_project():
    """Open the project in this directory and returns its information.

    Exits if a project is not found.
    """
    if not os.path.isfile("cerberus.confg"):
        sys.exit("No project found")
    data = json.load(open("cerberus.confg", "rU"))
    return data


def close_project(data):
    """Dump data back into cerberus.confg."""
    json.dump(data, open("cerberus.confg", "w"))


def new_args(subparser):
    """Add the subparser for the new command."""
    parser = subparser.add_parser(
        "new", help="Creates a new cerberus project in the current directory")
    parser.set_defaults(func=new_project)
    parser.add_argument(
        "name", help="The name for the new cerberus project")
    parser.add_argument(
        "target",
        help="The function that is to be run to the network. eg. myfile.main ")
    parser.add_argument(
        "--use-local-controller", dest="local", action="store_true",
        help="""Use a controller.py found in the directory of this project.
                Otherwise it uses the one at ~/.Cerberus/controller.py""")


def add_remote_args(subparser):
    """Add the subparser for the add-server command."""
    parser = subparser.add_parser(
        "add-server",
        help="""Adds another remote server to the collection for the current
        project.  Note: this server must be setup to use keys and not a
        password to connect via ssh.""")
    parser.set_defaults(func=add_remote)
    parser.add_argument(
        "-a", "--alias", help="The alias (nickname) for the remote server")
    parser.add_argument("location", help="Address or ip of the remote server")
    parser.add_argument("user", help="Username to use on the remote server")
    parser.add_argument(
        "-c", "--cores", type=int, default=0,
        help="""Specifies how many threads to run.
        Defaults to the number of cpu cores.""")
    parser.add_argument(
        "-u", "--upload", action="store_true",
        help="Upload files to server immediately")


def add_file_args(subparser):
    """Add the subparser for the add-file command."""
    parser = subparser.add_parser(
        "add-file", help="Adds files to the project")
    parser.set_defaults(func=add_file)
    parser.add_argument(
        "file", nargs="+", help="file(s) to be added to the project")


def list_args(subparser):
    """Add the subparser for the list command."""
    parser = subparser.add_parser(
        "list",
        help="""Lists all known remote servers and included files.""")
    parser.set_defaults(func=list_data)


def remove_remote_args(subparser):
    """Add the subparser for the remove-server command."""
    parser = subparser.add_parser(
        "remove-server",
        help="""Removes a remote server from the project and deletes this
        projects files from the server.  If there are duplicate entries with
        the same name both will be removed.""")
    parser.set_defaults(func=remove_remote)
    parser.add_argument(
        "name", help="The address or alias of the server to be removed")


def remove_file_args(subparser):
    """Add the subparser for the remove-file command."""
    parser = subparser.add_parser(
        "remove-file",
        help="""Removes a file from the project. This will not delete any files
        that are on remote servers.""")
    parser.set_defaults(func=remove_files)
    parser.add_argument(
        "file", nargs="+", help="The name of the file to be removed")


def update_args(subparser):
    """Add the subparser for the update command."""
    parser = subparser.add_parser(
        "update",
        help="""Deploys code and data files to all remote servers""")
    parser.set_defaults(func=update_remote)


def run_args(subparser):
    """Add the subparser for the run command."""
    parser = subparser.add_parser("run", help="Runs the project")
    parser.set_defaults(func=run)
    parser.add_argument(
        "-s", "--start", action="store", default=0, type=int,
        help="""Specifies the value to start incrementing from.
        Defaults to 0. (inclusive)""")

    parser.add_argument(
        "stop", type=int,
        help="Specifies the value to increment to (exclusive)")
    parser.add_argument(
        "output", help="Specifies a file to put the output into in json form")
    parser.add_argument(
        "-b", "--block-size", action="store", default=0, type=int,
        help="""Specifies how large to make the blocks. If left out block size
        will be determined by the number of values to be calculated.""")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-l", "--local-only", action="store_true",
        help="Only run the program locally")
    group.add_argument(
        "-r", "--remote-only", action="store_true",
        help="Only run the program remotely")


def remote_runner(server, blocks, results_queue, data, end_event):
    """Connect to a remote server and run the project on it."""
    remote_cmds = [
        "python", ".cerberus/" + data["name"] + "/controller.py",
        str(server["cores"]), data["file"], data["function"]]

    ssh_cmds = [
        "ssh", server["user"] + "@" + server["location"]]

    try:
        print(ssh_cmds + remote_cmds)
        proc = subprocess.Popen(
            ssh_cmds + remote_cmds, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("connected to " + server["name"])

        try:
            while not end_event.is_set():
                block_id, bounds = blocks.get(False)   # block_id
                data = {"start": bounds[0], "stop": bounds[1]}

                # send data as json
                print(json.dumps(data), file=proc.stdin)

                out_string = ""
                while not out_string.endswith("$"):
                    partial = proc.stdout.readline()
                    out_string += partial.strip()
                    time.sleep(0.5)

                # receive result as json
                result = json.loads(out_string.strip("$"))

                # add result to result queue
                results_queue.put((block_id, result["solution"]))

                # call task_done when we are finished
                blocks.task_done()
        except Queue.Empty:
            pass
    finally:
        # print("end", file=rem_in)
        # proc.communicate(input="end")
        print("end", file=proc.stdin)


def local_runner(blocks, results_queue, data, end_event):
    """Set up a process to complete part of a run locally."""
    module = __import__(data["file"])
    function = getattr(module, data["function"])

    pool = multiprocessing.Pool(max(multiprocessing.cpu_count() - 1, 1))
    try:
        while not end_event.is_set():
            block_id, bounds = blocks.get(False)

            inlist = list(range(bounds[0], bounds[1]))
            result = pool.map(function, inlist)
            out = zip(inlist, result)
            results_queue.put((block_id, out))
            blocks.task_done()

    except Queue.Empty:
        pass

    finally:
        pool.close()
        pool.join()


if __name__ == '__main__':
    main()
