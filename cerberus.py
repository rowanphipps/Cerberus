#!/usr/bin/python
from __future__ import print_function, unicode_literals
import argparse, multiprocessing, os, json, paramiko, math, Queue, time, subprocess, threading, sys

data = {}

def main():
	paramiko.util.log_to_file("paramiko.log")
	parser = argparse.ArgumentParser(prog="cerberus")

	subparser = parser.add_subparsers(
		dest="mode",
		title="modes",
		description="Cerberus has a number of different subcommands that each do different things.  For detailed help type `cerberus command -h`")

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
	if os.path.isfile("cerberus.conf"):
		sys.exit("Existing project already found") 
	data["name"] = args.name
	file, function = args.target.split(".")
	data["function"] = function.replace("()", "")
	data["file"] = file
	data["files"] = []
	data["remotes"] = []
	data["local"] = args.local
	close_project()

def add_remote(args):
	open_project()
	remote = {"location":args.location, "cores":args.cores, "user":args.user}
	if args.alias != None:
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
	data["remotes"].sort(key=lambda x:x["name"])
	if args.upload:
		_upload_to(remote)
	close_project()

def add_file(args):
	open_project()
	for name in args.file:
		data["files"].append(name)
	close_project()

def list_data(args):
	open_project()
	if len(data["remotes"]) > 0:
		print("Servers:")
		for server in data["remotes"]:
			if server["name"] == server["location"]:
				print(server["user"] + "@" + server["location"])
			else:
				print(server["user"] + "@" + server["name"] + " (" + server["location"] +")")
	else:
		print("No servers added")

	print("Included files and directories:")
	print(data["file"] + ".py")
	print("\n".join(data["files"]))

def remove_remote(args):
	open_project()
	for i in range(len(data["remotes"]) - 1, -1, -1):
		remote = data["remotes"][i]
		if remote["name"] == args.name:
			_remove_server(remote)
			data["remotes"].pop(i)

	close_project()

def remove_files(args):
	open_project()
	for fname in args.file:
		if fname in data["files"]:
			data["files"].remove(fname)
		else:
			print("File not found in project: " + fname)
	close_project()

def update_remote(args):
	# TODO: upload in parallel
	open_project()
	for server in data["remotes"]:
		_upload_to(server)

def run(args):
	open_project()
	size = args.stop - args.start
	if size < 1:
		sys.exit("stop - start must be at least 1")
	
	results = multiprocessing.Queue()
	blocks, total_blocks = _create_blocks(args)

	end_event = threading.Event()
	end_event.clear()
	consumers = []
	try:
		if not args.local_only:
			for server in data["remotes"]:
				cons = remoteRunner(server, blocks, results, data["name"], data["file"], data["function"], end_event)
				cons.start()
				consumers.append(cons)
		
		if not args.remote_only:
			cons = localRunner(blocks, results, data["file"], data["function"], end_event)
			cons.start()
			consumers.append(cons)

		full_results = {}
		complete_blocks = 0
		try:
			while complete_blocks < total_blocks:
				print("\r" + str(complete_blocks) + " complete of " + str(total_blocks), end="")
				sys.stdout.flush()
				while not results.empty():
					block_id, result = results.get(False)
					complete_blocks += 1
					for i in result:
						full_results[i[0]] = i[1]

				time.sleep(0.5)
		except Queue.Empty:
			pass
		print("\r" + str(complete_blocks) + " complete of " + str(total_blocks))
		sys.stdout.flush()

		end_event.set()
		time.sleep(0.01)
		for c in consumers:
			c.join()

		json.dump(full_results, open(args.output, "w"))
		print("Done")
	except KeyboardInterrupt:
		print("\nAborting")
		end_event.set()
		for c in consumers:
			c.join()

def _create_blocks(args):
	blocks = multiprocessing.JoinableQueue()

	if args.block_size == 0:
		block_size = 10 ** math.floor(math.log10(size))
	else: block_size = args.block_size
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

def _remove_server(server):
	ssh_string = server["user"] + "@" + str(server["location"])
	rm_args = ["ssh", ssh_string, "rm", "-rf", ".cerberus/" + data["name"]]
	print("Deleting files from  " + ssh_string + " ", end="")
	proc = subprocess.Popen(rm_args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
	proc.wait()
	if proc.returncode != 0:
		print("Error")
		print(proc.sterr.readline().strip())
	else:
		print(" ...Done")

def _upload_to(remote):
	_ensure_dir(".cerberus", remote)
	_ensure_dir(".cerberus/" + data["name"], remote)

	program_name = data["file"] + ".py"
	_upload_file(program_name, remote)
	for filename in data["files"]:
		_upload_file(filename, remote)

	if data["local"]:
		_upload_file("controller.py", remote)
	else:
		_upload_file("~/.cerberus/controller.py", remote)

def _upload_file(file, remote):
	rsync_args = ["rsync", "-rc",
		os.path.expanduser(file),
		remote["user"] + "@" + str(remote["location"]) + ":~/.cerberus/" + data["name"] + "/"]
	print("Uploading " + file + " ", end="")
	proc = subprocess.Popen(rsync_args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
	proc.wait()
	if proc.returncode != 0:
		print("Error")
		print(proc.sterr.readline().strip())
	else:
		print(" ...Done")

def _ensure_dir(name, remote):
	ssh_args = ["ssh", remote["user"] + "@" + str(remote["location"]), "mkdir", name]
	proc = subprocess.Popen(ssh_args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
	proc.wait()
	err = proc.stderr.readline().strip()
	if not err.endswith("File exists"):
		sys.exit(err)

def open_project():
	if not os.path.isfile("cerberus.confg"):
		sys.exit("No project found")	# TODO exit with an accurate code
	tmp_data = json.load(open("cerberus.confg", "rU"))
	for k in tmp_data.keys():
		data[k] = tmp_data[k]

def close_project():
	json.dump(data, open("cerberus.confg", "w"))

def new_args(subparser):
	parser = subparser.add_parser("new", help="Creates a new cerberus project in the current directory")
	parser.set_defaults(func=new_project)
	parser.add_argument("name", help="The name for the new cerberus project")
	parser.add_argument("target", help="The function that is to be run to the network.  eg. myfile.main ")
	parser.add_argument("--use-local-controller", dest="local", action="store_true", help="Use a controller.py found in the directory of this project.  Otherwise it uses the one at ~/.Cerberus/controller.py")

def add_remote_args(subparser):
	parser = subparser.add_parser("add-server", help="Adds another remote server to the collection for the current project.  Note: this server must be setup to use keys and not a password to connect via ssh.")
	parser.set_defaults(func=add_remote)
	parser.add_argument("-a", "--alias", help="The alias (nickname) for the remote server")
	parser.add_argument("location", help="Address or ip of the remote server")
	parser.add_argument("user", help="Username to use on the remote server")
	parser.add_argument("-c", "--cores", type=int, default=0, help="Specifies how many threads to run.  Defaults to the number of cpu cores.")
	parser.add_argument("-u", "--upload", action="store_true", help="Upload files to server immediately")

def add_file_args(subparser):
	parser = subparser.add_parser("add-file", help="Adds files to the project")
	parser.set_defaults(func=add_file)
	parser.add_argument("file", nargs="+", help="file(s) to be added to the project")

def list_args(subparser):
	parser = subparser.add_parser("list", help="Lists all known remote servers and included files for this project")
	parser.set_defaults(func=list_data)

def remove_remote_args(subparser):
	parser = subparser.add_parser("remove-server", help="Removes a remote server from the project and deletes this projects files from the server.  If there are duplicate entries with the same name both will be removed.")
	parser.set_defaults(func=remove_remote)
	parser.add_argument("name", help="The address or alias of the server to be removed")

def remove_file_args(subparser):
	parser = subparser.add_parser("remove-file", help="Removes a file from the project.  This will not delete any files that are on remote servers.")
	parser.set_defaults(func=remove_files)
	parser.add_argument("file", nargs="+", help="The name of the file to be removed")

def update_args(subparser):
	parser = subparser.add_parser("update", help="Deploys code and data files to all remote servers for this project")
	parser.set_defaults(func=update_remote)

def run_args(subparser):
	parser = subparser.add_parser("run", help="Runs the project")
	parser.set_defaults(func=run)
	parser.add_argument("-s", "--start", action="store", default=0, type=int, help="Specifies the value to start incrementing from.  Defaults to 0 (inclusive)")

	parser.add_argument("stop", type=int, help="Specifies the value to increment to (exclusive)")
	parser.add_argument("output", help="Specifies a file to put the output into in json form")
	parser.add_argument("-b", "--block-size", action="store", default=0, type=int, help="Specifies how large to make the blocks.  If left out block size will be determined by the number of values to be calculated.")
	group = parser.add_mutually_exclusive_group()
	group.add_argument("-l", "--local-only", action="store_true", help="Only run the program locally")
	group.add_argument("-r", "--remote-only", action="store_true", help="Only run the program remotely")


class remoteRunner(multiprocessing.Process):
	def __init__(self, server, queue, result_queue, name, module_name, function, end_event):
		multiprocessing.Process.__init__(self)
		self.server = server
		self.q = queue
		self.res = result_queue
		self.end = end_event
		self.name = name
		self.module_name = module_name
		self.function = function

	def run(self):
		args = ["python", ".cerberus/" + self.name + "/controller.py", str(self.server["cores"]), self.module_name, self.function]
		cmd = " ".join(args)
		
		with paramiko.SSHClient() as c:
			try :
				self.client = c
				self.client.load_system_host_keys()
				self.client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
				self.client.connect(str(self.server["location"]), username=str(self.server["user"]))
				print("connected to " + self.server["name"])
				rem_in, rem_out, rem_err = self.client.exec_command(cmd)	
				
				try:
					while not self.end.is_set():
						block_id, bounds = self.q.get(False)
						data = {"start": bounds[0], "stop": bounds[1]}

						print(json.dumps(data), file=rem_in)			# send data as json

						out_string = ""
						while not out_string.endswith("$"):
							partial = rem_out.readline()
							out_string += partial.strip()
							time.sleep(0.5)

						result = json.loads(out_string.strip("$"))		# recv result as json
						self.res.put((block_id, result["solution"]))			# add result to result queue

						self.q.task_done()				# call task_done to declare that we are finished with this block
				except Queue.Empty:
					pass
			finally:
				print("end", file=rem_in)

class localRunner(multiprocessing.Process):
	def __init__(self, queue, results, module_name, function, end_event):
		multiprocessing.Process.__init__(self)
		self.q = queue
		self.res = results
		self.end = end_event
		self.mod = __import__(module_name)
		module = self.mod
		self.function = eval("module." + function)

	def run(self):
		pool = multiprocessing.Pool(max(multiprocessing.cpu_count() - 1, 1))
		try:
			while not self.end.is_set():
				block_id, bounds = self.q.get(False)
				
				inlist = list(range(bounds[0], bounds[1]))
				result = pool.map(self.function, inlist)
				out = zip(inlist, result)
				self.res.put((block_id, out))
				self.q.task_done()

		except Queue.Empty:
			pass

		finally:
			pool.close()
			pool.join()

if __name__ == '__main__':
	main()