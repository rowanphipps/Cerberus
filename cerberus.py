#!/usr/bin/python
from __future__ import print_function, unicode_literals
import argparse, multiprocessing, os, json, paramiko, math, Queue, time, subprocess, threading

data = {}

def main():
	paramiko.util.log_to_file("paramiko.log")
	parser = argparse.ArgumentParser(prog="cerberus")

	subparser = parser.add_subparsers(dest="mode", title="modes", description="", help="all the different actions that can be taken")

	new_args(subparser)
	add_remote_args(subparser)
	add_file_args(subparser)
	list_args(subparser)
	remove_args(subparser)
	update_args(subparser)
	run_args(subparser)

	args = parser.parse_args()

	if args.mode == "new":
		new_project(args)
	elif args.mode == "add-remote":
		add_remote(args)
	elif args.mode == "add-file":
		add_file(args)
	elif args.mode == "list":
		list_remotes(args)
	elif args.mode == "remove":
		remove_remote(args)
	elif args.mode == "update":
		update_remote(args)
	elif args.mode == "run":
		run(args)

def new_project(args):
	if os.path.isfile("cerberus.conf"):
		print("[Error] Existing project already found") 
		sys.exit() # TODO exit with an accurate code
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
			print("[Error] Server already exists in this project.")
			sys.exit()
		if remote["name"] == server["name"]:
			print("[Error] Server with that name already exists in this project.")
			sys.exit()
	
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

def list_remotes(args):
	open_project()
	for server in data["remotes"]:
		if server["name"] == server["location"]:
			print(server["user"] + "@" + server["location"])
		else:
			print(server["user"] + "@" + server["name"] + " (" + server["location"] +")")

def remove_remote(args):
	# TODO: implement this function
	raise NotImplementedError()

def update_remote(args):
	# TODO: upload in parallel
	open_project()
	for server in data["remotes"]:
		_upload_to(server)

def run(args):
	open_project()
	size = args.stop - args.start
	# TODO: raise an error if size is negative

	print("local only: " + str(args.local_only))
	print("remote only: " + str(args.remote_only))

	
	results = multiprocessing.Queue()
	blocks, total_blocks = _create_blocks(args)

	end_event = threading.Event()
	end_event.clear()
	consumers = []
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
		while complete_blocks <= total_blocks:
			print(str(complete_blocks) + " remaining of " + str(total_blocks + 1))
			while not results.empty():
				block_id, result = results.get(False)
				complete_blocks += 1
				for i in result:
					full_results[i[0]] = i[1]

			time.sleep(1)
	except Queue.Empty:
		pass
	end_event.set()
	time.sleep(0.01)
	for c in consumers:
		c.join()

	json.dump(full_results, open(args.output, "w"))

	# TODO: loading bar / progress counter that doesn't print a new line each time

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

def _upload_to(remote):
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
		os.path.abspath(os.path.expanduser(file)),
		remote["user"] + "@" + str(remote["location"]) + ":~/.cerberus/" + data["name"] + "/"]
	print("Uploading " + file + " ", end="")
	subprocess.call(rsync_args)
	print(" ...Done")		# TODO: detect errors cleanly

def open_project():
	if not os.path.isfile("cerberus.confg"):
		print("[Error] No project found")	# TODO exit with an accurate code
		sys.exit()
	tmp_data = json.load(open("cerberus.confg", "rU"))
	for k in tmp_data.keys():
		data[k] = tmp_data[k]

def close_project():
	json.dump(data, open("cerberus.confg", "w"))

def new_args(subparser):
	new_parser = subparser.add_parser("new", help="Creates a new cerberus project in the current directory")
	new_parser.add_argument("name", help="The name for the new cerberus project")
	new_parser.add_argument("target", help="The function that is to be run to the network.  eg. myfile.main ")
	new_parser.add_argument("--use-local-controller", dest="local", action="store_true", help="Use a controller.py found in the directory of this project.  Otherwise it uses the one at ~/.Cerberus/controller.py")

def add_remote_args(subparser):
	parser = subparser.add_parser("add-remote", help="Adds another remote server to the collection for the current project.  Note: this server must be setup to use keys and not a password to connect via ssh.")
	parser.add_argument("-a", "--alias", help="The alias (nickname) for the remote server")
	parser.add_argument("location", help="Address or ip of the remote server")
	parser.add_argument("user", help="Username to use on the remote server")
	parser.add_argument("-c", "--cores", type=int, default=0, help="Specifies how many threads to run.  Defaults to the number of cpu cores.")
	parser.add_argument("-u", "--upload", action="store_true", help="Upload files to server immediately")

def add_file_args(subparser):
	parser = subparser.add_parser("add-file", help="Adds files to the project")
	parser.add_argument("file", nargs="+", help="file(s) to be added to the project")

def list_args(subparser):
	parser = subparser.add_parser("list", help="Lists all known remote servers for this project")

def remove_args(subparser):
	parser = subparser.add_parser("remove", help="Removes a remote server from the project")
	parser.add_argument("name", help="The address or alias of the server to be removed")

def update_args(subparser):
	parser = subparser.add_parser("update", help="Deplpys code and data files to all remote servers for this project")

def run_args(subparser):
	parser = subparser.add_parser("run", help="Runs the project")

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
						print("working on " + str(block_id) + " " + str(bounds))
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
				print(result)
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