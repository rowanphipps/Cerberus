from __future__ import print_function
import multiprocessing, sys, json

cpus = int(sys.argv[1])

module = __import__(sys.argv[2])

function_name = sys.argv[3]

with open("log.txt", "w") as f:

	print("starting logging", file=f)

	function = eval("module." + function_name)

	if cpus == 0:
		cpus = multiprocessing.cpu_count()

	print("cpus: " + str(cpus), file=f)
	f.flush()
	try:
		pool = multiprocessing.Pool(cpus)

		block = raw_input()
		print("recvd block: " + block, file=f)
		f.flush()
		while block != "end":
			data = json.loads(block)
			inlist = list(range(data["start"], data["stop"]))
			result = pool.map(function, inlist)
			out = zip(inlist, result)
			final_result = json.dumps({"solution":out})
			print("result: " + final_result, file=f)
			print(final_result+"$", file=sys.stdout)
			sys.stdout.flush()
			f.flush()
			block = raw_input()
			print("recvd block: " + block, file=f)

	finally:
		print("terminating...", file=f)
		pool.terminate()
		pool.join()
		print("done")