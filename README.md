#Cerberus#
======

Cerberus is a tool to allow python programs to be run on both the host and remote machines to increase performance.  In order for a function to be deployed with Cerberus it must take a single integer as a parameter and return its result in a form that is serializable via json.

## Requirements
Cerberus uses ssh to connect to and communicate with remote machines.  To eliminate the need for the user to type in the password for each machine every time they want to run code or upload files ssh must be configured to authenticate using keys and not passwords.  (look up ssh-keygen and ssh-copy-id for details)

Due to its dependence on ssh and rsync Cerberus most likely is not windows compatible as either the host or a remote machine.  (this is untested though so good luck!)  It is known to be compatible with macOS as both a host and remote and is probably compatible with Linux (it is known to work using raspbian as a remote)

Currently it has the following dependencies:

- [Paramiko](http://www.paramiko.org/)

## Installation
`Cerberus.py` should be placed somewhere on your path and can be renamed to `Cerberus` without the `.py` on the end.  `controller.py` should be placed in `~/.cerberus/`

##Usage

To use Cerberus you will first need to write a function like the one described above that takes a single int as input and returns its result as something compatible with json.  It can rely on data and or code in other files but these will need to be added to the Cerberus project explicitly.  In addition you need to make sure that any libraries the code depends on are preinstalled on all the machines the code is to be run on.

### Setup
Once you have a function that you want to run using Cerberus, you will need to create a new Cerberus project by running `cerberus new` with the name for the project and the function you want to run as arguments. eg.
```
cerberus new test_project my_file.some_function
```
In the above example `some_function` would be a function found in `my_file.py` that would take a single int as a parameter and return the results of its calculation.  Running this command would generate a file called `cerberus.confg` that contains information about the Cerberus project.  At this point it is possible to use Cerberus to run the function locally utilizing all of its CPU cores but first we probably want to add some remote servers for more compute resources.

Servers can be add to the project by running `cerberus add` followed by the address of the server and the username of the user on the server that is to be used.  The current user on the host machine must be setup to login via ssh using keys otherwise Cerberus is unable to use the server.  More options can be found using `cerberus add -h`.  Once servers have been added to the project they can be listed out by running `cerberus list`

### Running Code

Before we are able to run code we need to push the code and supporting files out to the servers, which is done by calling `cerberus update`.  This will upload the file originally specified for the project, a control script, and (if present) a data directory.  These are all uploaded to `~/.cerberus/project_name/` on the server under the specified user's account.

When we want to run the function with cerberus we call `cerberus run` and specify the value to end at as well as the name of a file to write the output to.  The output is written the the given filename in json format.  By default the function is called on every integer from 0 (inclusive) to the given stop value (exclusive).  There are options to specify the starting value as well as to restrict runs to either only the host machine (`-l`) and only the remote servers (`-r`).  In addition there is an option to specify the block size to use.  Keep in mind that ideally you want the block size to be larger than the number of cores on any machine running the code.  Also keep in mind that there is overhead involved in switching between blocks so don't make the too small, but that if they are too large some machines may finish well before others causing them to sit and idle uselessly.

The list on commands can be found with `cerberus -h` and help for a specific command can be found using `cerberus command_name -h`.
