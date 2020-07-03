# KVPaxos

KVPaxos is a key-value distributed storage system that uses Paxos and Parallel State Machine Replication to ensure consistency among replicas. It's developed as a prototype to measure latency and throughput when using state partitioning and balanced graph partitioning to schedule requests among threads, it includes 4 graph repartition algorithms to be used during execution: METIS, KaHIP, FENNEL and ReFENNEL.

KVPaxos is a prototype and so it does not cover many corner and common cases, it should not be used as it is in a real deploy context, but can be used as a starting point to other projects.


## Build

CMake is used to build the project, along with Conan to control dependecies. Conan downloads the following dependencies:

* LibEvent 2.1.11
* toml11
* tbb 2020.1

Dependencies not present in Conan are added as submodules, so make sure to recursively clone submodules too when cloning the project. For some reason that I've not cracked, `libpaxos` in `deps/libpaxos` is often cloned in an older commit, a `git checkout master` in the directory may be necessary.

Conan only controls dependecies that are directly used by KVPaxos, dependecies used by submodules need to be installed separately. The submodules and their dependecies are:

* LibPaxos
    * LibEvent 2+
* KaHIP
    * Scons
    * Argtable
    * OpenMPI
* METIS
    * No external dependencies to download.

Help would be appreciated in order to make Conan include those packages and link them to the submodules, preferably without making changes directly to submodules :).

## Usage

Inside the build folder, a folder `bin` will have the two executables, the replica and the client. The replica is started as follows:

```
    ./replica id path_to_paxos_conf
```

The arguments are:
* id - Replica's id.
* path_to_paxos_conf - Path to paxo's configuration file.

The client is started as follows:

```
    ./client reply_port replica_id path_to_paxos_conf path_to_requests (-v|percentage)
```

The arguments are:
* reply_port - Port in which the client will listen to replies from the replica.
* replica_id - Id of the replica that the client will connect to.
* path_to_paxos_conf - Path to paxo's configuration file.
* path_to_requests - Path to request's file.
* -v - Print full information of all recieved answers.
* percentage - Print this specified percentage of request's latency.

A paxos configuration file specifies Paxos characteristics, such as number of replicas and their addresses. An exemple of a configuration file can be found on the LibPaxos project, [here](https://github.com/gabrieltron/libpaxos/blob/master/paxos.conf).

A request's file is a file that specifies the requests to be sent from the client to the replica. They are toml files separated in two lists, load requests and requests. Client will wait the answer of all load requests, that populates de storage, before sending the other requests. The file format is as follows the exemple:

```
load_requests = [
["1", "0", ""],
["1", "1", ""],
["1", "2", ""],
["1", "3", ""],
["1", "4", ""],
]
requests = [
["0", "2", ""],
["1", "0", ""],
["2", "0", "3"],
["2", "0", "3"],
["2", "0", "3"],
]
```
The first field is the operation, they can be:
* 0 - READ;
* 1 - WRITE;
* 2 - SCAN;

The second field is the key where the operation will be performed, and the third is used to pass args, such as scan length.

### Output
The client will output message's delay, if `-v` is used, in a CSV format, where the first column is EPOCH and the second is the delay.
The replica will output throughput, always in a CSV format, where the first column is EPOCH and the second is the delay.
