/*
 * Copyright (c) 2014-2015, University of Lugano
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the copyright holders nor the names of it
 *       contributors may be used to endorse or promote products derived from
 *       this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


#include <evpaxos.h>
#include <evpaxos/paxos.h>

#include <algorithm>
#include <iostream>
#include <iterator>
#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <string.h>
#include <sstream>
#include <signal.h>
#include <thread>
#include <mutex>
#include <netinet/tcp.h>
#include <vector>

#include "types/types.h"
#include "scheduler/scheduler.hpp"
#include "graph/graph.hpp"


static int verbose = 0;
static int SLEEP = 2;
static int N_PARTITIONS = 4;
static int REPARTITION_INTERVAL = 1000;
static model::CutMethod CUT_METHOD = model::KAHIP;
static bool RUNNING;

struct callback_args {
	event_base* base;
	kvpaxos::Scheduler<int>* scheduler;
	int request_counter;
	std::mutex* counter_mutex;
};

static void
handle_sigint(int sig, short ev, void* arg)
{
	struct event_base* base = static_cast<event_base*>(arg);
	printf("Caught signal %d\n", sig);
	event_base_loopexit(base, NULL);
	RUNNING = false;
}

static void
deliver(unsigned iid, char* value, size_t size, void* arg)
{
	auto* request = (struct client_message*)value;
	auto* args = (callback_args*) arg;
	auto* scheduler = args->scheduler;
	scheduler ->schedule_and_answer(*request);

	args->counter_mutex->lock();
	args->request_counter++;
	args->counter_mutex->unlock();
}

void
print_throughput(int sleep_duration, int& counter, std::mutex& counter_mutex)
{
	while (RUNNING) {
		std::this_thread::sleep_for(std::chrono::seconds(sleep_duration));
		counter_mutex.lock();
			auto throughput = counter;
			counter = 0;
		counter_mutex.unlock();
		std::cout << "Answered " << throughput << " requests.\n";
	}
}

static void
start_replica(int id, const char* config)
{
	struct event* sig;
	struct event_base* base;
	struct evpaxos_replica* replica;
	deliver_function cb = deliver;
	RUNNING = true;

	base = event_base_new();
	replica = evpaxos_replica_init(id, config, cb, NULL, base);
	if (replica == NULL) {
		printf("Could not start the replica!\n");
		exit(1);
	}

	kvpaxos::Scheduler<int> scheduler(
		REPARTITION_INTERVAL, N_PARTITIONS, CUT_METHOD
	);
	scheduler.run();
	std::mutex counter_mutex;
	struct callback_args args;
	args.base = base;
	args.scheduler = &scheduler;
	args.request_counter = 0;
	args.counter_mutex = &counter_mutex;
	replica->arg = &args;

	std::thread throughput_thread(
		print_throughput, SLEEP, std::ref(args.request_counter), std::ref(counter_mutex)
	);

	sig = evsignal_new(base, SIGINT, handle_sigint, base);
	evsignal_add(sig, NULL);

	signal(SIGPIPE, SIG_IGN);
	event_base_dispatch(base);

	throughput_thread.join();
	event_free(sig);
	evpaxos_replica_free(replica);
	event_base_free(base);
}

static void
usage(std::string prog)
{
	std::cout << "Usage: " << prog << " id [path/to/paxos.conf] [-h] [-v]\n";
	std::cout << "-h, --help Output this message and exit";
	std::cout << "-v, --verbose Print delivered messages";
}

int
main(int argc, char const *argv[])
{
	int id;
	int i = 2;
	const char* config = "../paxos.conf";

	if (argc < 2) {
		usage(std::string(argv[0]));
		exit(1);
	}

	id = atoi(argv[1]);
	if (argc >= 3 && argv[2][0] != '-') {
		config = argv[2];
		i++;
	}

	while (i != argc) {
		auto arg = std::string(argv[i]);
		if (arg == "-h" || arg == "--help") {
			usage(arg);
			exit(1);
		} else if (arg == "-v" || arg == "--verbose") {
			verbose = 1;
		} else {
			usage(arg);
			exit(1);
		}
		i++;
	}

	start_replica(id, config);

	return 0;
}
