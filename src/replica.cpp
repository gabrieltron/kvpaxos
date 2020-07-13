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
#include <chrono>
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

#include "request/request_generation.h"
#include "types/types.h"
#include "scheduler/scheduler.hpp"
#include "graph/graph.hpp"


using toml_config = toml::basic_value<
	toml::discard_comments, std::unordered_map, std::vector
>;

static int verbose = 0;
static int SLEEP = 1;
static int N_PARTITIONS = 4;
static bool RUNNING;

struct replica_args {
	event_base* base;
	event* signal;
	kvpaxos::Scheduler<int>* scheduler;
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
	auto* args = (struct replica_args*) arg;
	auto* scheduler = args->scheduler;
	scheduler->schedule_and_answer(*request);
}

void
print_throughput(int sleep_duration, kvpaxos::Scheduler<int>* scheduler)
{
	auto already_counted = 0;
	while (RUNNING) {
		std::this_thread::sleep_for(std::chrono::seconds(sleep_duration));
		auto throughput = scheduler->n_executed_requests() - already_counted;
		std::cout << std::chrono::system_clock::now().time_since_epoch().count() << ",";
		std::cout << throughput << "\n";
		already_counted += throughput;
	}
}

static struct evpaxos_replica*
initialize_evpaxos_replica(int id, const toml_config& config)
{
	deliver_function cb = deliver;
	auto* base = event_base_new();
	auto paxos_config = toml::find<std::string>(config, "paxos_config");
	auto* replica = evpaxos_replica_init(id, paxos_config.c_str(), cb, NULL, base);

	auto* sig = evsignal_new(base, SIGINT, handle_sigint, base);
	evsignal_add(sig, NULL);
	signal(SIGPIPE, SIG_IGN);

	auto* args = new replica_args();
	args->base = base;
	args->signal = sig;
	replica->arg = args;

	return replica;
}

static kvpaxos::Scheduler<int>*
initialize_scheduler(const toml_config& config)
{
	auto repartition_method_s = toml::find<std::string>(
		config, "repartition_method"
	);
	auto repartition_method = model::string_to_cut_method.at(
		repartition_method_s
	);
	auto repartition_interval = toml::find<int>(
		config, "repartition_interval"
	);
	auto* scheduler = new kvpaxos::Scheduler<int>(
		repartition_interval, N_PARTITIONS, repartition_method
	);

	auto initial_requests = toml::find<std::string>(
		config, "requests_path"
	);
	if (not initial_requests.empty()) {
		auto populate_requests = std::move(
			workload::import_requests(initial_requests, "load_requests")
		);
		scheduler->process_populate_requests(populate_requests);
	}

	return scheduler;
}

static void
free_replica(struct evpaxos_replica* replica)
{
	auto* args = (struct replica_args*) replica->arg;
	event_free(args->signal);
	event_base_free(args->base);
	delete args->scheduler;
	free(args);
	evpaxos_replica_free(replica);
}

static void
start_replica(int id, const toml_config& config)
{
	struct evpaxos_replica* replica;
	RUNNING = true;

	replica = initialize_evpaxos_replica(id, config);
	if (replica == nullptr) {
		printf("Could not start the replica!\n");
		exit(1);
	}

	auto* scheduler = std::move(initialize_scheduler(config));
	scheduler->run();
	auto* args = (struct replica_args*) replica->arg;
	args->scheduler = scheduler;

	std::thread throughput_thread(
		print_throughput, SLEEP, scheduler
	);

	event_base_dispatch(args->base);

	throughput_thread.join();
	free_replica(replica);
}

static void
usage(std::string prog)
{
	std::cout << "Usage: " << prog << " id config\n";
}

int
main(int argc, char const *argv[])
{
	if (argc < 3) {
		usage(std::string(argv[0]));
		exit(1);
	}

	auto id = atoi(argv[1]);
	const auto config = toml::parse(argv[2]);

	start_replica(id, config);

	return 0;
}
