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
#include <mutex>
#include <netinet/tcp.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>
#include <thread>
#include <unordered_map>
#include <vector>

#include "evclient/evclient.h"
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
static bool RUNNING = true;
const int VALUE_SIZE = 128;


void
print_throughput(int sleep_duration, int n_requests, kvpaxos::Scheduler<int>* scheduler)
{
	auto already_counted = 0;
	while (RUNNING) {
		std::this_thread::sleep_for(std::chrono::seconds(sleep_duration));
		auto executed_requests = scheduler->n_executed_requests();
		auto throughput = executed_requests - already_counted;
		std::cout << std::chrono::system_clock::now().time_since_epoch().count() << ",";
		std::cout << throughput << "\n";
		already_counted += throughput;

		if (executed_requests == n_requests) {
			break;
		}
	}
}

static kvpaxos::Scheduler<int>*
initialize_scheduler(
	int n_requests,
	const toml_config& config)
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
		n_requests, repartition_interval, N_PARTITIONS,
		repartition_method
	);

	auto initial_requests = toml::find<std::string>(
		config, "load_requests_path"
	);
	if (not initial_requests.empty()) {
		auto populate_requests = std::move(
			workload::import_requests(initial_requests, "load_requests")
		);
		scheduler->process_populate_requests(populate_requests);
	}

	scheduler->run();
	return scheduler;
}

std::vector<struct client_message>
to_client_messages(
	std::vector<workload::Request>& requests)
{
	std::vector<struct client_message> client_messages;
	auto counter = 0;
	for (auto i = 0; i < requests.size(); i++) {
		auto& request = requests[i];
		struct client_message client_message;
		client_message.sin_port = htons(0);
		client_message.id = i;
		client_message.type = request.type();
		client_message.key = request.key();
		if (request.args().empty()) {
			memset(client_message.args, '#', VALUE_SIZE);
			client_message.args[VALUE_SIZE] = '\0';
			client_message.size = VALUE_SIZE;
		}
		else {
			for (auto i = 0; i < request.args().size(); i++) {
				client_message.args[i] = request.args()[i];
			}
			client_message.args[request.args().size()] = 0;
			client_message.size = request.args().size();
		}
		client_message.record_timestamp = false;

		client_messages.emplace_back(client_message);
	}

	return client_messages;
}

static std::unordered_map<int, time_point>
execute_requests(
	kvpaxos::Scheduler<int>& scheduler,
	std::vector<struct client_message>& requests,
	int print_percentage)
{
	srand (time(NULL));
	std::unordered_map<int, time_point> end_timestamp;

	for (auto& request: requests) {
		if (print_percentage >= rand() % 100 + 1) {
	    	auto timestamp = std::chrono::system_clock::now();
    		auto kv = std::make_pair(request.id, timestamp);
    		end_timestamp.insert(kv);
			request.record_timestamp = true;
		}
		scheduler.schedule_and_answer(request);
	}
	return end_timestamp;
}

bool sort_pair(const std::pair<int,time_point> &a,
              const std::pair<int,time_point> &b)
{
    return (a.second.time_since_epoch().count() < b.second.time_since_epoch().count());
}

static std::vector<std::pair<int, time_point>>
order_execution_timestamps(kvpaxos::Scheduler<int>& scheduler)
{
	std::vector<std::pair<int, time_point>> ordered_timestamps;
	auto timestamp_vectors = scheduler.execution_timestamps();
	for (auto& timestamp_vector: timestamp_vectors) {
		ordered_timestamps.insert(
			ordered_timestamps.end(),
			std::make_move_iterator(timestamp_vector.begin()),
			std::make_move_iterator(timestamp_vector.end())
		);
	}

	std::sort(
		ordered_timestamps.begin(),
		ordered_timestamps.end(),
		sort_pair
	);

	return ordered_timestamps;
}

static void
run(const toml_config& config)
{
	auto requests_path = toml::find<std::string>(
		config, "requests_path"
	);
	auto requests = std::move(workload::import_cs_requests(requests_path));
	auto* scheduler = initialize_scheduler(requests.size(), config);

	auto throughput_thread = std::thread(
		print_throughput, SLEEP, requests.size(), scheduler
	);

	auto print_percentage = toml::find<int>(
		config, "print_percentage"
	);
	auto client_messages = to_client_messages(requests);

	auto send_timestamps = execute_requests(
		*scheduler, client_messages, print_percentage
	);

	throughput_thread.join();

	auto execution_timestamps = order_execution_timestamps(*scheduler);

	for (auto& kv: execution_timestamps) {
		auto id = kv.first;
		auto execution_timestamp = kv.second;

		auto delay = execution_timestamp - send_timestamps[id];
		std::cout << id << "," << delay.count() << "\n";
	}
	std::cout << std::endl;
}

static void
usage(std::string prog)
{
	std::cout << "Usage: " << prog << " config\n";
}

int
main(int argc, char const *argv[])
{
	if (argc < 2) {
		usage(std::string(argv[0]));
		exit(1);
	}

	const auto config = toml::parse(argv[1]);

	run(config);

	return 0;
}
