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
#include <thread>
#include <unordered_map>
#include <utility>
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
static bool RUNNING = true;


void
metrics_loop(int sleep_duration, int n_requests, kvpaxos::Scheduler<int>* scheduler)
{
	auto already_counted_throughput = 0;
	auto counter = 0;
	while (RUNNING) {
		std::this_thread::sleep_for(std::chrono::seconds(sleep_duration));
		auto executed_requests = scheduler->n_executed_requests();
		auto throughput = executed_requests - already_counted_throughput;
		std::cout << counter << ",";
		std::cout << throughput << "\n";
		already_counted_throughput += throughput;
		counter++;

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
	auto n_partitions = toml::find<int>(
		config, "n_partitions"
	);
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
		n_requests, repartition_interval, n_partitions,
		repartition_method
	);

	auto n_initial_keys = toml::find<int>(
		config, "n_initial_keys"
	);
	std::vector<workload::Request> populate_requests;
	for (auto i = 0; i <= n_initial_keys; i++) {
		populate_requests.emplace_back(WRITE, i, "");
	}

	scheduler->process_populate_requests(populate_requests);

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
		for (auto i = 0; i < request.args().size(); i++) {
			client_message.args[i] = request.args()[i];
		}
		client_message.args[request.args().size()] = 0;
		client_message.size = request.args().size();

		client_messages.emplace_back(client_message);
	}

	return client_messages;
}

void
execute_requests(
	kvpaxos::Scheduler<int>& scheduler,
	std::vector<struct client_message>& requests,
	int print_percentage)
{
	for (auto& request: requests) {
		scheduler.schedule_and_answer(request);
	}
}

std::unordered_map<int, time_point>
join_maps(std::vector<std::unordered_map<int, time_point>> maps) {
	std::unordered_map<int, time_point> joined_map;
	for (auto& map: maps) {
		joined_map.insert(map.begin(), map.end());
	}
	return joined_map;
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
		metrics_loop, SLEEP, requests.size(), scheduler
	);

	auto print_percentage = toml::find<int>(
		config, "print_percentage"
	);
	auto client_messages = to_client_messages(requests);

	auto start_execution_timestamp = std::chrono::system_clock::now();
	execute_requests(*scheduler, client_messages, print_percentage);

	throughput_thread.join();
	auto end_execution_timestamp = std::chrono::system_clock::now();

	auto makespan = end_execution_timestamp - start_execution_timestamp;
	std::cout << "Makespan: " << makespan.count() << "\n";

	auto& repartition_times = scheduler->repartition_timestamps();
	std::cout << "Repartition at: ";
	for (auto& repartition_time : repartition_times) {
		std::cout << (repartition_time - start_execution_timestamp).count() << " ";
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
