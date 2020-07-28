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
#include <future>
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
const int VALUE_SIZE = 128;


std::vector<int>
metrics_loop(int sleep_duration, int n_requests, kvpaxos::Scheduler<int>* scheduler)
{
	auto already_counted_throughput = 0;
	auto already_counted_latency = 0;
	auto counter = 0;
	std::vector<int> latency_steps;
	while (RUNNING) {
		std::this_thread::sleep_for(std::chrono::seconds(sleep_duration));
		auto executed_requests = scheduler->n_executed_requests();
		auto throughput = executed_requests - already_counted_throughput;
		std::cout << counter << ",";
		std::cout << throughput << "\n";
		already_counted_throughput += throughput;
		counter++;

		auto latencies = std::move(scheduler->execution_timestamps());
		auto n_latencies = 0;
		for (auto kv: latencies) {
			n_latencies += kv.size();
		}
		latency_steps.emplace_back(n_latencies-already_counted_latency);
		already_counted_latency = n_latencies;
		if (executed_requests == n_requests) {
			break;
		}
	}

	return latency_steps;
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

static std::pair<std::unordered_map<int, time_point>, std::vector<int>>
execute_requests(
	kvpaxos::Scheduler<int>& scheduler,
	std::vector<struct client_message>& requests,
	int print_percentage)
{
	srand (time(NULL));
	std::unordered_map<int, time_point> end_timestamp;
	std::vector<int> send_order;

	for (auto& request: requests) {
		if (print_percentage >= rand() % 100 + 1) {
	    	auto timestamp = std::chrono::system_clock::now();
    		auto kv = std::make_pair(request.id, timestamp);
    		end_timestamp.insert(kv);
			request.record_timestamp = true;

			send_order.emplace_back(request.id);
		}
		scheduler.schedule_and_answer(request);
	}
	return std::make_pair(end_timestamp, send_order);
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

	auto p_latency_steps = std::async(
		metrics_loop, SLEEP, requests.size(), scheduler
	);

	auto print_percentage = toml::find<int>(
		config, "print_percentage"
	);
	auto client_messages = to_client_messages(requests);

	auto start_execution_timestamp = std::chrono::system_clock::now();
	auto send_metrics = execute_requests(
		*scheduler, client_messages, print_percentage
	);
	auto end_execution_timestamp = std::chrono::system_clock::now();
	auto& send_timestamps = send_metrics.first;
	auto& latencies_order = send_metrics.second;

	auto latency_steps = p_latency_steps.get();

	auto execution_timestamps = join_maps(scheduler->execution_timestamps());

	auto printed_latencies = 0;
	for (auto seconds = 0; seconds < latency_steps.size(); seconds++) {
		auto n_latencies = latency_steps[seconds];
		if (n_latencies == 0) {
			continue;
		}

		for (auto i = 0; i < n_latencies; i++) {
			auto requests_id = latencies_order[printed_latencies+i];
			auto send = send_timestamps[requests_id];
			auto executed = execution_timestamps[requests_id];
			auto delay = executed - send;
			std::cout << seconds << ",";
			std::cout << delay.count() << "\n";
		}
		printed_latencies += n_latencies;
	}

	auto makespan = end_execution_timestamp - start_execution_timestamp;
	std::cout << "Makespan: " << makespan.count() << "\n";
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
