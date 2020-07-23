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
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>
#include <thread>
#include <mutex>
#include <netinet/tcp.h>
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

	auto initial_requests = toml::find<std::vector<std::string>>(
		config, "requests_path"
	);
	if (not initial_requests.empty()) {
		auto populate_requests = std::move(
			workload::import_requests(initial_requests[0], "load_requests")
		);
		scheduler->process_populate_requests(populate_requests);
	}

	return scheduler;
}

static void
requests_loop(
	std::string& requests_path,
	std::queue<struct client_message>& requests_queue,
	tbb::concurrent_unordered_map<int, time_point>& timestamps,
	pthread_barrier_t& start_barrier,
	sem_t& requests_semaphore,
	std::mutex& queue_mutex,
	short answer_port,
	const toml_config& config)
{
	auto sleep_time = toml::find<int>(
		config, "sleep_time"
	);

	auto counter = 0;
	auto requests = std::move(workload::import_requests(requests_path, "requests"));
	pthread_barrier_wait(&start_barrier);
	for (auto& request: requests) {
		struct client_message client_message;
		client_message.s_addr = htonl(0);
		client_message.sin_port = htons(
			answer_port
		);
		client_message.id = counter;
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

		auto timestamp = std::chrono::system_clock::now();
		auto kv = std::make_pair(client_message.id, timestamp);
		timestamps.insert(kv);

		queue_mutex.lock();
			requests_queue.push(client_message);
		queue_mutex.unlock();
		sem_post(&requests_semaphore);

		auto delay = sleep_time;
		std::this_thread::sleep_for(std::chrono::nanoseconds(delay));

		counter++;
	}
}

static void
execution_loop(
	kvpaxos::Scheduler<int>* scheduler,
	std::queue<struct client_message>& requests_queue,
	sem_t& requests_semaphore,
	std::mutex& queue_mutex)
{
	scheduler->run();
	while(true) {
		sem_wait(&requests_semaphore);

		queue_mutex.lock();
			auto request = requests_queue.front();
			requests_queue.pop();
		queue_mutex.unlock();

		scheduler->schedule_and_answer(request);
	}
}

static std::vector<std::thread*>
start_listener_threads(
	tbb::concurrent_vector<time_point>& latencies,
	tbb::concurrent_unordered_map<int, time_point>& timestamps,
	unsigned short port,
	const toml_config& config)
{
	std::vector<std::thread*> listener_threads;
	auto n_listener_threads = toml::find<int>(
        config, "n_threads"
    );

    pthread_barrier_t start_barrier;
    pthread_barrier_init(&start_barrier, NULL, n_listener_threads+1);

    for (auto i = 0; i < n_listener_threads; i++) {
		auto* thread = new std::thread(
			listen_server, std::ref(latencies), std::ref(timestamps),
			 port+i, std::ref(start_barrier)
		);
        listener_threads.emplace_back(thread);
    }
    pthread_barrier_wait(&start_barrier);

	return listener_threads;
}

static void
run(unsigned short port, const toml_config& config)
{
	auto n_threads = toml::find<int>(
		config, "n_threads"
	);
	auto* scheduler = initialize_scheduler(config);
	std::queue<struct client_message> requests_queue;
	tbb::concurrent_unordered_map<int, time_point> timestamps;
	tbb::concurrent_vector<time_point> latencies;
	sem_t requests_semaphore;
	sem_init(&requests_semaphore, 0, 0);
	std::mutex queue_mutex;

	auto listener_threads = start_listener_threads(
		latencies, timestamps, port, config
	);

	auto execution_thread = std::thread(
		execution_loop, scheduler, std::ref(requests_queue),
		std::ref(requests_semaphore), std::ref(queue_mutex)
	);
	auto throughput_thread = std::thread(
		print_throughput, SLEEP, scheduler
	);

	std::vector<std::thread*> requests_threads;
    auto requests_path = toml::find<std::vector<std::string>>(
        config, "requests_path"
    );
	pthread_barrier_t start_barrier;
	pthread_barrier_init(&start_barrier, NULL, n_threads);
	for (auto i = 0; i < n_threads; i++) {
		auto* thread = new std::thread(
			requests_loop, std::ref(requests_path[i % requests_path.size()]),
			std::ref(requests_queue), std::ref(timestamps),
			std::ref(start_barrier), std::ref(requests_semaphore), std::ref(queue_mutex),
			port+i, config
		);
		requests_threads.emplace_back(thread);
	}

	for (auto* thread: requests_threads) {
		thread->join();
		delete thread;
	}

	RUNNING = false;
	throughput_thread.join();

	for (auto i = 0; i < latencies.size(); i++) {
		std::cout << i << "," << latencies.at(i).time_since_epoch().count() << "\n";
	}
	std::cout << std::endl;

	std::this_thread::sleep_for(std::chrono::seconds(30));
	execution_thread.~thread();

	for (auto* thread: listener_threads) {
		thread->join();
		delete thread;
	}

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

	auto port = atoi(argv[1]);
	const auto config = toml::parse(argv[2]);

	run(port, config);

	return 0;
}
