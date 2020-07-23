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
#include <queue>
#include <semaphore.h>
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
#include "storage/storage.h"
#include "types/types.h"
#include "graph/graph.hpp"


using toml_config = toml::basic_value<
	toml::discard_comments, std::unordered_map, std::vector
>;

static int verbose = 0;
static int SLEEP = 1;
static int N_PARTITIONS = 4;
static bool RUNNING = true;
const int VALUE_SIZE = 128;


static struct sockaddr_in
get_client_addr(unsigned long ip, unsigned short port)
{
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ip;
    addr.sin_port = port;
    return addr;
}

static void
answer_client(const char* answer, size_t length,
    client_message& message, int fd)
{
    auto client_addr = get_client_addr(message.s_addr, message.sin_port);
    auto bytes_written = sendto(
        fd, answer, length, 0,
        (const struct sockaddr *) &client_addr, sizeof(client_addr)
    );
    if (bytes_written < 0) {
        printf("Failed to send answer\n");
    }
}

static std::string
execute_request(
	kvstorage::Storage& storage,
	int key,
	request_type type,
	const std::string& args
)
{
    std::string answer;
    switch (type)
    {
    case READ:
    {
        answer = std::move(storage.read(key));
        break;
    }

    case WRITE:
    {
        storage.write(key, args);
        answer = args;
        break;
    }

    case SCAN:
    {
        auto length = std::stoi(args);
        auto values = std::move(storage.scan(key, length));

        std::ostringstream oss;
        std::copy(values.begin(), values.end(), std::ostream_iterator<std::string>(oss, ","));
        answer = std::string(oss.str());

        std::vector<int> keys(length);
        std::iota(keys.begin(), keys.end(), 1);
        break;
    }

    case ERROR:
        answer = "ERROR";
        break;
    default:
        break;
    }

	return answer;
}

void
print_throughput(int sleep_duration, int& n_executed_requests)
{
	auto already_counted = 0;
	while (RUNNING) {
		std::this_thread::sleep_for(std::chrono::seconds(sleep_duration));
		auto throughput = n_executed_requests - already_counted;
		std::cout << std::chrono::system_clock::now().time_since_epoch().count() << ",";
		std::cout << throughput << "\n";

		already_counted += throughput;
	}
}

static kvstorage::Storage
initialize_storage(const toml_config& config)
{
	auto storage = kvstorage::Storage();
	auto initial_requests = toml::find<std::vector<std::string>>(
		config, "requests_path"
	);
	if (not initial_requests.empty()) {
		auto populate_requests = std::move(
			workload::import_requests(initial_requests[0], "load_requests")
		);

		for (auto& request : populate_requests) {
			auto key = request.key();
			auto type = static_cast<request_type>(request.type());
			auto args = request.args();
			execute_request(storage, key, type, args);
		}
	}

	return storage;
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
	kvstorage::Storage& storage,
	std::queue<struct client_message>& requests_queue,
	sem_t& requests_semaphore,
	std::mutex& queue_mutex,
	int& n_executed_requests)
{
	auto socket_fd_ = socket(AF_INET, SOCK_DGRAM, 0);
	while(true) {
		sem_wait(&requests_semaphore);

		queue_mutex.lock();
			auto request = requests_queue.front();
			requests_queue.pop();
		queue_mutex.unlock();

		auto key = request.key;
		auto type = static_cast<request_type>(request.type);
		auto request_args = std::string(request.args);
		auto answer = execute_request(storage, key, type, request_args);

	    reply_message reply;
	    reply.id = request.id;
	    strncpy(reply.answer, answer.c_str(), answer.size());
	    reply.answer[answer.size()] = '\0';
		answer_client(
			(char *)&reply, sizeof(reply_message), request,
			socket_fd_
		);
		n_executed_requests++;
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
	auto storage = initialize_storage(config);
	std::queue<struct client_message> requests_queue;
	tbb::concurrent_unordered_map<int, time_point> timestamps;
	tbb::concurrent_vector<time_point> latencies;
	sem_t requests_semaphore;
	sem_init(&requests_semaphore, 0, 0);
	std::mutex queue_mutex;

	auto listener_threads = start_listener_threads(
		latencies, timestamps, port, config
	);

	int n_executed_requests = 0;
	auto execution_thread = std::thread(
		execution_loop, std::ref(storage), std::ref(requests_queue),
		std::ref(requests_semaphore), std::ref(queue_mutex),
		std::ref(n_executed_requests)
	);
	auto throughput_thread = std::thread(
		print_throughput, SLEEP, std::ref(n_executed_requests)
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
