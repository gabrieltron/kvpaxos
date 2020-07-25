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
#include <mutex>
#include <netinet/tcp.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>
#include <thread>
#include <unordered_map>
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
	auto initial_requests = toml::find<std::string>(
		config, "requests_path"
	);
	if (not initial_requests.empty()) {
		auto populate_requests = std::move(
			workload::import_requests(initial_requests, "load_requests")
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

static std::vector<time_point>
execute_requests(
	kvstorage::Storage& storage,
	std::vector<struct client_message>& requests,
	int print_percentage,
	int& n_executed_requests)
{
	srand (time(NULL));
	std::vector<time_point> delays;

	for (auto& request: requests) {
		time_point send_timestamp;
		if (print_percentage >= rand() % 100 + 1) {
	    	auto send_timestamp = std::chrono::system_clock::now();
			request.record_timestamp = true;
		}

		auto key = request.key;
		auto type = static_cast<request_type>(request.type);
		auto args = request.args;
		execute_request(storage, key, type, args);
		n_executed_requests++;

		if (request.record_timestamp) {
			auto delay = std::chrono::system_clock::now() - send_timestamp;
			delays.emplace_back(delay);
		}
	}
	return delays;
}

static void
run(unsigned short port, const toml_config& config)
{
	auto requests_path = toml::find<std::string>(
		config, "requests_path"
	);
	auto requests = std::move(workload::import_requests(requests_path, "requests"));
	auto storage = initialize_storage(config);

	auto n_executed_requests = 0;
	auto throughput_thread = std::thread(
		print_throughput, SLEEP, std::ref(n_executed_requests)
	);

	auto print_percentage = toml::find<int>(
		config, "print_percentage"
	);
	auto client_messages = to_client_messages(requests);

	auto delays = execute_requests(
		storage, client_messages, print_percentage, n_executed_requests
	);

	RUNNING = false;
	std::this_thread::sleep_for(std::chrono::seconds(1));

	for (auto i = 0; i < delays.size(); i++) {
		auto& delay = delays[i];
		std::cout << i << "," << delay.time_since_epoch().count() << "\n";
	}
	std::cout << std::endl;
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
