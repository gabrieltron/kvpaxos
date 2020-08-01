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
#include <queue>
#include <semaphore.h>
#include <netinet/in.h>
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
#include "storage/storage.h"
#include "types/types.h"


using toml_config = toml::basic_value<
	toml::discard_comments, std::unordered_map, std::vector
>;

static int verbose = 0;
static int SLEEP = 1;
static bool RUNNING = true;


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
print_throughput(int sleep_duration, int n_requests, int& n_executed_requests)
{
	auto already_counted = 0;
	auto counter = 0;
	while (RUNNING) {
		std::this_thread::sleep_for(std::chrono::seconds(sleep_duration));
		auto throughput = n_executed_requests - already_counted;
		std::cout << counter << ",";
		std::cout << throughput << "\n";

		already_counted += throughput;
		counter++;
		if (n_executed_requests == n_requests) {
			break;
		}
	}
}

static kvstorage::Storage
initialize_storage(const toml_config& config)
{
	auto storage = kvstorage::Storage();
	auto n_initial_keys = toml::find<int>(
		config, "n_initial_keys"
	);

	std::vector<workload::Request> populate_requests;
	for (auto i = 0; i <= n_initial_keys; i++) {
		populate_requests.emplace_back(WRITE, i, "");
	}

	for (auto& request : populate_requests) {
		auto key = request.key();
		auto type = static_cast<request_type>(request.type());
		auto args = request.args();
		execute_request(storage, key, type, args);
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
	kvstorage::Storage& storage,
	std::vector<struct client_message>& requests,
	int& n_executed_requests)
{
	srand (time(NULL));

	for (auto& request: requests) {
		auto key = request.key;
		auto type = static_cast<request_type>(request.type);
		auto args = request.args;
		auto answer = std::move(execute_request(storage, key, type, args));

        reply_message reply;
        reply.id = request.id;
        strncpy(reply.answer, answer.c_str(), answer.size());
        reply.answer[answer.size()] = '\0';
		n_executed_requests++;
	}
}

static void
run(const toml_config& config)
{
	auto requests_path = toml::find<std::string>(
		config, "requests_path"
	);
	auto requests = std::move(workload::import_cs_requests(requests_path));
	auto storage = initialize_storage(config);

	auto n_executed_requests = 0;
	auto throughput_thread = std::thread(
		print_throughput, SLEEP, requests.size(), std::ref(n_executed_requests)
	);

	auto client_messages = std::move(to_client_messages(requests));
	auto start_timestamp = std::chrono::system_clock::now();
	execute_requests(storage, client_messages, n_executed_requests);
	auto end_timestamp = std::chrono::system_clock::now();

	RUNNING = false;
	throughput_thread.join();
	std::cout << "Makespan: " << (end_timestamp - start_timestamp).count() << std::endl;
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
