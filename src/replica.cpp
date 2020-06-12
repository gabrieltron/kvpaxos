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
#include <sstream>
#include <signal.h>
#include <netinet/tcp.h>
#include <vector>

#include "types/types.h"
#include "storage/storage.h"


static int verbose = 0;

struct callback_args {
	event_base* base;
	kvstorage::Storage* storage;
};

static void
handle_sigint(int sig, short ev, void* arg)
{
	struct event_base* base = static_cast<event_base*>(arg);
	printf("Caught signal %d\n", sig);
	event_base_loopexit(base, NULL);
}

static void
on_event(struct bufferevent* bev, short ev, void *arg)
{
	if (ev & BEV_EVENT_EOF || ev & BEV_EVENT_ERROR) {
		bufferevent_free(bev);
	}
}

struct bufferevent*
connect_to_client(unsigned long ip, unsigned short port, struct event_base* base)
{
	struct bufferevent* bev;

	struct sockaddr_in addr;
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = ip;
	addr.sin_port = port;

	bev = bufferevent_socket_new(base, -1, BEV_OPT_CLOSE_ON_FREE);
	bufferevent_setcb(bev, NULL, NULL, on_event, NULL);
	bufferevent_enable(bev, EV_READ|EV_WRITE);
	bufferevent_socket_connect(bev, (struct sockaddr*)&addr, sizeof(addr));
	int flag = 1;
	setsockopt(bufferevent_getfd(bev), IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
	return bev;
}

static void
answer_client(const char* answer, size_t length, char* og_message,
	struct event_base* base)
{
	auto* message = (struct client_message*)og_message;
	auto* bev = connect_to_client(message->s_addr, message->sin_port, base);
	bufferevent_write(bev, answer, length);
}

std::vector<std::string>
split_string(std::string& string, char delimiter)
{
	std::replace(string.begin(), string.end(), delimiter, ' ');

	std::vector<std::string> array;
	std::stringstream ss(string);
	std::string temp;
	while (ss >> temp)
	    array.push_back(temp);
}

static void
deliver(unsigned iid, char* value, size_t size, void* arg)
{
	auto* request = (struct client_message*)value;
	auto* args = (callback_args*) arg;
	auto* storage = args->storage;

	auto type = static_cast<request_type>(request->type);
	auto key = request->key;
	auto request_args = std::string(request->args);

	std::string answer;
	switch (static_cast<request_type>(request->type))
	{
	case READ:
	{
		answer = storage->read(key);
		break;
	}

	case WRITE:
	{
		storage->write(key, request_args);
		answer = request_args;
		break;
	}

	case SCAN:
	{
		auto length = std::stoi(request_args);
		auto values = storage->scan(key, length);

		std::ostringstream oss;
		std::copy(values.begin(), values.end(), std::ostream_iterator<std::string>(oss, ","));
		answer = std::string(oss.str());
		break;
	}

	default:
		break;
	}

	answer_client(answer.c_str(), 800, value, args->base);
}

static void
start_replica(int id, const char* config)
{
	struct event* sig;
	struct event_base* base;
	struct evpaxos_replica* replica;
	deliver_function cb = deliver;

	base = event_base_new();
	replica = evpaxos_replica_init(id, config, cb, NULL, base);
	if (replica == NULL) {
		printf("Could not start the replica!\n");
		exit(1);
	}

	auto storage = kvstorage::Storage();
	struct callback_args args;
	args.base = base;
	args.storage = &storage;
	replica->arg = &args;

	sig = evsignal_new(base, SIGINT, handle_sigint, base);
	evsignal_add(sig, NULL);

	signal(SIGPIPE, SIG_IGN);
	event_base_dispatch(base);

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
