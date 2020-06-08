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

#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <signal.h>

#include "types/types.hpp"


static int verbose = 0;


static void
handle_sigint(int sig, short ev, void* arg)
{
	struct event_base* base = static_cast<event_base*>(arg);
	printf("Caught signal %d\n", sig);
	event_base_loopexit(base, NULL);
}

static peer*
find_client(struct peers** peers) {

}

static void
deliver(unsigned iid, char* value, size_t size, void* arg)
{
	auto* val = (struct client_request*)value;
	if (verbose) {
		std::cout << "[" << val->args << "] ";
		std::cout << (size_t)val->size << " bytes\n";
		std::cout << "Type " << val->type << "\n";
	}

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

    replica->arg = replica->peers;

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
