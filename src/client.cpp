#include <iostream>
#include <sstream>
#include <string>

#include "evclient/evclient.h"
#include "request/request_generation.h"


const int OUTSTANDING = 1;
const int VALUE_SIZE = 64;


static void send_requests(client* c, std::string config) {
	auto* v = (struct client_request*)c->send_buffer;
    v->size = c->value_size;
    auto size = sizeof(client_request) + v->size;
    v->type = READ;

    auto requests = workload::create_requests(config);
    for (auto request: requests) {
        std::stringstream stream;
        stream << request;
        auto str = stream.str();
        for (auto i = 0; i < str.size(); i++) {
            v->args[i] = str[i];
        }
        v->args[str.size()] = 0;
        paxos_submit(c->bev, c->send_buffer, size);
    }
}

void usage(std::string name) {
    std::cout << "Usage: " << name << " id client_config request_config\n";
}

int main(int argc, char* argv[]) {
    if (argc < 4) {
        usage(std::string(argv[0]));
        exit(1);
    }

    auto client_id = atoi(argv[1]);
    auto client_config = std::string(argv[2]);
    auto requests_config = std::string(argv[3]);

    auto* client = make_client(
        client_config.c_str(), client_id, OUTSTANDING, VALUE_SIZE,
        nullptr, nullptr
    );
	signal(SIGPIPE, SIG_IGN);
    send_requests(client, requests_config);
	event_base_dispatch(client->base);

    client_free(client);

    return 0;
}
