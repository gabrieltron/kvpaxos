#include <arpa/inet.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <iostream>
#include <netinet/tcp.h>

#include <sstream>
#include <string>

#include "evclient/evclient.h"
#include "request/request_generation.h"


const int OUTSTANDING = 1;
const int VALUE_SIZE = 64;
static unsigned short REPLY_PORT;


static void send_requests(client* c, std::string config) {
	auto* v = (struct client_message*)c->send_buffer;
    v->size = c->value_size;
    auto size = sizeof(struct client_message) + v->size;
    v->sin_port = htons(REPLY_PORT);
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

static void
read_reply(struct bufferevent* bev, void* args)
{
    void* reply = (void *) malloc(17);
    bufferevent_read(bev, reply, 17);
    printf("Server said %s\n", (char*) reply);
}

void usage(std::string name) {
    std::cout << "Usage: " << name << " port id client_config request_config\n";
}

int main(int argc, char* argv[]) {
    if (argc < 5) {
        usage(std::string(argv[0]));
        exit(1);
    }

    REPLY_PORT = atoi(argv[1]);
    auto client_id = atoi(argv[2]);
    auto client_config = std::string(argv[3]);
    auto requests_config = std::string(argv[4]);

    auto* client = make_client(
        client_config.c_str(), client_id, OUTSTANDING, VALUE_SIZE,
        REPLY_PORT, nullptr, read_reply
    );
	signal(SIGPIPE, SIG_IGN);
    send_requests(client, requests_config);
	event_base_dispatch(client->base);

    client_free(client);

    return 0;
}
