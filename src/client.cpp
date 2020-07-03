#include <arpa/inet.h>
#include <chrono>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <iostream>
#include <netinet/tcp.h>
#include <thread>
#include <unordered_map>

#include <sstream>
#include <string>

#include "evclient/evclient.h"
#include "request/request_generation.h"


const int OUTSTANDING = 1;
const int VALUE_SIZE = 128;
static unsigned short REPLY_PORT;
static bool VERBOSE;
static int PRINT_PERCENTAGE = 100;

struct callback_args {
	int request_counter;
    int n_load_requests;
    std::string path;
};

static void
send_requests(client* c, const std::vector<workload::Request>& requests)
{
	auto* v = (struct client_message*)c->send_buffer;
    v->sin_port = htons(REPLY_PORT);

    auto counter = 0;
    for (auto& request: requests) {
        v->id = counter;
        v->type = request.type();
        v->key = request.key();
        if (request.args().empty()) {
            memset(v->args, '#', VALUE_SIZE);
            v->args[VALUE_SIZE] = '\0';
            v->size = VALUE_SIZE;
        }
        else {
            for (auto i = 0; i < request.args().size(); i++) {
                v->args[i] = request.args()[i];
            }
            v->args[request.args().size()] = 0;
            v->size = request.args().size();
        }
        auto size = sizeof(struct client_message) + v->size;
        auto timestamp = std::chrono::system_clock::now();
        auto kv = std::make_pair(v->id, timestamp);
        c->sent_requests_timestamp->insert(kv);
        paxos_submit(c->bev, c->send_buffer, size);
        counter++;
    }
}

static void
read_reply(struct bufferevent* bev, void* args)
{
    auto* c = (client *)args;
    auto* c_args = (struct callback_args *) c->args;
    reply_message reply;
    bufferevent_read(bev, &reply, sizeof(reply_message));

    if (c->sent_requests_timestamp->find(reply.id) == c->sent_requests_timestamp->end()) {
        // already recieved an answer for this request.
        return;
    }
    auto delay_ns =
        std::chrono::system_clock::now() - c->sent_requests_timestamp->at(reply.id);
    if (PRINT_PERCENTAGE >= rand() % 100 + 1) {
        if (VERBOSE) {
            std::cout << "Client " << c->id << "; ";
            std::cout << "Request " << reply.id << "; ";
            std::cout << "He said " << reply.answer << "; ";
            std::cout << "Delay " << delay_ns.count() << ";\n";
        } else {
            std::cout << std::chrono::system_clock::now().time_since_epoch().count() << ",";
            std::cout << delay_ns.count() << "\n";
        }
    }
    c->sent_requests_timestamp->erase(reply.id);

    c_args->request_counter++;
    if (c_args->request_counter == c_args->n_load_requests) {
        auto requests = std::move(workload::import_requests(
            c_args->path, "requests"
        ));
        send_requests(c, requests);
    }
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
    auto requests_path = std::string(argv[4]);
    if (argc < 7 and std::string(argv[5]) == "-v") {
        VERBOSE= true;
        PRINT_PERCENTAGE = atoi(argv[6]);
    } else {
        VERBOSE = false;
    }
    srand (time(NULL));

    auto* client = make_client(
        client_config.c_str(), client_id, OUTSTANDING, VALUE_SIZE,
        REPLY_PORT, nullptr, read_reply
    );
	signal(SIGPIPE, SIG_IGN);

    struct callback_args args;
    args.request_counter = 0;
    args.path = requests_path;
    client->args = &args;

    auto requests = std::move(workload::import_requests(requests_path, "load_requests"));
    args.n_load_requests = requests.size();
    send_requests(client, requests);
	event_base_dispatch(client->base);

    client_free(client);

    return 0;
}
