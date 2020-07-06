#include <arpa/inet.h>
#include <chrono>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <iostream>
#include <netinet/tcp.h>
#include <tbb/concurrent_unordered_map.h>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <sstream>
#include <string>

#include "evclient/evclient.h"
#include "request/request_generation.h"


const int OUTSTANDING = 1;
const int VALUE_SIZE = 128;
static unsigned short REPLY_PORT;
static bool VERBOSE;
static int PRINT_PERCENTAGE = 100;

struct dispatch_requests_args {
    client* c;
    std::vector<workload::Request>* requests;
};

struct client_args {
    tbb::concurrent_unordered_map<int, time_point>* sent_timestamp;
    std::unordered_set<int>* answered_requests;
};


static void
send_requests(client* c, const std::vector<workload::Request>& requests)
{
    auto* client_args = (struct client_args *) c->args;
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
        client_args->sent_timestamp->insert(kv);
        bufferevent_lock(c->bev);
        paxos_submit(c->bev, c->send_buffer, size);
        bufferevent_unlock(c->bev);
        counter++;
    }
}

static void
dispatch_requests_thread(evutil_socket_t fd, short event, void *arg)
{
    auto* dispatch_args = (dispatch_requests_args *)arg;
    auto* requests = (std::vector<workload::Request>*) dispatch_args->requests;
    auto* client = dispatch_args->c;
    std::thread send_requests_thread(send_requests, client, std::ref(*requests));
    send_requests_thread.detach();
}

static void
read_reply(struct bufferevent* bev, void* args)
{
    auto* c = (client *)args;
    auto* client_args = (struct client_args *) c->args;
    auto* answered_requests = client_args->answered_requests;
    auto* sent_timestamp = client_args->sent_timestamp;
    reply_message reply;
    bufferevent_lock(bev);
    bufferevent_read(bev, &reply, sizeof(reply_message));
    bufferevent_unlock(bev);

    if (answered_requests->find(reply.id) != answered_requests->end()) {
        // already recieved an answer for this request.
        return;
    }
    auto delay_ns =
        std::chrono::system_clock::now() - sent_timestamp->at(reply.id);
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
    answered_requests->insert(reply.id);
}

void usage(std::string name) {
    std::cout << "Usage: " << name << " port id client_config request_config (-v|percentage)\n";
}

int main(int argc, char* argv[]) {
    if (argc < 6) {
        usage(std::string(argv[0]));
        exit(1);
    }

    REPLY_PORT = atoi(argv[1]);
    auto client_id = atoi(argv[2]);
    auto client_config = std::string(argv[3]);
    auto requests_path = std::string(argv[4]);
    if (std::string(argv[5]) == "-v") {
        VERBOSE= true;
    } else {
        VERBOSE = false;
        PRINT_PERCENTAGE = atoi(argv[5]);
    }
    srand (time(NULL));

    auto* client = make_client(
        client_config.c_str(), client_id, OUTSTANDING, VALUE_SIZE,
        REPLY_PORT, nullptr, read_reply
    );
	signal(SIGPIPE, SIG_IGN);

    struct client_args client_args;
    std::unordered_set<int> answered_requests;
    tbb::concurrent_unordered_map<int, time_point> sent_timestamp;
    client_args.answered_requests = &answered_requests;
    client_args.sent_timestamp = &sent_timestamp;
    client->args = &client_args;

    auto requests = std::move(workload::import_requests(requests_path, "requests"));
    dispatch_requests_args dispatch_args;
    dispatch_args.c = client;
    dispatch_args.requests = &requests;
	auto time = (struct timeval){1, 0};
	auto send_event = evtimer_new(
        client->base, dispatch_requests_thread, &dispatch_args
    );
	event_add(send_event, &time);

	event_base_dispatch(client->base);
    client_free(client);

    return 0;
}
