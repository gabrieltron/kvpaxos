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

using toml_config = toml::basic_value<
	toml::discard_comments, std::unordered_map, std::vector
>;

struct dispatch_requests_args {
    client* c;
    std::vector<workload::Request>* requests;
};

struct client_args {
    bool verbose;
    int print_percentage;
    unsigned short reply_port;
    tbb::concurrent_unordered_map<int, time_point>* sent_timestamp;
    std::unordered_set<int>* answered_requests;
};


static void
send_requests(client* c, const std::vector<workload::Request>& requests)
{
    auto* client_args = (struct client_args *) c->args;
	auto* v = (struct client_message*)c->send_buffer;
    v->sin_port = htons(client_args->reply_port);

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
    delete &requests;
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
read_reply(const struct reply_message& reply, void* args)
{
    auto* client_args = (struct client_args *) args;
    auto* answered_requests = client_args->answered_requests;
    auto* sent_timestamp = client_args->sent_timestamp;

    if (answered_requests->find(reply.id) != answered_requests->end()) {
        // already recieved an answer for this request.
        return;
    }
    auto delay_ns =
        std::chrono::system_clock::now() - sent_timestamp->at(reply.id);
    if (client_args->print_percentage >= rand() % 100 + 1) {
        if (client_args->verbose) {
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

static struct client*
make_ev_client(const toml_config& config)
{
    auto paxos_config = toml::find<std::string>(
        config, "paxos_config"
    );
    auto proposer_id = toml::find<int>(
        config, "proposer_id"
    );
    auto* client = make_client(
        paxos_config.c_str(), proposer_id, OUTSTANDING, VALUE_SIZE,
        nullptr, read_reply
    );
	signal(SIGPIPE, SIG_IGN);
    return client;
}

static struct client_args*
make_client_args(const toml_config& config, unsigned short port, bool verbose)
{
    auto* client_args = new struct client_args();
    client_args->verbose = verbose;
    client_args->print_percentage = toml::find<int>(
        config, "print_percentage"
    );
    client_args->reply_port = port;
    auto* answered_requests = new std::unordered_set<int>();
    auto* sent_timestamp = new tbb::concurrent_unordered_map<int, time_point>();
    client_args->answered_requests = answered_requests;
    client_args->sent_timestamp = sent_timestamp;
    return client_args;
}

static void
free_client_args(struct client_args* client_args)
{
    delete client_args->answered_requests;
    delete client_args->sent_timestamp;
    delete client_args;
}

static void
schedule_send_requests_event(struct client* client, const toml_config& config)
{
    auto requests_path = toml::find<std::string>(
        config, "requests_path"
    );
    auto requests = std::move(workload::import_requests(requests_path, "requests"));
    auto* requests_pointer = new std::vector<workload::Request>(requests);
    auto* dispatch_args = new struct dispatch_requests_args();
    dispatch_args->c = client;
    dispatch_args->requests = requests_pointer;
	auto time = (struct timeval){1, 0};
	auto send_event = evtimer_new(
        client->base, dispatch_requests_thread, dispatch_args
    );
	event_add(send_event, &time);
}

static void
start_client(const toml_config& config, unsigned short port, bool verbose)
{
    auto* client = make_ev_client(config);
    client->args = make_client_args(config, port, verbose);
    schedule_send_requests_event(client, config);
    std::thread listen_thread(listen_server, client, port);

	event_base_dispatch(client->base);

    free_client_args((struct client_args *)client->args);
    client_free(client);
}

void usage(std::string name) {
    std::cout << "Usage: " << name << " port config [-v]\n";
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        usage(std::string(argv[0]));
        exit(1);
    }

    auto reply_port = atoi(argv[1]);
    const auto config = toml::parse(argv[2]);
    bool verbose;
    if (argc >= 4 and std::string(argv[3]) == "-v") {
        verbose = true;
    } else {
        verbose = false;
    }
    srand (time(NULL));

    start_client(config, reply_port, verbose);

    return 0;
}
