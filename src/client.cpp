#include <arpa/inet.h>
#include <chrono>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/listener.h>
#include <iostream>
#include <netinet/tcp.h>
#include <pthread.h>
#include <semaphore.h>
#include <tbb/concurrent_unordered_map.h>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <sstream>
#include <string>

#include "constants/constants.h"
#include "evclient/evclient.h"
#include "request/request_generation.h"


using toml_config = toml::basic_value<
	toml::discard_comments, std::unordered_map, std::vector
>;

struct dispatch_requests_args {
    client* c;
    std::vector<workload::Request>* requests;
    int request_id, sleep_time, n_listener_threads;
};


static void
send_request(evutil_socket_t fd, short event, void *arg)
{
    auto* dispatch_args = (dispatch_requests_args *)arg;
    auto* requests = (std::vector<workload::Request>*) dispatch_args->requests;
    auto* c = dispatch_args->c;
    auto requests_id = dispatch_args->request_id;
    auto sleep_time = dispatch_args->sleep_time;
    auto n_listener_threads = dispatch_args->n_listener_threads;

    auto* client_args = (struct client_args *) c->args;
	auto* v = (struct client_message*)c->send_buffer;

    auto request = requests->at(requests_id);
    v->sin_port = htons(
        client_args->reply_port + (requests_id % n_listener_threads)
    );
    v->id = requests_id;
    v->type = request.type();
    v->key = request.key();
    if (request.type() == WRITE and request.args().empty()) {
        memset(v->args, '#', VALUE_SIZE);
        v->args[VALUE_SIZE-1] = '\0';
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
    paxos_submit(c->bev, c->send_buffer, size);
    requests_id++;

    if (requests_id != requests->size()) {
        dispatch_args->request_id++;
        long delay = sleep_time / n_listener_threads;
        auto time = (struct timeval){0, delay};
        auto send_event = evtimer_new(
            c->base, send_request, dispatch_args
        );
        event_add(send_event, &time);
    } else {
        event_base_loopexit(c->base, NULL);
    }
}

static void
read_reply(const struct reply_message& reply, void* args)
{
    auto* client_args = (struct client_args *) args;
    auto* sent_timestamp = client_args->sent_timestamp;
    auto* print_mutex = client_args->print_mutex;

    if (client_args->print_percentage >= rand() % 100 + 1) {
        auto now = std::chrono::system_clock::now();
        auto delay_ns = now - sent_timestamp->at(reply.id);

        std::lock_guard<std::mutex> lock(*print_mutex);
        if (client_args->verbose) {
            std::cout << "Request " << reply.id << "; ";
            std::cout << "He said " << reply.answer << "; ";
            std::cout << "Delay " << delay_ns.count() << ";\n";
        } else {
            std::cout << now.time_since_epoch().count() << ",";
            std::cout << delay_ns.count() << "\n";
        }
    }
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
    auto* sent_timestamp = new tbb::concurrent_unordered_map<int, time_point>();
    auto* print_mutex = new std::mutex();
    client_args->sent_timestamp = sent_timestamp;
    client_args->print_mutex = print_mutex;
    return client_args;
}

static void
free_client_args(struct client_args* client_args)
{
    delete client_args->print_mutex;
    delete client_args->sent_timestamp;
    delete client_args;
}

static void
schedule_send_requests_event(
    struct client* client,
    pthread_barrier_t& start_barrier,
    int n_listener_threads,
    const toml_config& config)
{
    auto requests_path = toml::find<std::string>(
        config, "requests_path"
    );
    auto requests = workload::import_cs_requests(requests_path);
    auto* requests_pointer = new std::vector<workload::Request>(requests);
    auto sleep_time = toml::find<int>(
        config, "sleep_time"
    );

    auto* dispatch_args = new struct dispatch_requests_args();
    dispatch_args->c = client;
    dispatch_args->requests = requests_pointer;
    dispatch_args->request_id = 0;
    dispatch_args->sleep_time = sleep_time;
    dispatch_args->n_listener_threads = n_listener_threads;

	auto time = (struct timeval){1, 0};
	auto send_event = evtimer_new(
        client->base, send_request, dispatch_args
    );
	event_add(send_event, &time);
}

static void
start_client(const toml_config& config, unsigned short port, bool verbose)
{
    auto* client = make_ev_client(config);
    client->args = make_client_args(config, port, verbose);

    auto n_listener_threads = toml::find<int>(
        config, "n_threads"
    );
    auto n_total_requests = toml::find<int>(
        config, "n_requests"
    );

    pthread_barrier_t start_barrier;
    pthread_barrier_init(&start_barrier, NULL, n_listener_threads+1);

    std::vector<std::thread> listener_threads;
    for (auto i = 0; i < n_listener_threads; i++) {
        listener_threads.emplace_back(
            listen_server, client, n_total_requests,
            port+i, std::ref(start_barrier)
        );
    }
    pthread_barrier_wait(&start_barrier);

    schedule_send_requests_event(
        client, start_barrier, n_listener_threads, config
    );
	event_base_loop(client->base, EVLOOP_NO_EXIT_ON_EMPTY);

    for (auto& thread: listener_threads) {
        thread.join();
    }

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
