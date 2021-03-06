#include "evclient.h"

bool RUNNING = true;

long
timeval_diff(struct timeval* t1, struct timeval* t2)
{
	long us;
	us = (t2->tv_sec - t1->tv_sec) * 1e6;
	if (us < 0) return 0;
	us += (t2->tv_usec - t1->tv_usec);
	return us;
}

void
handle_sigint(int sig, short ev, void* arg)
{
	struct event_base* base = (struct event_base*)arg;
	RUNNING = false;
	printf("Caught signal %d\n", sig);
	event_base_loopexit(base, NULL);
}

static struct bufferevent*
connect_to_proposer(
    struct client* c, const char* config, int proposer_id,
    bufferevent_event_cb on_connect
)
{
	struct bufferevent* bev;
	struct evpaxos_config* conf = evpaxos_config_read(config);
	if (conf == NULL) {
		printf("Failed to read config file %s\n", config);
		return NULL;
	}
	struct sockaddr_in addr = evpaxos_proposer_address(conf, proposer_id);
	evthread_use_pthreads();
	bev = bufferevent_socket_new(c->base, -1, BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);
	bufferevent_setcb(bev, NULL, NULL, on_connect, c);
	bufferevent_enable(bev, EV_READ|EV_WRITE);
	bufferevent_socket_connect(bev, (struct sockaddr*)&addr, sizeof(addr));
	int flag = 1;
	setsockopt(bufferevent_getfd(bev), IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
	return bev;
}

void
listen_server(
	struct client* client, 
	int& n_answered_requests,
	std::mutex& requests_counter_mutex,
	int n_total_requests, 
	unsigned short port,
	pthread_barrier_t& start_barrier
) {
	auto fd = socket(AF_INET, SOCK_DGRAM, 0);
	if (fd < 0) {
		printf("Failed to create.");
		return;
	}

	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(struct sockaddr_in));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(0);
	addr.sin_port = htons(port);
	auto binded = bind(fd, (const struct sockaddr *)&addr, sizeof(addr));
	if (binded < 0) {
		printf("Failed to bind to socket.");
		return;
	}

	struct timeval timeout;
	timeout.tv_sec = 1;
	timeout.tv_usec = 0;
	setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

	std::unordered_set<int> answered_requests;
	pthread_barrier_wait(&start_barrier);
	while (RUNNING) {
		struct reply_message reply;
		auto n_bytes = recv(fd, &reply, sizeof(reply_message), 0);
		if (n_bytes == -1) {
			if (n_answered_requests == n_total_requests) {
				break;
			} else if(n_answered_requests >= 99*n_total_requests/100) {
				break;
			}
			continue;
		}

		if (answered_requests.find(reply.id) != answered_requests.end()) {
			continue;
		}

		{
			std::scoped_lock lock(requests_counter_mutex);
			n_answered_requests++;
		}

		client->reply_cb(reply, client->args);
		answered_requests.insert(reply.id);
	}
}

struct client*
make_client(
    const char* config, int proposer_id, int outstanding,
	int value_size, bufferevent_event_cb on_connect,
	reply_callback on_reply
)
{
	struct client* c;
	c = (struct client*)malloc(sizeof(struct client));
	c->base = event_base_new();

	memset(&c->stats, 0, sizeof(struct stats));
	c->bev = connect_to_proposer(c, config, proposer_id, on_connect);
	if (c->bev == NULL) {
		return NULL;
	}

	c->id = rand();
	c->value_size = value_size;
	c->outstanding = outstanding;
	c->send_buffer = (char *)malloc(sizeof(client_message) + value_size);
	c->reply_cb = on_reply;
	c->sent_requests_timestamp = new std::unordered_map<
		int, std::chrono::_V2::system_clock::time_point
	>();

	c->sig = evsignal_new(c->base, SIGINT, handle_sigint, c->base);
	evsignal_add(c->sig, NULL);

	return c;
}

void
client_free(struct client* c)
{
	free(c->send_buffer);
	//free(c->listener);
	bufferevent_free(c->bev);
	//event_free(c->stats_ev);
	event_free(c->sig);
	event_base_free(c->base);
	// (c->learner)
//		evlearner_free(c->learner);
	delete c->sent_requests_timestamp;
	free(c);
}

