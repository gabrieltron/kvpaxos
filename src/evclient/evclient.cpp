#include "evclient.h"


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

static void
event_cb(struct bufferevent *bev, short ev, void *ptr)
{
	auto* c = (client *)ptr;
	if (c->callbacks->eventcb) {
		return c->callbacks->eventcb(bev, ev, ptr);
	}

	if (ev & BEV_EVENT_EOF || ev & BEV_EVENT_ERROR) {
		bufferevent_free(bev);
	}
}

static void
read_server(struct bufferevent* bev, void* args)
{
	auto* c = (client *)args;
	c->callbacks->readcb(bev, args);
    bufferevent_free(bev);
}

static void
recieve_connection(struct evconnlistener *l, evutil_socket_t fd,
	struct sockaddr* addr, int socklen, void *arg)
{
	client* c = static_cast<client*>(arg);
	bufferevent* bev = bufferevent_socket_new(c->base, -1, BEV_OPT_CLOSE_ON_FREE);

	bufferevent_setfd(bev, fd);
	bufferevent_setcb(bev, read_server, NULL, event_cb, c);
	bufferevent_enable(bev, EV_READ|EV_WRITE);
    int flag = 1;
	setsockopt(bufferevent_getfd(bev), IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
}

bool
listen_server(struct client* c, unsigned short port)
{
	struct sockaddr_in addr;
	unsigned flags = LEV_OPT_CLOSE_ON_EXEC
		| LEV_OPT_CLOSE_ON_FREE
		| LEV_OPT_REUSEABLE;

	/* listen on the given port at address 0.0.0.0 */
	memset(&addr, 0, sizeof(struct sockaddr_in));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(0);
	addr.sin_port = htons(port);

	c->listener = evconnlistener_new_bind(c->base, recieve_connection, c,
		flags, -1, (struct sockaddr*)&addr, sizeof(addr));
	if (c->listener == NULL) {
		return false;
	}

    return true;
}

struct client*
make_client(
    const char* config, int proposer_id, int outstanding,
	int value_size, bufferevent_event_cb on_connect,
	bufferevent_data_cb on_reply
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
	c->callbacks = (bufferevent_callbacks *) malloc(sizeof(bufferevent_callbacks));
	c->callbacks->readcb = on_reply;
	c->callbacks->writecb = NULL;
	c->callbacks->eventcb = NULL;
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
	free(c->callbacks);
	free(c->listener);
	bufferevent_free(c->bev);
	event_free(c->stats_ev);
	event_free(c->sig);
	event_base_free(c->base);
	if (c->learner)
		evlearner_free(c->learner);
	delete c->sent_requests_timestamp;
	free(c);
}

