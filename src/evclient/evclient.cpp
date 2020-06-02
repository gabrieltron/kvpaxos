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

void
update_stats(struct stats* stats, struct client_value* delivered, size_t size)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	long lat = timeval_diff(&delivered->t, &tv);
	stats->delivered_count++;
	stats->delivered_bytes += size;
	stats->avg_latency = stats->avg_latency +
		((lat - stats->avg_latency) / stats->delivered_count);
	if (stats->min_latency == 0 || lat < stats->min_latency)
		stats->min_latency = lat;
	if (lat > stats->max_latency)
		stats->max_latency = lat;
}

struct bufferevent*
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
	bev = bufferevent_socket_new(c->base, -1, BEV_OPT_CLOSE_ON_FREE);
	bufferevent_setcb(bev, NULL, NULL, on_connect, c);
	bufferevent_enable(bev, EV_READ|EV_WRITE);
	bufferevent_socket_connect(bev, (struct sockaddr*)&addr, sizeof(addr));
	int flag = 1;
	setsockopt(bufferevent_getfd(bev), IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
	return bev;
}

struct client*
make_client(
    const char* config, int proposer_id, int outstanding, int value_size,
    deliver_function on_deliver, bufferevent_event_cb on_connect
)
{
	struct client* c;
	c = (struct client*)malloc(sizeof(struct client));
	c->base = event_base_new();

	memset(&c->stats, 0, sizeof(struct stats));
	c->bev = connect_to_proposer(c, config, proposer_id, on_connect);
	if (c->bev == NULL)
		exit(1);

	c->id = rand();
	c->value_size = value_size;
	c->outstanding = outstanding;
	c->send_buffer = (char *)malloc(sizeof(struct client_value) + value_size);

	paxos_config.learner_catch_up = 0;
    if (on_deliver != NULL)
	    c->learner = evlearner_init(config, on_deliver, c, c->base);

	c->sig = evsignal_new(c->base, SIGINT, handle_sigint, c->base);
	evsignal_add(c->sig, NULL);

	return c;
}

void
client_free(struct client* c)
{
	free(c->send_buffer);
	bufferevent_free(c->bev);
	event_free(c->stats_ev);
	event_free(c->sig);
	event_base_free(c->base);
	if (c->learner)
		evlearner_free(c->learner);
	free(c);
}

struct client*
start_client(
    const char* config, int proposer_id, int outstanding, int value_size,
    deliver_function on_deliver, bufferevent_event_cb on_connect
)
{
	struct client* client;
	client = make_client(
        config, proposer_id, outstanding, value_size, on_deliver, on_connect
    );
	signal(SIGPIPE, SIG_IGN);
	event_base_dispatch(client->base);

    return client;
}
