#ifndef _EVPAXOS_CLIENT_H_
#define _EVPAXOS_CLIENT_H_

#ifdef __cplusplus
extern "C" {
#endif


#include <errno.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <signal.h>
#include <event2/event.h>
#include <netinet/tcp.h>

#include <evpaxos.h>


struct client_value
{
	int client_id;
	struct timeval t;
	size_t size;
	char value[0];
};

struct stats
{
	long min_latency;
	long max_latency;
	long avg_latency;
	int delivered_count;
	size_t delivered_bytes;
};

struct client
{
	int id;
	int value_size;
	int outstanding;
	char* send_buffer;
	struct stats stats;
	struct event_base* base;
	struct bufferevent* bev;
	struct event* stats_ev;
	struct timeval stats_interval;
	struct event* sig;
	struct evlearner* learner;
};

struct client* make_client(
    const char* config, int proposer_id, int outstanding, int value_size,
    bufferevent_event_cb on_connect, deliver_function on_deliver
);
void client_free(struct client* c);

#ifdef __cplusplus
}
#endif

#endif
