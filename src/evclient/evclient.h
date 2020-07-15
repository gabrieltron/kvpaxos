#ifndef _EVPAXOS_CLIENT_H_
#define _EVPAXOS_CLIENT_H_


#include <errno.h>
#include <event2/event.h>
#include <event2/listener.h>
#include <event2/thread.h>
#include <evpaxos.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <semaphore.h>
#include <string.h>
#include <signal.h>
#include <netinet/tcp.h>
#include <unordered_map>
#include <unordered_set>

#include "types/types.h"


struct client* make_client(
    const char* config, int proposer_id, int outstanding,
	int value_size, bufferevent_event_cb on_connect,
	reply_callback on_reply
);
void listen_server(
	struct client* c, unsigned short port, sem_t& semaphore,
	pthread_barrier_t& start_barrier);
void client_free(struct client* c);


#endif
