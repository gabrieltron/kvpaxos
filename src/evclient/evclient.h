#ifndef _EVPAXOS_CLIENT_H_
#define _EVPAXOS_CLIENT_H_


#include <errno.h>
#include <event2/event.h>
#include <event2/listener.h>
#include <evpaxos.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <signal.h>
#include <netinet/tcp.h>

#include "types/types.h"


struct client* make_client(
    const char* config, int proposer_id, int outstanding,
	int value_size, unsigned short reply_port, bufferevent_event_cb on_connect,
	bufferevent_data_cb on_reply
);
void client_free(struct client* c);


#endif
