#ifndef _EVPAXOS_CLIENT_H_
#define _EVPAXOS_CLIENT_H_

#ifdef __cplusplus
extern "C" {
#endif


#include <errno.h>
#include <event2/event.h>
#include <evpaxos.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <signal.h>
#include <netinet/tcp.h>

#include "types/types.hpp"


struct client* make_client(
    const char* config, int proposer_id, int outstanding, int value_size,
    bufferevent_event_cb on_connect, deliver_function on_deliver
);
void client_free(struct client* c);

#ifdef __cplusplus
}
#endif

#endif
