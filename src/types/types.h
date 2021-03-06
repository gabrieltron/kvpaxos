/*
    Most of LibPaxos structs are forward declarated in header files and
    defined in private cpp, which means we can't access its members.
    Here the same types are declared again so we can access its members.
	There are also new structs designated to the KV application and message
	passing.
*/

#ifndef _KVPAXOS_TYPES_H_
#define _KVPAXOS_TYPES_H_


#include <chrono>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <tbb/concurrent_unordered_map.h>

#include <evpaxos.h>
#include <evpaxos/paxos.h>

#include "constants/constants.h"


typedef std::chrono::_V2::system_clock::time_point time_point;

struct client_args {
    bool verbose;
    int print_percentage;
    unsigned short reply_port;
    tbb::concurrent_unordered_map<int, time_point>* sent_timestamp;
	std::mutex* print_mutex;
};

struct reply_message {
	int id;
	char answer[VALUE_SIZE*MAX_SCAN_LENGTH+MAX_SCAN_LENGTH];
};

enum request_type
{
	READ,
	WRITE,
	SCAN,
	SYNC,
	ERROR
};

struct stats
{
	long min_latency;
	long max_latency;
	long avg_latency;
	int delivered_count;
	size_t delivered_bytes;
};

typedef void (*reply_callback)(const struct reply_message& c, void *args);
struct client
{
	int id;
	int value_size;
	int outstanding;
	char* send_buffer;
	std::unordered_map<int, time_point>*
		sent_requests_timestamp;
	struct stats stats;
	struct event_base* base;
	struct bufferevent* bev;
	struct evconnlistener* listener;
	reply_callback reply_cb;
	struct event* stats_ev;
	struct timeval stats_interval;
	struct event* sig;
	struct evlearner* learner;
	void* args;
};

struct peer
{
	int id;
	int status;
	struct bufferevent* bev;
	struct event* reconnect_ev;
	struct sockaddr_in addr;
	struct peers* peers;
};

struct evpaxos_replica
{
	struct peers* peers;
	struct evlearner* learner;
	struct evproposer* proposer;
	struct evacceptor* acceptor;
	deliver_function deliver;
	void* arg;
};

enum paxos_message_type
{
	PAXOS_PREPARE,
	PAXOS_PROMISE,
	PAXOS_ACCEPT,
	PAXOS_ACCEPTED,
	PAXOS_PREEMPTED,
	PAXOS_REPEAT,
	PAXOS_TRIM,
	PAXOS_ACCEPTOR_STATE,
	PAXOS_CLIENT_VALUE
};
typedef enum paxos_message_type paxos_message_type;

struct paxos_value
{
	int paxos_value_len;
	char *paxos_value_val;
};
typedef struct paxos_value paxos_value;

struct paxos_prepare
{
	uint32_t iid;
	uint32_t ballot;
};
typedef struct paxos_prepare paxos_prepare;

struct paxos_promise
{
	uint32_t aid;
	uint32_t iid;
	uint32_t ballot;
	uint32_t value_ballot;
	paxos_value value;
};
typedef struct paxos_promise paxos_promise;

struct paxos_accept
{
	uint32_t iid;
	uint32_t ballot;
	paxos_value value;
};
typedef struct paxos_accept paxos_accept;

struct paxos_accepted
{
	uint32_t aid;
	uint32_t iid;
	uint32_t ballot;
	uint32_t value_ballot;
	paxos_value value;
};
typedef struct paxos_accepted paxos_accepted;

struct paxos_preempted
{
	uint32_t aid;
	uint32_t iid;
	uint32_t ballot;
};
typedef struct paxos_preempted paxos_preempted;

struct paxos_repeat
{
	uint32_t from;
	uint32_t to;
};
typedef struct paxos_repeat paxos_repeat;

struct paxos_trim
{
	uint32_t iid;
};
typedef struct paxos_trim paxos_trim;

struct paxos_acceptor_state
{
	uint32_t aid;
	uint32_t trim_iid;
};
typedef struct paxos_acceptor_state paxos_acceptor_state;

struct paxos_client_value
{
	paxos_value value;
};
typedef struct paxos_client_value paxos_client_value;


struct paxos_message
{
	paxos_message_type type;
	union
	{
		paxos_prepare prepare;
		paxos_promise promise;
		paxos_accept accept;
		paxos_accepted accepted;
		paxos_preempted preempted;
		paxos_repeat repeat;
		paxos_trim trim;
		paxos_acceptor_state state;
		paxos_client_value client_value;
	} u;
};
typedef struct paxos_message paxos_message;

typedef void (*peer_cb)(struct peer* p, paxos_message* m, void* arg);

struct subscription
{
	paxos_message_type type;
	peer_cb callback;
	void* arg;
};

struct peers
{
	int peers_count, clients_count;
	struct peer** peers;   /* peers we connected to */
	struct peer** clients; /* peers we accepted connections from */
	struct evconnlistener* listener;
	struct event_base* base;
	struct evpaxos_config* config;
	int subs_count;
	struct subscription subs[32];
};

#endif
