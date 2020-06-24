#ifndef _KVPAXOS_SCHEDULER_H_
#define _KVPAXOS_SCHEDULER_H_


#include <memory>
#include <netinet/tcp.h>
#include <pthread.h>
#include <queue>
#include <semaphore.h>
#include <thread>
#include <unordered_map>
#include <vector>

#include "partition.hpp"
#include "request/request.hpp"
#include "storage/storage.h"
#include "types/types.h"


namespace kvpaxos {

template <typename T>
class Scheduler {
public:

    Scheduler(int n_partitions)
        : n_partitions_{n_partitions}
    {
        for (auto i = 0; i < n_partitions_; i++) {
            partitions_.emplace(i, i);
        }
    }

    void run() {
        for (auto& kv : partitions_) {
            kv.second.start_worker_thread();
        }
    }

    void schedule_and_answer(struct client_message& request) {
        auto type = static_cast<request_type>(request.type);

        if (type == WRITE) {
            if (data_to_partition_.find(request.key) == data_to_partition_.end()) {
                auto partition_id = round_robin_counter_;
                partitions_.at(partition_id).insert_data(request.key);
                data_to_partition_[request.key] = &partitions_.at(partition_id);

                round_robin_counter_ = (round_robin_counter_+1) % n_partitions_;
            }
        }

        std::unordered_set<int> involved_partitions{request.key};

        auto partition = data_to_partition_[request.key];
        partition->push_request(request);
    }

private:
    int n_partitions_;
    int round_robin_counter_ = 0;
    kvstorage::Storage storage_;
    std::unordered_map<int, Partition<T>> partitions_;
    std::unordered_map<T, Partition<T>*> data_to_partition_;
};

};


#endif
