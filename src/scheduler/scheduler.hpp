#ifndef _KVPAXOS_SCHEDULER_H_
#define _KVPAXOS_SCHEDULER_H_


#include <memory>
#include <netinet/tcp.h>
#include <pthread.h>
#include <queue>
#include <semaphore.h>
#include <string>
#include <string.h>
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
        if (type == SYNC) {
            return;
        }

        if (type == WRITE) {
            if (not mapped(request.key)) {
                add_key(request.key);
            }
        }

        auto partitions = std::move(involved_partitions(request));
        if (partitions.empty()) {
            request.type = ERROR;
            return partitions_.at(0).push_request(request);
        }

        auto arbitrary_partition = *begin(partitions);
        if (partitions.size() > 1) {
            sync_partitions(partitions);
            arbitrary_partition->push_request(request);
            sync_partitions(partitions);
        } else {
            arbitrary_partition->push_request(request);
        }
    }

private:
    std::unordered_set<Partition<T>*> involved_partitions(
        const struct client_message& request)
    {
        std::unordered_set<Partition<T>*> partitions;
        auto type = static_cast<request_type>(request.type);

        auto range = 1;
        if (type == SCAN) {
            range = std::stoi(request.args);
        }

        for (auto i = 0; i < range; i++) {
            if (not mapped(request.key + i)) {
                return std::unordered_set<Partition<T>*>();
            }

            partitions.insert(data_to_partition_.at(request.key + i));
        }

        return partitions;
    }

    struct client_message create_sync_request(int coordinator_id) {
        struct client_message sync_message;
        sync_message.id = sync_counter_;
        sync_message.type = SYNC;
        sync_message.key = 0;

        auto string_id = std::to_string(coordinator_id);
        memcpy(sync_message.args, string_id.c_str(), string_id.size());
        sync_message.args[string_id.size()] = '\0';
        sync_counter_++;
        return sync_message;
    }

    void sync_partitions(const std::unordered_set<Partition<T>*>& partitions) {
        auto arbitrary_partition = *begin(partitions);
        auto sync_message = std::move(
            create_sync_request(arbitrary_partition->id())
        );
        arbitrary_partition->start_barrier(sync_message.id, partitions.size());
        for (auto partition : partitions) {
            partition->push_request(sync_message);
        }
    }

    void add_key(T key) {
        auto partition_id = round_robin_counter_;
        partitions_.at(partition_id).insert_data(key);
        data_to_partition_[key] = &partitions_.at(partition_id);

        round_robin_counter_ = (round_robin_counter_+1) % n_partitions_;
    }

    bool mapped(T key) const {
        return data_to_partition_.find(key) != data_to_partition_.end();
    }

    int n_partitions_;
    int round_robin_counter_;
    int sync_counter_;
    kvstorage::Storage storage_;
    std::unordered_map<int, Partition<T>> partitions_;
    std::unordered_map<T, Partition<T>*> data_to_partition_;
};

};


#endif
