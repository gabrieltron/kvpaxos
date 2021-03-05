#ifndef _KVPAXOS_SCHEDULER_H_
#define _KVPAXOS_SCHEDULER_H_


#include <condition_variable>
#include <memory>
#include <netinet/tcp.h>
#include <pthread.h>
#include <queue>
#include <semaphore.h>
#include <shared_mutex>
#include <string>
#include <string.h>
#include <thread>
#include <unordered_map>
#include <vector>

#include "graph/partitioning.h"
#include "partition.hpp"
#include "pattern_tracker.hpp"
#include "request/request.hpp"
#include "storage/storage.h"
#include "types/types.h"


namespace kvpaxos {

template <typename T>
class Scheduler {
public:

    Scheduler(int repartition_interval,
                int n_partitions,
                model::CutMethod repartition_method
    ) : n_partitions_{n_partitions},
        repartition_interval_{repartition_interval},
        repartition_method_{repartition_method},
        pattern_tracker_{PatternTracker<T>(n_partitions)},
        data_to_partition_id_{std::unordered_map<T, int>()}
    {
        for (auto i = 0; i < n_partitions_; i++) {
            partitions_.emplace(i, i);
        }

        pthread_barrier_init(&repartition_barrier_, NULL, 2);
    }

    void populate_n_initial_keys(int n_keys) {
        for (auto i = 0; i < n_keys; i++) {
            add_key(i);
        }
        Partition<T>::populate_n_initial_keys(n_keys);
    }

    void run() {
        for (auto& kv : partitions_) {
            kv.second.start_worker_thread();
        }
        pattern_tracker_.run();
    }

    int n_executed_requests() {
        return Partition<T>::n_executed_requests();
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

        auto involved_partitions_ids = std::move(involved_partitions(request));
        if (involved_partitions_ids.empty()) {
            request.type = ERROR;
            return partitions_.at(0).push_request(request);
        }

        auto arbitrary_partition_id = *begin(involved_partitions_ids);
        auto& arbitrary_partition = partitions_.at(arbitrary_partition_id);
        if (involved_partitions_ids.size() > 1) {
            sync_partitions(involved_partitions_ids);
            arbitrary_partition.push_request(request);
            sync_partitions(involved_partitions_ids);
        } else {
            arbitrary_partition.push_request(request);
        }

        if (repartition_method_ != model::ROUND_ROBIN) {
            pattern_tracker_.push_request(request);
            pattern_tracker_.register_access(involved_partitions_ids);
            n_dispatched_requests_++;
            if (
                n_dispatched_requests_ % repartition_interval_ == 0
            ) {
                struct client_message sync_message;
                sync_message.type = SYNC;
                sync_message.s_addr = (unsigned long) &repartition_barrier_;

                pattern_tracker_.push_request(sync_message);
                pthread_barrier_wait(&repartition_barrier_);
                repartition_data();
                sync_all_partitions();
            }
        }
    }

private:
    std::unordered_set<int> involved_partitions(
        const struct client_message& request)
    {
        std::unordered_set<int> involved_partitions_ids;
        auto type = static_cast<request_type>(request.type);

        auto range = 1;
        if (type == SCAN) {
            range = std::stoi(request.args);
        }

        for (auto i = 0; i < range; i++) {
            if (not mapped(request.key + i)) {
                return std::unordered_set<int>();
            }

            involved_partitions_ids.insert(data_to_partition_id_.at(request.key + i));
        }

        return involved_partitions_ids;
    }

    struct client_message create_sync_request(int n_partitions) {
        struct client_message sync_message;
        sync_message.id = sync_counter_;
        sync_message.type = SYNC;

        // this is a gross workaround to send the barrier to the partitions.
        // a more elegant approach would be appreciated.
        auto* barrier = new pthread_barrier_t();
        pthread_barrier_init(barrier, NULL, n_partitions);
        sync_message.s_addr = (unsigned long) barrier;

        return sync_message;
    }

    void sync_partitions(const std::unordered_set<int>& partitions_ids) {
        auto sync_message = create_sync_request(partitions_ids.size());
        for (auto partition_id : partitions_ids) {
            auto& partition = partitions_.at(partition_id);
            partition.push_request(sync_message);
        }
    }

    void sync_all_partitions() {
        std::unordered_set<int> partitions_ids;
        for (auto i = 0; i < partitions_.size(); i++) {
            partitions_ids.insert(i);
        }
        sync_partitions(partitions_ids);
    }

    void add_key(T key) {
        auto partition_id = round_robin_counter_;
        partitions_.at(partition_id).insert_data(key);
        data_to_partition_id_.emplace(key, partition_id);
        round_robin_counter_ = (round_robin_counter_+1) % n_partitions_;
    }

    bool mapped(T key) const {
        return data_to_partition_id_.find(key) != data_to_partition_id_.end();
    }

    void repartition_data() {
        const auto& workload_graph = pattern_tracker_.workload_graph();
        auto accesses_per_partition = pattern_tracker_.accesses_per_partition();
        auto partition_scheme = model::cut_graph(
            workload_graph,
            data_to_partition_id_,
            accesses_per_partition,
            repartition_method_
        );

        auto sorted_vertex = std::move(workload_graph.sorted_vertex());
        data_to_partition_id_ = std::unordered_map<T, int>();
        pattern_tracker_.reset_accesses();
        for (auto i = 0; i < partition_scheme.size(); i++) {
            auto partition_id = partition_scheme[i];
            if (partition_id >= n_partitions_) {
                printf("ERROR: partition was %d!\n", partition_id);
                fflush(stdout);
            }

            auto data = sorted_vertex[i];
            data_to_partition_id_.emplace(data, partition_id);
            auto vertice_weight = pattern_tracker_.workload_graph().vertice_weight(data);
            pattern_tracker_.register_accesses_to_partition(partition_id, vertice_weight);
        }
    }

    int n_partitions_;
    int round_robin_counter_ = 0;
    int sync_counter_ = 0;
    int n_dispatched_requests_ = 0;
    PatternTracker<T> pattern_tracker_;
    kvstorage::Storage storage_;
    std::unordered_map<int, Partition<T>> partitions_;
    std::unordered_map<T, int> data_to_partition_id_;

    model::CutMethod repartition_method_;
    int repartition_interval_;
    pthread_barrier_t repartition_barrier_;
};

};


#endif
