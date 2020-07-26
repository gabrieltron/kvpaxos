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
#include <utility>
#include <vector>

#include "graph/partitioning.h"
#include "partition.hpp"
#include "request/request.hpp"
#include "storage/storage.h"
#include "types/types.h"


namespace kvpaxos {

template <typename T>
class Scheduler {
public:

    Scheduler(int n_requests,
                int repartition_interval,
                int n_partitions,
                model::CutMethod repartition_method
    ) : n_partitions_{n_partitions},
        repartition_interval_{repartition_interval},
        repartition_method_{repartition_method}
    {
        for (auto i = 0; i < n_partitions_; i++) {
            auto* partition = new Partition<T>(i);
            partitions_.emplace(i, partition);
        }
        data_to_partition_ = new std::unordered_map<T, Partition<T>*>();

        pthread_barrier_init(&start_repartition_barrier_, NULL, 2);
        pthread_barrier_init(&finish_repartition_barrier_, NULL, 2);
        repartition_thread_ = std::thread(&Scheduler<T>::repartition_data_, this);
    }

    ~Scheduler() {
        for (auto partition: partitions_) {
            delete partition;
        }
        delete data_to_partition_;
    }

    void process_populate_requests(const std::vector<workload::Request>& requests) {
        for (auto& request : requests) {
            add_key(request.key());
        }
        Partition<T>::populate_storage(requests);
    }

    void run() {
        for (auto& kv : partitions_) {
            kv.second->start_worker_thread();
        }
    }

    int n_executed_requests() {
        auto n_executed_requests = 0;
        for (auto& kv: partitions_) {
            auto* partition = kv.second;
            n_executed_requests += partition->n_executed_requests();
        }
        return n_executed_requests;
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
            return partitions_.at(0)->push_request(request);
        }

        auto arbitrary_partition = *begin(partitions);
        if (partitions.size() > 1) {
            sync_partitions(partitions);
            arbitrary_partition->push_request(request);
            sync_partitions(partitions);
        } else {
            arbitrary_partition->push_request(request);
        }

        n_dispatched_requests_++;
        if (repartition_interval_ > 0) {
            if (
                n_dispatched_requests_ % repartition_interval_ == 0
            ) {
                auto& execution_mutex = Partition<T>::execution_mutex();
                std::unique_lock lock(execution_mutex);

                pthread_barrier_wait(&start_repartition_barrier_);
                pthread_barrier_wait(&finish_repartition_barrier_);
            }
        }
    }

    std::vector<std::vector<std::pair<int, time_point>>>
    execution_timestamps() const {
        std::vector<std::vector<std::pair<int, time_point>>> timestamps;
        for (auto& kv: partitions_) {
            auto& partition = kv.second;
            timestamps.emplace_back(partition->execution_timestamps());
        }
        return timestamps;
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

            partitions.insert(data_to_partition_->at(request.key + i));
        }

        return partitions;
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

    void sync_partitions(const std::unordered_set<Partition<T>*>& partitions) {
        auto sync_message = std::move(
            create_sync_request(partitions.size())
        );
        for (auto partition : partitions) {
            partition->push_request(sync_message);
        }
    }

    void sync_all_partitions() {
        std::unordered_set<Partition<T>*> partitions;
        for (auto i = 0; i < partitions_.size(); i++) {
            partitions.insert(partitions_.at(i));
        }
        sync_partitions(partitions);
    }

    void add_key(T key) {
        auto partition_id = round_robin_counter_;
        partitions_.at(partition_id)->insert_data(key);
        data_to_partition_->emplace(key, partitions_.at(partition_id));

        round_robin_counter_ = (round_robin_counter_+1) % n_partitions_;
    }

    bool mapped(T key) const {
        return data_to_partition_->find(key) != data_to_partition_->end();
    }

    void repartition_data_() {
        while (true) {
            pthread_barrier_wait(&start_repartition_barrier_);

            auto& workload_graph = Partition<T>::workload_graph();
            auto partition_scheme = std::move(
                model::cut_graph(workload_graph, partitions_.size(), repartition_method_)
            );

            delete data_to_partition_;
            data_to_partition_ = new std::unordered_map<T, Partition<T>*>();
            auto sorted_vertex = std::move(workload_graph.sorted_vertex());
            for (auto i = 0; i < partition_scheme.size(); i++) {
                auto partition = partition_scheme[i];
                if (partition >= n_partitions_) {
                    printf("ERROR: partition was %d!\n", partition);
                    fflush(stdout);
                }
                auto data = sorted_vertex[i];
                data_to_partition_->emplace(data, partitions_.at(partition));
            }

            pthread_barrier_wait(&finish_repartition_barrier_);
        }
    }

    int n_partitions_;
    int round_robin_counter_ = 0;
    int sync_counter_ = 0;
    int n_dispatched_requests_ = 0;
    kvstorage::Storage storage_;
    std::unordered_map<int, Partition<T>*> partitions_;
    std::unordered_map<T, Partition<T>*>* data_to_partition_;

    model::CutMethod repartition_method_;
    int repartition_interval_;
    std::thread repartition_thread_;
    pthread_barrier_t start_repartition_barrier_, finish_repartition_barrier_;
};

};


#endif
