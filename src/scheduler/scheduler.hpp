#ifndef _KVPAXOS_SCHEDULER_H_
#define _KVPAXOS_SCHEDULER_H_


#include <condition_variable>
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

#include "graph/partitioning.h"
#include "partition.hpp"
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
        executing_{true},
        new_partition_ready_{false}
    {
        for (auto i = 0; i < n_partitions_; i++) {
            partitions_.emplace(i, i);
        }
        data_to_partition_ = new std::unordered_map<T, Partition<T>*>();
        repartition_thread_ = std::thread(&Scheduler<T>::repartition_data_, this);
    }

    ~Scheduler() {
        executing_ = false;
        repartition_cv_.notify_one();
        repartition_thread_.join();
        delete data_to_partition_;
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
        if (new_partition_ready_) {
            std::lock_guard<std::mutex> lk(update_partition_mutex_);
            sync_all_partitions();
            delete data_to_partition_;
            data_to_partition_ = updata_data_to_partition_aux_;
            new_partition_ready_ = false;
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

        if (repartition_interval_ > 0) {
            if (
                Partition<T>::n_executed_requests() % repartition_interval_ == 0
                and Partition<T>::n_executed_requests() > 0
            ) {
                repartition_cv_.notify_one();
            }
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
            partitions.insert(&partitions_.at(i));
        }
        sync_partitions(partitions);
    }

    void add_key(T key) {
        auto partition_id = round_robin_counter_;
        partitions_.at(partition_id).insert_data(key);
        data_to_partition_->emplace(key, &partitions_.at(partition_id));

        round_robin_counter_ = (round_robin_counter_+1) % n_partitions_;
    }

    bool mapped(T key) const {
        return data_to_partition_->find(key) != data_to_partition_->end();
    }

    void repartition_data_() {
        while (executing_) {
            std::unique_lock<std::mutex> lk(repartition_mutex_);
            repartition_cv_.wait(lk);
            if (not executing_) {
                return;
            }

            auto& workload_graph = Partition<T>::workload_graph();
            auto partition_scheme = std::move(
                model::cut_graph(workload_graph, partitions_.size(), repartition_method_)
            );

            std::lock_guard<std::mutex> upd_lk(update_partition_mutex_);
            updata_data_to_partition_aux_ = new std::unordered_map<T, Partition<T>*>();
            for (auto data = 0; data < partition_scheme.size(); data++) {
                auto partition = partition_scheme[data];
                updata_data_to_partition_aux_->emplace(data, &partitions_.at(partition));
            }

            new_partition_ready_ = true;
        }
    }

    int n_partitions_;
    int round_robin_counter_ = 0;
    int sync_counter_ = 0;
    bool executing_;
    kvstorage::Storage storage_;
    std::unordered_map<int, Partition<T>> partitions_;
    std::unordered_map<T, Partition<T>*>* data_to_partition_;

    model::CutMethod repartition_method_;
    int repartition_interval_;
    bool new_partition_ready_;
    std::thread repartition_thread_;
    std::condition_variable repartition_cv_;
    std::mutex repartition_mutex_, update_partition_mutex_;
    std::unordered_map<T, Partition<T>*>* updata_data_to_partition_aux_;
};

};


#endif
