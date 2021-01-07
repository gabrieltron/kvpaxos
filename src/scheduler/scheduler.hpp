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

#include "graph/graph.hpp"
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
        repartition_method_{repartition_method}
    {
        for (auto i = 0; i < n_partitions_; i++) {
            partitions_.emplace(i, i);
        }
        data_to_partition_id_ = std::unordered_map<T, int>();

        sem_init(&graph_requests_semaphore_, 0, 0);
        pthread_barrier_init(&repartition_barrier_, NULL, 2);
        graph_thread_ = std::thread(&Scheduler<T>::update_graph_loop, this);
    }

    void process_populate_requests(const std::vector<workload::Request>& requests) {
        for (auto& request : requests) {
            add_key(request.key());
        }
        Partition<T>::populate_storage(requests);
    }

    void run() {
        for (auto& kv : partitions_) {
            kv.second.start_worker_thread();
        }
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

        auto partitions_ids = std::move(involved_partitions(request));
        if (partitions_ids.empty()) {
            request.type = ERROR;
            return partitions_.at(0).push_request(request);
        }

        auto arbitrary_partition_id = *begin(partitions_ids);
        auto& arbitrary_partition = partitions_.at(arbitrary_partition_id);
        if (partitions_ids.size() > 1) {
            sync_partitions(partitions_ids);
            arbitrary_partition.push_request(request);
            sync_partitions(partitions_ids);
        } else {
            arbitrary_partition.push_request(request);
        }

        if (repartition_method_ != model::ROUND_ROBIN) {
            graph_requests_mutex_.lock();
                graph_requests_queue_.push(request);
            graph_requests_mutex_.unlock();
            sem_post(&graph_requests_semaphore_);

            n_dispatched_requests_++;
            if (
                n_dispatched_requests_ % repartition_interval_ == 0
            ) {
                struct client_message sync_message;
                sync_message.type = SYNC;

                graph_requests_mutex_.lock();
                    graph_requests_queue_.push(sync_message);
                graph_requests_mutex_.unlock();
                sem_post(&graph_requests_semaphore_);

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

        if (repartition_method_ != model::ROUND_ROBIN) {
            struct client_message write_message;
            write_message.type = WRITE;
            write_message.key = key;

            graph_requests_mutex_.lock();
                graph_requests_queue_.push(write_message);
            graph_requests_mutex_.unlock();
            sem_post(&graph_requests_semaphore_);
        }
    }

    bool mapped(T key) const {
        return data_to_partition_id_.find(key) != data_to_partition_id_.end();
    }

    void update_graph_loop() {
        while(true) {
            sem_wait(&graph_requests_semaphore_);
            graph_requests_mutex_.lock();
                auto request = std::move(graph_requests_queue_.front());
                graph_requests_queue_.pop();
            graph_requests_mutex_.unlock();

            if (request.type == SYNC) {
                pthread_barrier_wait(&repartition_barrier_);
            } else {
                update_graph(request);
            }
        }
    }

    void update_graph(const client_message& message) {
        std::vector<int> data{message.key};
        if (message.type == SCAN) {
            for (auto i = 1; i < std::stoi(message.args); i++) {
                data.emplace_back(message.key+i);
            }
        }

        for (auto i = 0; i < data.size(); i++) {
            if (not workload_graph_.vertice_exists(data[i])) {
                workload_graph_.add_vertice(data[i]);
            }

            workload_graph_.increase_vertice_weight(data[i]);
            for (auto j = i+1; j < data.size(); j++) {
                if (not workload_graph_.vertice_exists(data[j])) {
                    workload_graph_.add_vertice(data[j]);
                }
                if (not workload_graph_.are_connected(data[i], data[j])) {
                    workload_graph_.add_edge(data[i], data[j]);
                }

                workload_graph_.increase_edge_weight(data[i], data[j]);
            }
        }
    }

    void repartition_data() {
        auto partition_scheme = std::move(
            model::cut_graph(workload_graph_, partitions_, repartition_method_)
        );

        auto sorted_vertex = std::move(workload_graph_.sorted_vertex());
        data_to_partition_id_ = std::unordered_map<T, int>();
        for (auto i = 0; i < partition_scheme.size(); i++) {
            auto partition_id = partition_scheme[i];
            if (partition_id >= n_partitions_) {
                printf("ERROR: partition was %d!\n", partition_id);
                fflush(stdout);
            }
            auto data = sorted_vertex[i];
            data_to_partition_id_.emplace(data, partition_id);
        }
    }

    int n_partitions_;
    int round_robin_counter_ = 0;
    int sync_counter_ = 0;
    int n_dispatched_requests_ = 0;
    kvstorage::Storage storage_;
    std::unordered_map<int, Partition<T>> partitions_;
    std::unordered_map<T, int> data_to_partition_id_;

    std::thread graph_thread_;
    std::queue<struct client_message> graph_requests_queue_;
    sem_t graph_requests_semaphore_;
    std::mutex graph_requests_mutex_;

    model::Graph<T> workload_graph_;
    model::CutMethod repartition_method_;
    int repartition_interval_;
    pthread_barrier_t repartition_barrier_;
};

};


#endif
