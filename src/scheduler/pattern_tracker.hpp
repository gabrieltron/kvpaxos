#ifndef KVPAXOS_PATTERN_TRACKER_H
#define KVPAXOS_PATTERN_TRACKER_H


#include <evpaxos/paxos.h>
#include <mutex>
#include <queue>
#include <semaphore.h>
#include <thread>
#include <unordered_set>

#include "graph/graph.hpp"
#include "types/types.h"


namespace kvpaxos {

template <typename T>
class PatternTracker {
public:
    PatternTracker<T>()
        : executing_{true},
          workload_graph_(model::Graph<T>())
    {}

    PatternTracker<T>(std::unordered_set<T> initial_variables)
        : executing_{true},
          workload_graph_(model::Graph<T>())
    {
        for (const auto& variable: initial_variables) {
            workload_graph_.add_vertice(variable);
        }
    }

    ~PatternTracker() {
        executing_ = false;
        if (update_thread_.joinable()) {
            sem_post(&semaphore_);
            update_thread_.join();
        }
    }

    const model::Graph<T>& workload_graph() {
        return workload_graph_;
    }

    void run() {
        sem_init(&semaphore_, 0, 0);
        executing_ = true;
        update_thread_ = std::thread(&PatternTracker<T>::thread_loop, this);
    }

    void push_request(const client_message& request) {
        {
            std::scoped_lock lock(queue_mutex_);
            requests_queue_.push(request);
        }
        sem_post(&semaphore_);
    }


private:
    void thread_loop() {
        while(executing_) {
            sem_wait(&semaphore_);
            if (not executing_) {
                return;
            }

            queue_mutex_.lock();
                auto request = std::move(requests_queue_.front());
                requests_queue_.pop();
            queue_mutex_.unlock();

            auto type = static_cast<request_type>(request.type);
            switch (type) {
            case SYNC:
            {
                auto barrier = (pthread_barrier_t*) request.s_addr;
                break;
            }
            default:
            {
                update_workload_graph(request);
                break;
            }
            }

        }
    }

    void update_workload_graph(const client_message& request) {
        std::vector<int> data{request.key};
        if (request.type == SCAN) {
            for (auto i = 1; i < std::stoi(request.args); i++) {
                data.emplace_back(request.key+i);
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
                if (not workload_graph_.are_connected(data[j], data[i])) {
                    workload_graph_.add_edge(data[j], data[i]);
                }

                workload_graph_.increase_edge_weight(data[i], data[j]);
                workload_graph_.increase_edge_weight(data[j], data[i]);
            }
        }

    }

    model::Graph<T> workload_graph_;

    bool executing_;
    std::thread update_thread_;
    std::queue<struct client_message> requests_queue_;
    sem_t semaphore_;
    std::mutex queue_mutex_;

};

}

#endif
