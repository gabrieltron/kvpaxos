#ifndef KVPAXOS_PARTITION_H
#define KVPAXOS_PARTITION_H


#include <evpaxos.h>
#include <queue>
#include <semaphore.h>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "types/types.h"


namespace kvpaxos {

template <typename T>
class Partition {
public:
    Partition(int id)
        : id_{id},
          executing_{true}
    {}

    ~Partition() {
        executing_ = false;
        if (worker_thread_.joinable()) {
            sem_post(&semaphore_);
            worker_thread_.join();
        }
    }

    void start_worker_thread() {
        sem_init(&semaphore_, 0, 0);
        worker_thread_ = std::thread(&Partition<T>::thread_loop, this);
    }

    void push_request(struct client_message request) {
        requests_queue_.push(std::move(request));
        sem_post(&semaphore_);
    }

    void insert_data(const T& data, int weight = 0) {
        data_set_.insert(data);
        weight_[data] = weight;
        total_weight_ += weight;
    }

    void remove_data(const T& data) {
        data_set_.erase(data);
        total_weight_ -= weight_[data];
        weight_.erase(data);
    }

    void increase_weight(const T& data, int weight) {
        weight_[data] += weight;
        total_weight_ += weight;
    }

    int weight() const {
        return total_weight_;
    }

    const std::unordered_set<T>& data() const {
        return data_set_;
    }

private:
    void thread_loop() {
        while (executing_) {
            sem_wait(&semaphore_);
            if (not executing_) {
                return;
            }

            auto request = std::move(requests_queue_.front());
            requests_queue_.pop();

            printf("Hey, look at me, thread %d executing request %d\n", id_, request.id);
        }
    }

    int id_;

    bool executing_;
    std::thread worker_thread_;
    sem_t semaphore_;
    std::queue<struct client_message> requests_queue_;

    int total_weight_ = 0;
    std::unordered_set<T> data_set_;
    std::unordered_map<T, int> weight_;
};

}

#endif
