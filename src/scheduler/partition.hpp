#ifndef KVPAXOS_PARTITION_H
#define KVPAXOS_PARTITION_H


#include <arpa/inet.h>
#include <chrono>
#include <evpaxos.h>
#include <pthread.h>
#include <queue>
#include <iterator>
#include <mutex>
#include <numeric>
#include <semaphore.h>
#include <sstream>
#include <shared_mutex>
#include <string>
#include <string.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "graph/graph.hpp"
#include "request/request.hpp"
#include "storage/storage.h"
#include "types/types.h"


namespace kvpaxos {

template <typename T>
class Partition {
public:
    Partition(int id, int n_requests, pthread_barrier_t* end_barrier)
        : id_{id},
          n_requests_{n_requests},
          end_barrier_{end_barrier},
          executing_{true}
    {
        socket_fd_ = socket(AF_INET, SOCK_DGRAM, 0);
    }

    ~Partition() {
        executing_ = false;
        if (worker_thread_.joinable()) {
            sem_post(&semaphore_);
            worker_thread_.join();
        }
    }

    static void populate_storage(const std::vector<workload::Request>& requests) {
        for (auto& request : requests) {
            if (request.type() != WRITE) {
                continue;
            }

            storage_.write(request.key(), request.args());
            workload_graph_.add_vertice(request.key());
        }
    }

    void start_worker_thread() {
        sem_init(&semaphore_, 0, 0);
        worker_thread_ = std::thread(&Partition<T>::thread_loop, this);
    }

    void push_request(struct client_message request) {
        queue_mutex_.lock();
            requests_queue_.push(std::move(request));
        queue_mutex_.unlock();
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

    int id() const {
        return id_;
    }

    const std::unordered_set<T>& data() const {
        return data_set_;
    }

    const std::vector<std::pair<int, time_point>>& execution_timestamps() const {
        return execution_timestamps_;
    }

    static const model::Graph<T>& workload_graph() {
        return workload_graph_;
    }

    static int n_executed_requests() {
        return n_executed_requests_;
    }

    static std::shared_mutex& execution_mutex() {
        return execution_mutex_;
    }

private:
    void on_event(struct bufferevent* bev, short ev, void *arg)
    {
        if (ev & BEV_EVENT_EOF || ev & BEV_EVENT_ERROR) {
            bufferevent_free(bev);
        }
    }

    struct sockaddr_in get_client_addr(unsigned long ip, unsigned short port)
    {
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = ip;
        addr.sin_port = port;
        return addr;
    }

    void answer_client(const char* answer, size_t length,
        client_message& message)
    {
        auto client_addr = get_client_addr(message.s_addr, message.sin_port);
        auto bytes_written = sendto(
            socket_fd_, answer, length, 0,
            (const struct sockaddr *) &client_addr, sizeof(client_addr)
        );
        if (bytes_written < 0) {
            printf("Failed to send answer\n");
        }
    }

    void update_graph(const std::vector<T>& data) {
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

    void thread_loop() {
        while (executing_) {
            sem_wait(&semaphore_);
            if (not executing_) {
                return;
            }
            std::shared_lock lock(execution_mutex_);

            queue_mutex_.lock();
                auto request = std::move(requests_queue_.front());
                requests_queue_.pop();
            queue_mutex_.unlock();

            auto type = static_cast<request_type>(request.type);
            auto key = request.key;
            auto request_args = std::string(request.args);

            std::string answer;
            switch (type)
            {
            case READ:
            {
                answer = std::move(storage_.read(key));
                update_graph(std::vector<T>{key});
                break;
            }

            case WRITE:
            {
                storage_.write(key, request_args);
                answer = request_args;
                update_graph(std::vector<T>{key});
                break;
            }

            case SCAN:
            {
                auto length = std::stoi(request_args);
                auto values = std::move(storage_.scan(key, length));


                std::ostringstream oss;
                std::copy(values.begin(), values.end(), std::ostream_iterator<std::string>(oss, ","));
                answer = std::string(oss.str());

                std::vector<T> keys(length);
                std::iota(keys.begin(), keys.end(), 1);
                update_graph(keys);
                break;
            }

            case SYNC:
            {
                auto barrier = (pthread_barrier_t*) request.s_addr;
                auto coordinator = pthread_barrier_wait(barrier);
                if (coordinator) {
                    pthread_barrier_destroy(barrier);
                    delete barrier;
                }
                break;
            }

            case ERROR:
                answer = "ERROR";
                break;
            default:
                break;
            }

            if (type == SYNC) {
                continue;
            }

            reply_message reply;
            reply.id = request.id;
            strncpy(reply.answer, answer.c_str(), answer.size());
            reply.answer[answer.size()] = '\0';

            //answer_client((char *)&reply, sizeof(reply_message), request);

            if (request.record_timestamp) {
                auto timestamp = std::chrono::system_clock::now();
                execution_timestamps_.emplace_back(request.id, timestamp);
            }

            {
                std::lock_guard<std::mutex> lk(executed_requests_mutex_);
                n_executed_requests_++;
            }

            if (n_executed_requests_ == n_requests_) {
                pthread_barrier_wait(end_barrier_);
            }
        }
    }

    int id_, n_requests_, socket_fd_;
    static kvstorage::Storage storage_;
    static model::Graph<T> workload_graph_;
    static int n_executed_requests_;
    static std::mutex executed_requests_mutex_;

    bool executing_;
    pthread_barrier_t* end_barrier_;
    std::thread worker_thread_;
    sem_t semaphore_;
    std::queue<struct client_message> requests_queue_;
    std::mutex queue_mutex_;
    static std::shared_mutex execution_mutex_;
    std::vector<std::pair<int, time_point>> execution_timestamps_;

    int total_weight_ = 0;
    std::unordered_set<T> data_set_;
    std::unordered_map<T, int> weight_;
};

template<typename T>
kvstorage::Storage Partition<T>::storage_;
template<typename T>
model::Graph<T> Partition<T>::workload_graph_;
template<typename T>
int Partition<T>::n_executed_requests_ = 0;
template<typename T>
std::mutex Partition<T>::executed_requests_mutex_;
template<typename T>
std::shared_mutex Partition<T>::execution_mutex_;

}

#endif
