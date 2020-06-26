#ifndef KVPAXOS_PARTITION_H
#define KVPAXOS_PARTITION_H


#include <arpa/inet.h>
#include <evpaxos.h>
#include <pthread.h>
#include <queue>
#include <iterator>
#include <mutex>
#include <semaphore.h>
#include <sstream>
#include <string>
#include <string.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>

#include "storage/storage.h"
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

private:
    void on_event(struct bufferevent* bev, short ev, void *arg)
    {
        if (ev & BEV_EVENT_EOF || ev & BEV_EVENT_ERROR) {
            bufferevent_free(bev);
        }
    }

    int connect_to_client(unsigned long ip, unsigned short port)
    {
        auto fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            printf("Failed to create socket.\n");
            return -1;
        }
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = ip;
        addr.sin_port = port;
        auto failed = connect(fd, (struct sockaddr*)&addr, sizeof(addr));
        if (failed) {
            printf("Failed to stablish connection to client.\n");
            close(fd);
            return -1;
        }
        return fd;
    }

    void answer_client(const char* answer, size_t length,
        client_message& message)
    {
        auto fd = connect_to_client(message.s_addr, message.sin_port);
        if (fd < 0) {
            return;
        }

        auto bytes_written = write(fd, answer, length);
        if (bytes_written < 0) {
            printf("Failed to send answer\n");
        }

        close(fd);
    }

    void thread_loop() {
        while (executing_) {
            sem_wait(&semaphore_);
            if (not executing_) {
                return;
            }

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
                break;
            }

            case WRITE:
            {
                storage_.write(key, request_args);
                answer = request_args;
                break;
            }

            case SCAN:
            {
                auto length = std::stoi(request_args);
                auto values = std::move(storage_.scan(key, length));

                std::ostringstream oss;
                std::copy(values.begin(), values.end(), std::ostream_iterator<std::string>(oss, ","));
                answer = std::string(oss.str());
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

            answer_client((char *)&reply, sizeof(reply_message), request);
        }
    }

    int id_;
    static kvstorage::Storage storage_;

    bool executing_;
    std::thread worker_thread_;
    sem_t semaphore_;
    std::queue<struct client_message> requests_queue_;
    std::mutex queue_mutex_;

    int total_weight_ = 0;
    std::unordered_set<T> data_set_;
    std::unordered_map<T, int> weight_;
};

template<typename T>
kvstorage::Storage Partition<T>::storage_;

}

#endif
