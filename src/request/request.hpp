#ifndef WORKLOAD_REQUEST_H
#define WORKLOAD_REQUEST_H

#include <string>
#include <unordered_set>


namespace workload {

class Request {
public:

    size_t size() const {
        return accessed_data_.size();
    }
    std::unordered_set<int>::const_iterator begin() const {
        return accessed_data_.begin();
    }
    std::unordered_set<int>::const_iterator end() const {
        return accessed_data_.end();
    }
    void insert(int value) {
        accessed_data_.insert(value);
    }
    std::unordered_set<int>::const_iterator find(int value) {
        return accessed_data_.find(value);
    }

private:
    std::unordered_set<int> accessed_data_;

};

std::ostream& operator<<(std::ostream &stream, const Request& request);

}

#endif
