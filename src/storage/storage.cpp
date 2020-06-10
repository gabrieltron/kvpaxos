#include "storage.h"


namespace kvstorage {

std::string Storage::read(int key) const {
    return storage_.at(key);
}

std::string Storage::write(int key, const std::string& value) {
    storage_[key] = value;
}

std::vector<std::string> Storage::scan(int start, int length) {
    auto values = std::vector<std::string>();
    for (auto i = 0; i < length; i++) {
        auto key = (start + i) % storage_.size();
        values.push_back(storage_[key]);
    }
    return values;
}

};
