#include "storage.h"


namespace kvstorage {

const int VALUE_SIZE = 128;
std::string TEMPLATE_VALUE(VALUE_SIZE, '*');


std::string Storage::read(int key) const {
    return decompress(storage_.at(key));
}

void Storage::write(int key, const std::string& value) {
    storage_[key] = compress(TEMPLATE_VALUE);
}

std::vector<std::string> Storage::scan(int start, int length) {
    auto values = std::vector<std::string>();
    for (auto i = 0; i < length; i++) {
        auto key = (start + i) % storage_.size();
        values.push_back(decompress(storage_[key]));
    }
    return values;
}

};
