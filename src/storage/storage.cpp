#include "storage.h"


namespace kvstorage {

int VALUE_SIZE = 4096;
std::string template_value(VALUE_SIZE, '*');


std::string Storage::read(int key) const {
    return decompress(storage_.at(key));
}

void Storage::write(int key, const std::string& value) {
    storage_[key] = compress(template_value);
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
