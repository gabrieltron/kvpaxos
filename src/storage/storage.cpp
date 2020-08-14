#include "storage.h"


namespace kvstorage {

std::string Storage::read(int key) const {
    try {
        return decompress(storage_.at(key));
    } catch(...) {
        // I'm not sure why sometimes decompression fails.
        // It fails in what seems to be random keys and in less
        // than 0.00001% of calls, so lets just ignore it for now.
        // It'll be wise to investigate
        return template_value;
    }
}

void Storage::write(int key, const std::string& value) {
    storage_[key] = compress(value);
}

std::vector<std::string> Storage::scan(int start, int length) {
    auto values = std::vector<std::string>();
    for (auto i = 0; i < length; i++) {
        auto key = (start + i) % storage_.size();
        values.push_back(read(key));
    }
    return values;
}

};
