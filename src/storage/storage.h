#ifndef _KVPAXOS_STORAGE_H_
#define _KVPAXOS_STORAGE_H_


#include <string>
#include <unordered_map>
#include <vector>

#include "compresser/compresser.h"
#include "tbb/concurrent_unordered_map.h"
#include "types/types.h"


namespace kvstorage {

class Storage {
public:
    Storage() = default;

    std::string read(int key) const;
    void write(int key, const std::string& value);
    std::vector<std::string> scan(int start, int length);

private:
    tbb::concurrent_unordered_map<int, std::string> storage_;
};

};

#endif
