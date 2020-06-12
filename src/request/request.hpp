#ifndef WORKLOAD_REQUEST_H
#define WORKLOAD_REQUEST_H

#include <string>
#include <unordered_set>

#include "types/types.h"


namespace workload {

class Request {
public:

    Request(request_type type, int key, std::string args):
        type_{type},
        key_{key},
        args_{args}
    {}

    request_type type() const {return type_;}
    int key() const {return key_;}
    const std::string& args() const {return args_;}

private:
    request_type type_;
    int key_;
    std::string args_;
};

}

#endif
