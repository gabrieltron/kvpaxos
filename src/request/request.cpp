#include "request.hpp"

namespace workload {

std::ostream& operator<<(std::ostream &stream, const Request& request) {
    stream << std::string("(");
    auto counter = 0;
    for (auto& variable : request) {
        stream << std::to_string(variable);
        counter++;
        if (counter != request.size()) {
            stream << std::string(",");
        }
    }
    stream << std::string(")");
    return stream;
}

}
