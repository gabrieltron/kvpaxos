#include <iostream>
#include <string>

#include "evclient/evclient.h"


const int OUTSTANDING = 1;
const int VALUE_SIZE = 64;


void usage(std::string name) {
    std::cout << "Usage: " << name << " id client_config request_config\n";
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        usage(std::string(argv[0]));
        exit(1);
    }

    auto client_id = atoi(argv[1]);
    auto client_config = std::string(argv[2]);
    auto request_config = std::string(argv[3]);

    auto* client = start_client(
        client_config.c_str(), client_id, OUTSTANDING, VALUE_SIZE,
        nullptr, nullptr
    );

    return 0;
}
