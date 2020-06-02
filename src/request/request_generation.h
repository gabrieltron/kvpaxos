#ifndef WORKLOAD_REQUEST_GENERATOR_H
#define WORKLOAD_REQUEST_GENERATOR_H

#include <algorithm>
#include <functional>
#include <random>
#include <unordered_set>
#include <vector>

#include <toml11/toml.hpp>
#include "random.h"

namespace workload {

typedef std::unordered_set<int> Request;
typedef toml::basic_value<toml::discard_comments, std::unordered_map> toml_config;

std::vector<Request> create_requests(std::string config_path);
std::vector<Request> import_requests(const toml_config& config);
std::vector<workload::Request> generate_single_data_requests(
    const toml_config& config
);
std::vector<workload::Request> generate_multi_data_requests(
    const toml_config& config
);
std::vector<Request> random_single_data_requests(
    int n_requests,
    rfunc::RandFunction& data_rand
);
std::vector<Request> generate_fixed_data_requests(
    int n_variables, int requests_per_variable
);
std::vector<Request> random_multi_data_requests(
    int n_requests,
    int n_variables,
    rfunc::RandFunction& data_rand,
    rfunc::RandFunction& size_rand
);
void shuffle_requests(std::vector<Request>& requests);
std::vector<Request> merge_requests(
    std::vector<Request> single_data_requests,
    std::vector<Request> multi_data_requests,
    int single_data_pick_probability
);

}

#endif
