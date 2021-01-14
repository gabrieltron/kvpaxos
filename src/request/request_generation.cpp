#include "request_generation.h"


namespace workload {

Request make_request(char* type_buffer, char* key_buffer, char* arg_buffer) {
    auto type = static_cast<request_type>(std::stoi(type_buffer));
    auto key = std::stoi(key_buffer);
    auto arg = std::string(arg_buffer);

    return Request(type, key, arg);
}

std::vector<Request> import_cs_requests(const std::string& file_path)
{
    std::ifstream infile(file_path);

    std::vector<Request> requests;
    std::string line;
    char type_buffer[2];
    char key_buffer[11];
    char arg_buffer[129];
    auto* reading_buffer = type_buffer;
    auto buffer_index = 0;
    while (std::getline(infile, line)) {
        for (auto& character: line) {
            if (character == ',') {
                reading_buffer[buffer_index] = '\0';
                if (reading_buffer == type_buffer) {
                    reading_buffer = key_buffer;
                } else if (reading_buffer == key_buffer) {
                    reading_buffer = arg_buffer;
                } else {
                    reading_buffer = type_buffer;
                    requests.emplace_back(make_request(
                        type_buffer,
                        key_buffer,
                        arg_buffer
                    ));
                }
                buffer_index = 0;
            } else {
                reading_buffer[buffer_index] = character;
                buffer_index++;
            }
        }
    }

    requests.emplace_back(make_request(type_buffer, key_buffer, arg_buffer));
    return requests;
}

std::vector<Request> import_requests(const std::string& file_path,
    const std::string& field)
    {
    const auto file = toml::parse(file_path);
    auto str_requests = toml::find<std::vector<std::vector<std::string>>>(
        file, field
    );

    auto requests = std::vector<Request>();
    for (auto& request : str_requests) {
        auto type = static_cast<request_type>(std::stoi(request[0]));
        auto key = std::stoi(request[1]);
        auto arg = request[2];

        requests.push_back(Request(type, key, arg));
    }

    return requests;
}

/*
std::vector<Request> create_requests(
    std::string config_path
) {
    const auto config = toml::parse(config_path);
    const auto import = toml::find<bool>(
        config, "workload", "requests", "import_requests"
    );
    if (import) {
        return import_requests(config_path);
    }

    auto single_data_requests = generate_single_data_requests(config);
    auto multi_data_requests = generate_multi_data_requests(config);

    const auto single_data_pick_probability = toml::find<int>(
        config, "workload", "requests", "single_data_pick_probability"
    );
    auto requests = workload::merge_requests(
        single_data_requests,
        multi_data_requests,
        single_data_pick_probability
    );

    return requests;
}

std::vector<workload::Request> generate_single_data_requests(
    const toml_config& config
) {
    const auto single_data_distributions = toml::find<std::vector<std::string>>(
        config, "workload", "requests", "single_data", "distribution_pattern"
    );
    const auto n_requests = toml::find<std::vector<int>>(
        config, "workload", "requests", "single_data", "n_requests"
    );
    const auto n_variables = toml::find<int>(
        config, "workload", "n_variables"
    );

    auto requests = std::vector<workload::Request>();
    auto binomial_counter = 0;
    for (auto i = 0; i < n_requests.size(); i++) {
        auto current_distribution_ = single_data_distributions[i];
        auto current_distribution = rfunc::string_to_distribution.at(
            current_distribution_
        );

        auto new_requests = std::vector<workload::Request>();
        if (current_distribution == rfunc::FIXED) {
            const auto requests_per_data = floor(n_variables);

            new_requests = workload::generate_fixed_data_requests(
                n_variables, requests_per_data
            );
        } else if (current_distribution == rfunc::UNIFORM) {
            auto data_rand = rfunc::uniform_distribution_rand(
                0, n_variables-1
            );

            new_requests = workload::random_single_data_requests(
                n_requests[i], data_rand
            );
        } else if (current_distribution == rfunc::BINOMIAL) {
            const auto success_probability = toml::find<std::vector<double>>(
                config, "workload", "requests", "single_data", "success_probability"
            );
            auto data_rand = rfunc::binomial_distribution(
                n_variables-1, success_probability[binomial_counter]
            );

            new_requests = workload::random_single_data_requests(
                n_requests[i], data_rand
            );
            binomial_counter++;
        }

        requests.insert(requests.end(), new_requests.begin(), new_requests.end());
    }

    return requests;
}

std::vector<workload::Request> generate_multi_data_requests(
    const toml_config& config
) {
    const auto n_requests = toml::find<std::vector<int>>(
        config, "workload", "requests", "multi_data", "n_requests"
    );

    const auto min_involved_data = toml::find<std::vector<int>>(
        config, "workload", "requests", "multi_data",
        "min_involved_data"
    );
    const auto max_involved_data = toml::find<std::vector<int>>(
        config, "workload", "requests", "multi_data",
        "max_involved_data"
    );

    const auto size_distribution_ = toml::find<std::vector<std::string>>(
        config, "workload", "requests", "multi_data", "size_distribution_pattern"
    );
    const auto data_distribution_ = toml::find<std::vector<std::string>>(
        config, "workload", "requests", "multi_data", "data_distribution_pattern"
    );
    const auto n_variables = toml::find<int>(
        config, "workload", "n_variables"
    );

    auto size_binomial_counter = 0;
    auto data_binomial_counter = 0;
    auto requests = std::vector<workload::Request>();
    for (auto i = 0; i < n_requests.size(); i++) {
        auto new_requests = std::vector<workload::Request>();

        auto size_distribution = rfunc::string_to_distribution.at(
            size_distribution_[i]
        );
        rfunc::RandFunction size_rand;
        if (size_distribution == rfunc::UNIFORM) {
            size_rand = rfunc::uniform_distribution_rand(
                min_involved_data[i], max_involved_data[i]
            );
        } else if (size_distribution == rfunc::FIXED) {
            size_rand = rfunc::fixed_distribution(max_involved_data[i]);
        } else if (size_distribution == rfunc::BINOMIAL) {
            const auto success_probability = toml::find<std::vector<double>>(
                config, "workload", "requests", "multi_data",
                "size_success_probability"
            );
            size_rand = rfunc::ranged_binomial_distribution(
                min_involved_data[i], max_involved_data[i],
                success_probability[size_binomial_counter]
            );
            size_binomial_counter++;
        }

        auto data_distribution = rfunc::string_to_distribution.at(
            data_distribution_[i]
        );
        rfunc::RandFunction data_rand;
        if (data_distribution == rfunc::UNIFORM) {
            data_rand = rfunc::uniform_distribution_rand(0, n_variables-1);
        } else if (data_distribution == rfunc::BINOMIAL) {
            const auto success_probability = toml::find<std::vector<double>>(
                config, "workload", "requests", "multi_data",
                "data_success_probability"
            );
            data_rand = rfunc::binomial_distribution(
                n_variables-1, success_probability[data_binomial_counter]
            );
            data_binomial_counter++;
        }

        new_requests = workload::random_multi_data_requests(
            n_requests[i],
            n_variables,
            data_rand,
            size_rand
        );
        requests.insert(requests.end(), new_requests.begin(), new_requests.end());
    }

    return requests;
}

std::vector<Request> random_single_data_requests(
    int n_requests, rfunc::RandFunction& data_rand
) {
    auto requests = std::vector<Request>();
    for (auto i = 0; i < n_requests; i++) {
        auto request = Request();
        auto data = data_rand();
        request.insert(data);
        requests.push_back(request);
    }
    return requests;
}

std::vector<Request> generate_fixed_data_requests(
    int n_variables, int requests_per_variable
) {
    auto requests = std::vector<Request>();
    for (auto i = 0; i < n_variables; i++) {
        for (auto j = 0; j < requests_per_variable; j++) {
            auto request = Request();
            request.insert(i);
            requests.push_back(request);
        }
    }

    shuffle_requests(requests);
    return requests;
}

std::vector<Request> random_multi_data_requests(
    int n_requests,
    int n_variables,
    rfunc::RandFunction& data_rand,
    rfunc::RandFunction& size_rand
) {
    auto requests = std::vector<Request>();

    for (auto i = 0; i < n_requests; i++) {
        auto request = Request();
        auto request_size = size_rand();

        for (auto j = 0; j < request_size; j++) {
            auto data = data_rand();
            while (request.find(data) != request.end()) {
                data = data_rand();
            }
            request.insert(data);
        }

        requests.push_back(request);
    }
    return requests;
}

void shuffle_requests(std::vector<Request>& requests) {
    auto rng = std::default_random_engine {};
    std::shuffle(std::begin(requests), std::end(requests), rng);
}

std::vector<Request> merge_requests(
    std::vector<Request> single_data_requests,
    std::vector<Request> multi_data_requests,
    int single_data_pick_probability
) {
    auto shuffled_requests = std::vector<Request>();
    auto rand = rfunc::uniform_distribution_rand(1, 100);
    std::reverse(single_data_requests.begin(), single_data_requests.end());
    std::reverse(multi_data_requests.begin(), multi_data_requests.end());

    while (!single_data_requests.empty() and !multi_data_requests.empty()) {
        auto random_value = rand();

        if (single_data_pick_probability > random_value) {
            shuffled_requests.push_back(single_data_requests.back());
            single_data_requests.pop_back();
        } else {
            shuffled_requests.push_back(multi_data_requests.back());
            multi_data_requests.pop_back();
        }

    }

    while (!single_data_requests.empty()) {
        shuffled_requests.push_back(single_data_requests.back());
        single_data_requests.pop_back();
    }
    while (!multi_data_requests.empty()) {
        shuffled_requests.push_back(multi_data_requests.back());
        multi_data_requests.pop_back();
    }

    return shuffled_requests;
}
*/

}
