#ifndef RFUNC_RANDOM_H
#define RFUNC_RANDOM_H

#include <functional>
#include <random>
#include <unordered_map>

namespace rfunc {

typedef std::function<int()> RandFunction;

enum Distribution {FIXED, UNIFORM, BINOMIAL};
const std::unordered_map<std::string, Distribution> string_to_distribution({
    {"FIXED", Distribution::FIXED},
    {"UNIFORM", Distribution::UNIFORM},
    {"BINOMIAL", Distribution::BINOMIAL}
});

RandFunction uniform_distribution_rand(int min_value, int max_value);
RandFunction fixed_distribution(int value);
RandFunction binomial_distribution(
    int n_experiments, double success_probability
);
RandFunction ranged_binomial_distribution(
    int min_value, int n_experiments, double success_probability
);

}

#endif
