#include "random.h"

namespace rfunc {

RandFunction uniform_distribution_rand(int min_value, int max_value) {
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<int> distribution(min_value, max_value);

    return std::bind(distribution, generator);
}

RandFunction fixed_distribution(int value) {
    return [value]() {
        return value;
    };
}

RandFunction binomial_distribution(
    int n_experiments, double success_probability
) {
    std::random_device rd;
    std::mt19937 generator(rd());
    std::binomial_distribution<> distribution(
        n_experiments, success_probability
    );

    return std::bind(distribution, generator);
}

RandFunction ranged_binomial_distribution(
    int min_value, int n_experiments, double success_probability
) {
    auto random_func = binomial_distribution(
        n_experiments - min_value, success_probability
    );

    return [min_value, random_func](){
        auto value = random_func() + min_value;
        return value;
    };
}

}
