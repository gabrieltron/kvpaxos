#ifndef MODEL_PARTITIONING_H
#define MODEL_PARTITIONING_H


#include <algorithm>
#include <float.h>
#include <fstream>
#include <kaHIP_interface.h>
#include <math.h>
#include <metis.h>
#include <string>
#include <tbb/concurrent_unordered_map.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "graph.hpp"
#include "scheduler/partition.hpp"


namespace model {

enum CutMethod {METIS, KAHIP, FENNEL, REFENNEL, REFENNEL2, ROUND_ROBIN};
const std::unordered_map<std::string, CutMethod> string_to_cut_method({
    {"METIS", METIS},
    {"KAHIP", KAHIP},
    {"FENNEL", FENNEL},
    {"REFENNEL", REFENNEL},
    {"REFENNEL2", REFENNEL2},
    {"ROUND_ROBIN", ROUND_ROBIN}
});

std::vector<int> cut_graph (
    const Graph<int>& graph,
    std::unordered_map<int, int>& vertice_to_partition,
    std::unordered_map<int, int>& weight_per_partition,
    CutMethod method
);

std::vector<int> multilevel_cut
    (const Graph<int>& graph, int n_partitions, CutMethod cut_method);
std::vector<int> fennel_cut(const Graph<int>& graph, int n_partitions);
std::vector<int> refennel_cut(
    const Graph<int>& graph,
    std::unordered_map<int, int>& vertice_to_partition,
    std::unordered_map<int, int>& size_per_partition,
    CutMethod method
);

int fennel_inter_cost(
    const tbb::concurrent_unordered_map<int, int>& edges,
    const std::unordered_set<int>& vertex_in_partition
);
int fennel_vertice_partition(
    int vertice,
    double alpha,
    double gamma,
    int max_partition_size,
    const Graph<int>& graph,
    const std::unordered_map<int, int>& vertice_to_partition,
    const std::unordered_map<int, int>& weight_per_partition
);

}

#endif
