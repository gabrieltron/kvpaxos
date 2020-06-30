#ifndef MODEL_PARTITIONING_H
#define MODEL_PARTITIONING_H

#include <algorithm>
#include <float.h>
#include <fstream>
#include <kaHIP_interface.h>
#include <math.h>
#include <metis.h>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "graph.hpp"


namespace model {

enum CutMethod {METIS, KAHIP, FENNEL, REFENNEL};
const std::unordered_map<std::string, CutMethod> string_to_cut_method({
    {"METIS", METIS},
    {"KAHIP", KAHIP},
    {"FENNEL", FENNEL},
    {"REFENNEL", REFENNEL}
});


std::vector<int> multilevel_cut
    (const Graph<int>& graph, int n_partitions, CutMethod cut_method);

/*
workload::PartitionScheme fennel_cut(Graph& graph, int n_partitions);
workload::PartitionScheme refennel_cut(
    Graph& graph, workload::PartitionScheme& old_partition
);
int fennel_inter_cost(
    const std::unordered_map<int, int>& edges,
    workload::Partition& partition
);
int fennel_vertice_partition(
    Graph& graph,
    int vertice,
    std::vector<workload::Partition>& partitions,
    double gamma
);
int biggest_value_index(std::vector<double>& partitions_score);

*/
}

#endif
