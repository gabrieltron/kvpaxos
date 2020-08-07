#include "partitioning.h"


namespace model {


// used during refennel
static std::unordered_map<int, int> refennel_vertice_to_partition;  // maps vertice to
                                                                    // prev part and weight
static std::vector<int> refennel_partitions_size;
static std::unordered_map<int, int> old_vertice_weight;


std::vector<int> cut_graph (
    const Graph<int>& graph, int n_paritions, CutMethod method
) {
    if (method == METIS) {
        return multilevel_cut(graph, n_paritions, method);
    } else if (method == KAHIP) {
        return multilevel_cut(graph, n_paritions, method);
    } else if (method == FENNEL) {
        return fennel_cut(graph, n_paritions);
    } else {
        return refennel_cut(graph, n_paritions);
    }
}

std::vector<int> multilevel_cut(
    const Graph<int>& graph, int n_partitions, CutMethod cut_method
)
{
    auto& vertex = graph.vertex();
    auto sorted_vertex = std::move(graph.sorted_vertex());
    int n_constrains = 1;

    auto vertice_weight = std::vector<int>();
    for (auto& vertice : sorted_vertex) {
        vertice_weight.push_back(vertex.at(vertice));
    }

    auto x_edges = std::vector<int>();
    auto edges = std::vector<int>();
    auto edges_weight = std::vector<int>();

    x_edges.push_back(0);
    for (auto& vertice : sorted_vertex) {
        auto last_edge_index = x_edges.back();
        auto n_neighbours = graph.vertice_edges(vertice).size();
        x_edges.push_back(last_edge_index + n_neighbours);

        for (auto& vk: graph.vertice_edges(vertice)) {
            auto neighbour = vk.first;
            auto weight = vk.second;
            edges.push_back(neighbour);
            edges_weight.push_back(weight);
        }
    }

    int options[METIS_NOPTIONS];
    METIS_SetDefaultOptions(options);
    options[METIS_OPTION_OBJTYPE] = METIS_OBJTYPE_CUT;
    options[METIS_OPTION_NUMBERING] = 0;
    options[METIS_OPTION_UFACTOR] = 200;

    int objval;
    int n_vertex = vertice_weight.size();
    auto vertex_partitions = std::vector<int>(n_vertex, 0);
    if (cut_method == METIS) {
        METIS_PartGraphKway(
            &n_vertex, &n_constrains, x_edges.data(), edges.data(),
            vertice_weight.data(), NULL, edges_weight.data(), &n_partitions, NULL,
            NULL, options, &objval, vertex_partitions.data()
        );
    } else {
        double imbalance = 0.2;  // equal to METIS default imbalance
        kaffpa(
            &n_vertex, vertice_weight.data(), x_edges.data(),
            edges_weight.data(), edges.data(), &n_partitions,
            &imbalance, true, -1, FAST, &objval,
            vertex_partitions.data()
        );
    }

    return vertex_partitions;
}

std::unordered_map<int, int> sum_neighbours(
    const tbb::concurrent_unordered_map<int, int>& edges,
    const std::unordered_map<int, int>& vertice_to_partition
) {
    std::unordered_map<int, int> partition_sums;
    for (auto& kv : edges) {
        auto vertice = kv.first;
        auto weight = kv.second;
        if (vertice_to_partition.find(vertice) == vertice_to_partition.end()) {
            continue;
        }

        auto partition = vertice_to_partition.at(vertice);
        if (partition_sums.find(partition) == partition_sums.end()) {
            partition_sums[partition] = 0;
        }
        partition_sums[partition] += weight;

    }
    return partition_sums;
}

int fennel_vertice_partition(
    const Graph<int>& graph,
    int vertice,
    const std::vector<int>& partitions_weight,
    const std::unordered_map<int, int>& vertice_to_partition,
    double alpha,
    double gamma
) {
    double biggest_score = -DBL_MAX;
    auto id = 0;
    auto designated_partition = 0;
    auto neighbours_in_partition = std::move(
        sum_neighbours(graph.vertice_edges(vertice), vertice_to_partition)
    );
    for (auto i = 0; i < partitions_weight.size(); i++) {
        auto& partition_weight = partitions_weight[i];

        int inter_cost;
        if (neighbours_in_partition.find(i) == neighbours_in_partition.end()) {
            inter_cost = 0;
        } else {
            inter_cost = neighbours_in_partition[i];
        }

        auto intra_cost =
            (std::pow(partition_weight + graph.vertice_weight(vertice), gamma));
        intra_cost -= std::pow(partition_weight, gamma);
        intra_cost *= alpha;

        auto score = inter_cost - intra_cost;
        if (score > biggest_score) {
            biggest_score = score;
            designated_partition = id;
        }
        id++;
    }

    return designated_partition;
}

std::vector<int> fennel_cut(const Graph<int>& graph, int n_partitions) {
    std::vector<int> partitions_weight;
    for (auto i = 0; i < n_partitions; i++) {
        // vertices there and total weight
        partitions_weight.emplace_back(0);
    }

    const auto edges_weight = graph.total_edges_weight();
    const auto vertex_weight = graph.total_vertex_weight();
    const auto gamma = 3 / 2.0;
    const auto alpha =
        edges_weight * std::pow(n_partitions, (gamma - 1)) / std::pow(graph.total_vertex_weight(), gamma);

    std::unordered_map<int, int> vertice_to_partition;
    std::vector<int> final_partitioning;
    auto& vertex = graph.vertex();
    auto sorted_vertex = std::move(graph.sorted_vertex());
    for (auto& vertice : sorted_vertex) {
        auto partition = fennel_vertice_partition(
            graph, vertice, partitions_weight, vertice_to_partition, alpha, gamma
        );
        partitions_weight[partition] += graph.vertice_weight(vertice);
        vertice_to_partition[vertice] = partition;
        final_partitioning.emplace_back(partition);
    }

    return final_partitioning;
}

std::vector<int> refennel_cut(const Graph<int>& graph, int n_partitions) {
    if (refennel_partitions_size.empty()) {
        for (auto i = 0; i < n_partitions; i++) {
            refennel_partitions_size.emplace_back(0);
        }
    }

    const auto edges_weight = graph.total_edges_weight();
    const auto vertex_weight = graph.total_vertex_weight();
    const auto gamma = 3 / 2.0;
    const auto alpha =
        edges_weight * std::pow(n_partitions, (gamma - 1)) / std::pow(graph.total_vertex_weight(), gamma);

    auto final_partitioning = std::vector<int>();
    auto& vertex = graph.vertex();
    auto sorted_vertex = std::move(graph.sorted_vertex());
    for (auto& vertice : sorted_vertex) {
        auto new_partition = fennel_vertice_partition(
            graph, vertice, refennel_partitions_size, refennel_vertice_to_partition,
            alpha, gamma
        );

        if (refennel_vertice_to_partition.find(vertice) != refennel_vertice_to_partition.end()) {
            auto old_partition_id = refennel_vertice_to_partition[vertice];
            auto old_weight = old_vertice_weight[vertice];
            refennel_partitions_size[old_partition_id] -= old_weight;
        }

        auto weight = graph.vertice_weight(vertice);
        refennel_vertice_to_partition[vertice] = new_partition;
        refennel_partitions_size[new_partition] += weight;
        old_vertice_weight[vertice] = weight;

        final_partitioning.push_back(new_partition);
    }

    return final_partitioning;

}

}
