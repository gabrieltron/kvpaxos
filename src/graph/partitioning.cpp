#include "partitioning.h"


namespace model {


// used during refennel
static std::unordered_map<int, std::pair<int, int>> previous_info;  // maps vertice to
                                                                    // prev part and weight
static std::vector<std::pair<std::unordered_set<int>, int>> old_partition;


std::vector<int> multilevel_cut(
    const Graph<int>& graph, int n_partitions, CutMethod cut_method)
    {

    auto& vertex = graph.vertex();
    int n_vertice = vertex.size();
    int n_edges = n_vertice * (n_vertice - 1);
    int n_constrains = 1;

    auto vertice_weight = std::vector<int>();
    for (auto i = 0; i < vertex.size(); i++) {
        vertice_weight.push_back(vertex.at(i));
    }

    auto x_edges = std::vector<int>();
    auto edges = std::vector<int>();
    auto edges_weight = std::vector<int>();

    x_edges.push_back(0);
    for (auto vertice = 0; vertice < vertex.size(); vertice++) {
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
    auto vertex_partitions = std::vector<int>(n_vertice, 0);
    if (cut_method == METIS) {
        METIS_PartGraphKway(
            &n_vertice, &n_constrains, x_edges.data(), edges.data(),
            vertice_weight.data(), NULL, edges_weight.data(), &n_partitions, NULL,
            NULL, options, &objval, vertex_partitions.data()
        );
    } else {
        double imbalance = 0.2;  // equal to METIS default imbalance
        kaffpa(
            &n_vertice, vertice_weight.data(), x_edges.data(),
            edges_weight.data(), edges.data(), &n_partitions,
            &imbalance, true, -1, FAST, &objval,
            vertex_partitions.data()
        );
    }

    return vertex_partitions;
}

int fennel_inter_cost(
    const tbb::concurrent_unordered_map<int, int>& edges,
    const std::unordered_set<int>& vertex_in_partition
) {
    auto cost = 0;
    for (auto& kv : edges) {
        auto vertice = kv.first;
        auto weight = kv.second;
        if (vertex_in_partition.find(vertice) != vertex_in_partition.end()) {
            cost += weight;
        }
    }
    return cost;
}

int fennel_vertice_partition(
    const Graph<int>& graph, int vertice,
    const std::vector<std::pair<std::unordered_set<int>, int>>& partitions,
    double gamma
) {
    double biggest_score = -DBL_MAX;
    auto id = 0;
    auto designated_partition = 0;
    for (auto& partition : partitions) {
        auto& partition_weight = partition.second;
        auto& edges = graph.vertice_edges(vertice);

        auto inter_cost = fennel_inter_cost(edges, partition.first);
        auto intra_cost =
            (std::pow(partition_weight + graph.vertice_weight(vertice), gamma));
        intra_cost -= std::pow(partition_weight, gamma);
        intra_cost *= gamma;
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
    auto partitions = std::vector<std::pair<std::unordered_set<int>, int>>();
    for (auto i = 0; i < n_partitions; i++) {
        // vertices there and total weight
        partitions.emplace_back(std::unordered_set<int>(), 0);
    }

    const auto edges_weight = graph.total_edges_weight();
    const auto vertex_weight = graph.total_vertex_weight();
    const auto gamma = 3 / 2.0;
    const auto alpha =
        edges_weight * std::pow(partitions.size(), (gamma - 1)) / std::pow(graph.total_vertex_weight(), gamma);

    auto final_partitioning = std::vector<int>();
    for (auto vertice = 0; vertice < graph.vertex().size(); vertice++) {
        auto partition = fennel_vertice_partition(graph, vertice, partitions, gamma);
        partitions[partition].first.insert(vertice);
        partitions[partition].second += graph.vertice_weight(vertice);
        final_partitioning.emplace_back(partition);
    }

    return final_partitioning;
}

std::vector<int> refennel_cut(const Graph<int>& graph, int n_partitions) {
    if (old_partition.empty()) {
        for (auto i = 0; i < n_partitions; i++) {
            old_partition.emplace_back(std::unordered_set<int>(), 0);
        }
    }

    const auto edges_weight = graph.total_edges_weight();
    const auto vertex_weight = graph.total_vertex_weight();
    const auto gamma = 3 / 2.0;
    const auto alpha =
        edges_weight * std::pow(old_partition.size(), (gamma - 1)) / std::pow(graph.total_vertex_weight(), gamma);

    auto final_partitioning = std::vector<int>();
    for (auto vertice = 0; vertice < graph.vertex().size(); vertice++) {
        auto new_partition = fennel_vertice_partition(
            graph, vertice, old_partition, gamma
        );

        if (previous_info.find(vertice) != previous_info.end()) {
            auto old_partition_id = previous_info[vertice].first;
            auto previous_weight = previous_info[vertice].second;
            old_partition[old_partition_id].first.erase(vertice);
            old_partition[old_partition_id].second -= previous_weight;
        }

        auto weight = graph.vertice_weight(vertice);
        old_partition[new_partition].first.insert(vertice);
        old_partition[new_partition].second += weight;
        previous_info[vertice] = std::make_pair(new_partition, weight);

        final_partitioning.push_back(new_partition);
    }

    return final_partitioning;

}

}
