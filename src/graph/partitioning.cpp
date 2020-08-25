#include "partitioning.h"


namespace model {

// This is a workaround to reuse the same method for both fennel
// and refennel when calculating the vertex's partition.
struct dummy_partition {
    int id_;
    int weight_ = 0;

    dummy_partition(int id): id_{id}{}

    int id() const {return id_;}
    int weight() const {return weight_;}
};


std::vector<int> cut_graph (
    const Graph<int>& graph,
    std::unordered_map<int, kvpaxos::Partition<int>*>& partitions,
    CutMethod method,
    const std::unordered_map<int, kvpaxos::Partition<int>*>& old_data_to_partition =
        std::unordered_map<int, kvpaxos::Partition<int>*>(),
    bool firt_repartition = true
) {
    if (method == METIS) {
        return multilevel_cut(graph, partitions.size(), method);
    } else if (method == KAHIP) {
        return multilevel_cut(graph, partitions.size(), method);
    } else if (method == FENNEL) {
        return fennel_cut(graph, partitions.size());
    } else {
        return refennel_cut(graph, old_data_to_partition, partitions, firt_repartition);
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

template <typename T>
std::unordered_map<int, int> sum_neighbours(
    const std::unordered_map<int, int>& edges,
    const std::unordered_map<int, T*>& vertice_to_partition
) {
    std::unordered_map<int, int> partition_sums;
    for (auto& kv : edges) {
        auto vertice = kv.first;
        auto weight = kv.second;
        if (vertice_to_partition.find(vertice) == vertice_to_partition.end()) {
            continue;
        }

        auto partition = vertice_to_partition.at(vertice)->id();
        if (partition_sums.find(partition) == partition_sums.end()) {
            partition_sums[partition] = 0;
        }
        partition_sums[partition] += weight;

    }
    return partition_sums;
}

template <typename T>
int fennel_vertice_partition(
    const Graph<int>& graph,
    int vertice,
    const std::unordered_map<int, T*>& partitions,
    const std::unordered_map<int, T*>& vertice_to_partition,
    int max_partition_size,
    double alpha,
    double gamma
) {
    double biggest_score = -DBL_MAX;
    auto designated_partition = -1;
    auto neighbours_in_partition = std::move(
        sum_neighbours<T>(graph.vertice_edges(vertice), vertice_to_partition)
    );
    for (auto i = 0; i < partitions.size(); i++) {
        auto partition_weight = partitions.at(i)->weight();
        if (max_partition_size) {
            if (partition_weight + graph.vertice_weight(vertice) > max_partition_size) {
                continue;
            }
        }

        int inter_cost;
        if (neighbours_in_partition.find(i) == neighbours_in_partition.end()) {
            inter_cost = 0;
        } else {
            inter_cost = neighbours_in_partition[i];
        }

        double intra_cost =
            (std::pow(partition_weight + graph.vertice_weight(vertice), gamma));
        intra_cost -= std::pow(partition_weight, gamma);
        intra_cost *= alpha;

        double score = inter_cost - intra_cost;
        if (score > biggest_score) {
            biggest_score = score;
            designated_partition = i;
        }
    }

    return designated_partition;
}

std::vector<int> fennel_cut(const Graph<int>& graph, int n_partitions) {
    std::unordered_map<int, dummy_partition*> partitions;
    for (auto i = 0; i < n_partitions; i++) {
        auto* partition = new dummy_partition(i);
        partitions.emplace(i, partition);
    }

    const auto edges_weight = graph.total_edges_weight();
    const auto vertex_weight = graph.total_vertex_weight();
    const auto gamma = 3 / 2.0;
    const double alpha =
        edges_weight * std::pow(n_partitions, (gamma - 1)) / std::pow(graph.total_vertex_weight(), gamma);

    std::unordered_map<int, dummy_partition*> vertice_to_partition;
    std::vector<int> final_partitioning;
    auto sorted_vertex = std::move(graph.sorted_vertex());
    auto partition_max_size = 1.2 * graph.total_vertex_weight() / n_partitions;
    for (auto& vertice : sorted_vertex) {
        auto partition = fennel_vertice_partition<dummy_partition>(
            graph, vertice, partitions, vertice_to_partition,
            partition_max_size, alpha, gamma
        );
        if (partition == -1) {
            partition_max_size = 0;  // remove partition limit
            partition = fennel_vertice_partition<dummy_partition>(
                    graph, vertice, partitions, vertice_to_partition,
                    partition_max_size, alpha, gamma
            );
        }
        partitions[partition]->weight_ += graph.vertice_weight(vertice);
        vertice_to_partition[vertice] = partitions[partition];
        final_partitioning.emplace_back(partition);
    }

    for (auto& kv: partitions) {
        delete kv.second;
    }

    return final_partitioning;
}

std::vector<int> refennel_cut(
    const Graph<int>& graph,
    const std::unordered_map<int, kvpaxos::Partition<int>*>& old_data_to_partition,
    std::unordered_map<int, kvpaxos::Partition<int>*>& partitions,
    bool first_repartition
) {
    if (first_repartition) {
        auto new_mapping = fennel_cut(graph, partitions.size());
        for (auto data = 0; data < new_mapping.size(); data++) {
            auto* old_partition = old_data_to_partition.at(data);
            old_partition->remove_data(data);

            auto* new_partition = partitions.at(new_mapping[data]);
            new_partition->insert_data(data, graph.vertice_weight(data));
        }
        return new_mapping;
    }

    const auto n_partitions = partitions.size();

    const auto edges_weight = graph.total_edges_weight();
    const auto vertex_weight = graph.total_vertex_weight();
    const auto gamma = 3 / 2.0;
    const auto alpha =
        edges_weight * std::pow(n_partitions, (gamma - 1)) / std::pow(graph.total_vertex_weight(), gamma);

    auto final_partitioning = std::vector<int>();
    auto sorted_vertex = std::move(graph.sorted_vertex());
    auto partition_max_size = 1.2 * graph.total_vertex_weight() / n_partitions;
    for (auto& vertice : sorted_vertex) {
        auto* old_partition = old_data_to_partition.at(vertice);
        old_partition->remove_data(vertice);

        auto new_partition = fennel_vertice_partition<kvpaxos::Partition<int>>(
            graph, vertice, partitions, old_data_to_partition,
            partition_max_size, alpha, gamma
        );
        if (new_partition == -1) {
            partition_max_size = 0;  // remove partition limit
            new_partition = fennel_vertice_partition<kvpaxos::Partition<int>>(
                    graph, vertice, partitions, old_data_to_partition,
                    partition_max_size, alpha, gamma
                );
        }

        partitions.at(new_partition)->insert_data(vertice, graph.vertice_weight(vertice));

        final_partitioning.push_back(new_partition);
    }

    return final_partitioning;

}

}
