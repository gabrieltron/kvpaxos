#include "partitioning.h"


namespace model {

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

}
