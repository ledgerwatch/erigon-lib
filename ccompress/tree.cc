#include "tree.h"

namespace __tree {
    struct min_heap_compare {
        bool operator()(std::unique_ptr<huff_node> &a, std::unique_ptr<huff_node> &b) {
            return a->weight > b->weight;
        }
    };

    std::unique_ptr<huff_node> build_tree(std::vector<std::unique_ptr<huff_node>> v) {

        std::make_heap(v.begin(), v.end(), min_heap_compare());

        int weight;
        int value = -1;

        while (v.size() > 1) {

            std::pop_heap(v.begin(), v.end(), min_heap_compare());
            std::unique_ptr<huff_node> first(std::move(v.back()));
            v.pop_back();

            std::pop_heap(v.begin(), v.end(), min_heap_compare());
            std::unique_ptr<huff_node> second(std::move(v.back()));
            v.pop_back();

            weight = first->weight + second->weight;

            std::unique_ptr<huff_node> combined(
                new huff_node(value, weight, std::move(first), std::move(second)));

            v.push_back(std::move(combined));
            std::push_heap(v.begin(), v.end(), min_heap_compare());
        }

        return std::move(v[0]);
    }

    void dfs(std::unique_ptr<huff_node> &node, int *bit_length, std::vector<int> *bit_lengths) {
        if (node->left_child != nullptr) {
            (*bit_length)++;
            dfs(node->left_child, bit_length, bit_lengths);
        }

        if (node->right_child != nullptr) {
            (*bit_length)++;
            dfs(node->right_child, bit_length, bit_lengths);
        }

        if (node->value != -1) {
            // if ((*bit_length) > 16)
            //     std::cout << "GOT HERE: " << (*bit_length) << "\n";
            // //     *bit_length = 16;
            // assert((*bit_length) <= 16);
            bit_lengths->at(node->value) = (*bit_length);
        }

        (*bit_length)--;
    }
}
