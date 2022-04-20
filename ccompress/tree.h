#ifndef CCOMPRESS_TREE_
#define CCOMPRESS_TREE_

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include <vector>

namespace __tree {

    struct huff_node {

        int value;
        int weight;
        std::unique_ptr<huff_node> left_child;
        std::unique_ptr<huff_node> right_child;

        huff_node(int v, int w) : value(v), weight(w), left_child(nullptr), right_child(nullptr) {}
        huff_node(int v, int w, std::unique_ptr<huff_node> l, std::unique_ptr<huff_node> r)
            : value(v), weight(w) {
            this->left_child = std::move(l);
            this->right_child = std::move(r);
        }

        ~huff_node() {}
    };

    std::unique_ptr<huff_node> build_tree(std::vector<std::unique_ptr<huff_node>> v);

    void dfs(std::unique_ptr<huff_node> &node, int *bit_length, std::vector<int> *bit_lengths);

}

#endif // CCOMPRESS_TREE_