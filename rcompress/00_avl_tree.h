#pragma once

#include <cassert>
#include <iostream>

// remove those when not using tests and/or prints
#include "rand.h"

#include <deque>
#include <string>
#include <tuple>
#include <vector>

class avl_tree;

struct bst_node {
    bst_node(uint8_t key);
    ~bst_node();

    bst_node *parent; // 8
    bst_node *left;   // 8
    bst_node *right;  // 8

    avl_tree *map; // 8

    uint32_t order_num; // 4
    uint16_t num_ref;   // 2
    int8_t height;      // 1
    uint8_t key;        // 1

    bool insert(bst_node *node);
    bst_node *find(uint8_t _key);
    bst_node *find_min();
    bst_node *next_larger();
};

class avl_tree {
private:
    bst_node *root;

public:
    avl_tree(/* args */);
    ~avl_tree();

private:
    void left_rotate(bst_node *node);
    void right_rotate(bst_node *node);
    void rebalance(bst_node *node);

public:
    // bst_node *get(uint8_t _key);
    // void set(bst_node *node);
    void insert(bst_node *node);
    bst_node *find(uint8_t _key);

    bst_node *get_root();
    void print();
};
