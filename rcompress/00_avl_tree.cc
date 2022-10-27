#include "00_avl_tree.h"

bst_node::bst_node(uint8_t _key) {
    height = -1;
    key = _key;
    num_ref = 0;
    parent = nullptr;
    left = nullptr;
    right = nullptr;
    map = new avl_tree();
}

bst_node::~bst_node() {
    delete map;
    delete left;
    delete right;
}

int8_t _height(bst_node *node) {
    return node == nullptr ? -1 : node->height;
}

void update_height(bst_node *node) {
    assert(node != nullptr);
    int8_t l_height = _height(node->left);
    int8_t r_height = _height(node->right);

    node->height = l_height > r_height ? l_height + 1 : r_height + 1;
}

bool bst_node::insert(bst_node *node) {
    assert(node != nullptr);

    if (node->key == this->key) return false;

    if (node->key < this->key) {
        if (this->left == nullptr) {
            node->parent = this;
            this->left = node;
            return true;
        } else {
            return this->left->insert(node);
        }
    } else {
        if (this->right == nullptr) {
            node->parent = this;
            this->right = node;
            return true;
        } else {
            return this->right->insert(node);
        }
    }
}

bool insert_iterative(bst_node *root, bst_node *to_insert) {

    bst_node *x = root;
    bst_node *y = nullptr;

    while (x != nullptr) {
        y = x;
        if (to_insert->key == x->key)
            return false;
        else if (to_insert->key < x->key)
            x = x->left;
        else
            x = x->right;
    }

    if (to_insert->key == y->key)
        return false;
    else if (to_insert->key < y->key)
        y->left = to_insert;
    else
        y->right = to_insert;

    return true;
}

bst_node *bst_node::find(uint8_t _key) {

    if (_key == this->key)
        return this;
    else if (_key < this->key) {
        if (this->left == nullptr)
            return nullptr;
        else
            return this->left->find(_key);
    } else {
        if (this->right == nullptr)
            return nullptr;
        else
            return this->right->find(_key);
    }
}

bst_node *find_iterative(bst_node *root, uint8_t _key) {
    while (root != nullptr) {
        if (_key > root->key) {
            root = root->right;
        } else if (_key < root->key) {
            root = root->left;
        } else
            return root;
    }
    return nullptr;
}

bst_node *bst_node::find_min() {

    bst_node *current = this;
    while (current->left != nullptr)
        current = current->left;

    return current;
}

bst_node *bst_node::next_larger() {
    if (this->right != nullptr)
        return this->right->find_min();

    bst_node *current = this;
    while (current->parent != nullptr && current == current->parent->right)
        current = current->parent;
    return current->parent;
}

// ------------------- AVL ---------------------

avl_tree::avl_tree() {
    root = nullptr;
}
avl_tree::~avl_tree() {

    delete root;
}

void avl_tree::left_rotate(bst_node *node) {

    assert(node->right != nullptr);
    bst_node *right = node->right;
    assert(right->parent != nullptr);

    right->parent = node->parent;
    if (right->parent == nullptr) {
        root = right;
    } else {
        assert(right->parent != nullptr);
        if (right->parent->left == node) {
            right->parent->left = right;
            assert(right->parent->left != nullptr);
        } else if (right->parent->right == node) {
            right->parent->right = right;
            assert(right->parent->right != nullptr);
        }
    }

    node->right = right->left;
    if (node->right != nullptr)
        node->right->parent = node;

    right->left = node;
    node->parent = right;
    update_height(node);
    update_height(right);
}

void avl_tree::right_rotate(bst_node *node) {

    assert(node->left != nullptr);
    bst_node *left = node->left;
    assert(left->parent != nullptr);

    left->parent = node->parent;
    if (left->parent == nullptr) {
        root = left;
    } else {
        assert(left->parent != nullptr);
        if (left->parent->left == node) {
            left->parent->left = left;
            assert(left->parent->left != nullptr);
        } else if (left->parent->right == node) {
            left->parent->right = left;
            assert(left->parent->right != nullptr);
        }
    }
    node->left = left->right;
    if (node->left != nullptr)
        node->left->parent = node;

    left->right = node;
    node->parent = left;
    update_height(node);
    update_height(left);
}

void avl_tree::rebalance(bst_node *node) {

    while (node != nullptr) {
        update_height(node);

        if (_height(node->left) >= 2 + _height(node->right)) {
            if (_height(node->left->left) >= _height(node->left->right))
                right_rotate(node);
            else {
                left_rotate(node->left);
                right_rotate(node);
            }
        } else if (_height(node->right) >= 2 + _height(node->left)) {
            if (_height(node->right->right) >= _height(node->right->left))
                left_rotate(node);
            else {
                right_rotate(node->right);
                left_rotate(node);
            }
        }
        node = node->parent;
    }
}

int nodes_created = 0;
void avl_tree::insert(bst_node *node) {

    if (this->root == nullptr) {
        this->root = node;
        nodes_created++;
        return;
    }

    // bool inserted = root->insert(node);
    bool inserted = insert_iterative(root, node);
    if (inserted) {

        nodes_created++;

        rebalance(node);

    } else {
        delete node;
    }
}

bst_node *avl_tree::find(uint8_t _key) {

    if (this->root == nullptr)
        return nullptr;

    // return root->find(_key);
    return find_iterative(this->root, _key);
}

bst_node *avl_tree::get_root() {
    return this->root;
}

void avl_tree::print() {

    std::deque<std::tuple<bst_node *, int>> stack;
    std::vector<std::tuple<int, uint16_t>> out;
    out.reserve(256);

    int level = 0;
    if (root != nullptr) {
        stack.push_back(std::make_tuple(root, level));
        out.push_back(std::make_tuple(level, root->key));
        level++;
    }

    bst_node *node;
    int _lvl;
    int max_level = 0;
    while (stack.size() > 0) {
        std::tie(node, _lvl) = stack.front();
        stack.pop_front();

        if (_lvl > max_level) max_level = _lvl;

        if (node->left != nullptr) {
            stack.push_back(std::make_tuple(node->left, _lvl + 1));
            out.push_back(std::make_tuple(_lvl + 1, node->left->key));
        } else {
            out.push_back(std::make_tuple(_lvl + 1, 256));
        }

        if (node->right != nullptr) {
            stack.push_back(std::make_tuple(node->right, _lvl + 1));
            out.push_back(std::make_tuple(_lvl + 1, node->right->key));
        } else {
            out.push_back(std::make_tuple(_lvl + 1, 256));
        }
    }

    std::string _out = "";
    int _lvl2;
    uint16_t _k, _k2;
    for (int i = 0; i < (int)out.size();) {
        std::tie(_lvl, _k) = out.at(i);

        if (_k == 256) {
            i++;
            continue;
        }
        std::cout << _lvl << ": " << std::to_string(_k) << "\n";
        for (int q = 0; q < max_level * 2; q++)
            _out += " ";
        _out += std::to_string(_k);

        int j = i + 1;
        for (; j < (int)out.size(); j++) {
            std::tie(_lvl2, _k2) = out.at(j);
            if (_lvl2 == _lvl) {
                for (int q = 0; q < (max_level + 1) * 2; q++)
                    _out += " ";
                if (_k2 < 256)
                    _out += std::to_string(_k2);
                else
                    _out += " ";
            } else {
                break;
            }
        }

        _out += "\n\n";
        max_level--;

        i = j;
    }

    std::cout << _out << "\n";
}

// ------------------- TESTS ---------------------

class trie {
private:
    avl_tree *map;

public:
    trie(/* args */);
    ~trie();

    void insert(std::vector<uint8_t> *word, int w_size);
    std::vector<std::tuple<uint8_t, uint8_t>> extract_next();
    avl_tree *get_map();
};

avl_tree *trie::get_map() {
    return map;
}

// void trie_node::print(std::vector<uint8_t> *v) {

//     assert(childs.size() <= 256);

//     uint8_t any = 0;
//     for (auto it = childs.begin(); it != childs.end(); ++it) {
//         any = 1;
//         v->push_back(it->first);
//         childs[it->first]->print(v);
//     }

//     // for (int i = 0; i < 256; i++) {
//     //     if (childs[i] != nullptr) {
//     //         any = true;
//     //         v->push_back(i);
//     //         childs[i]->print(v);
//     //     }
//     // }

//     if (!any) {
//         for (int j = 0; j < (int)v->size(); j++)
//             std::cout << (int)v->at(j) << " ";
//         std::cout << "\n";
//     }

//     v->pop_back();
// }

// void recursive_print(bst_node *node, std::vector<uint8_t> *prefix, std::vector<uint8_t> *sizes, std::vector<uint8_t> *out) {

//     if (node == nullptr)
//         return;

//     recursive_print(node->left, prefix, sizes, out); // the lowest key in this map

//     prefix->push_back(node->key);
//     // std::cout << (int)(node->key) << " ";
//     // (*idx)++;

//     bst_node *root = node->map->get_root();
//     if (root != nullptr) {

//         recursive_print(root, prefix, sizes, out); // go to the lowest key of this node's map

//     } else {
//         // here we are reached the end/bottom

//         // std::cout << "\n";
//         int p_size = prefix->size();
//         sizes->push_back(p_size);
//         for (int i = 0; i < p_size; i++) {
//             // std::cout << (int)prefix->at(i) << " ";
//             out->push_back(prefix->at(i));
//         }
//         // std::cout << "\n----------------\n";

//         // *idx = 0;
//     }
//     prefix->pop_back();
//     // std::cout << "idx: " << *idx << "\n";
//     recursive_print(node->right, prefix, sizes, out);
// }

trie::trie(/* args */) {
    map = new avl_tree();
}

trie::~trie() {
    std::cout << "NODES_CREATED: " << nodes_created << "\n";
    std::cout << "AVG BYTES: " << nodes_created * sizeof(bst_node) << "\n";

    // std::vector<uint8_t> prefix;
    // prefix.reserve(256);
    // std::vector<uint8_t> out;
    // out.reserve((1 << 25));
    // std::vector<uint8_t> sizes;
    // sizes.reserve(N);
    // recursive_print(map->get_root(), &prefix, &sizes, &out);

    // int out_size = out.size();
    // std::cout << "out_size: " << out_size << "\n";

    delete map;
}

void trie::insert(std::vector<uint8_t> *word, int w_size) {

    avl_tree *_map = this->map;
    bst_node *node;

    for (int i = 0; i < w_size; i++) {
        node = _map->find(word->at(i));
        if (node == nullptr) {
            node = new bst_node(word->at(i));
            _map->insert(node);
        }

        _map = node->map;
        assert(_map != nullptr);
    }
}

// std::vector<std::tuple<uint8_t, uint8_t>> extract_next() {
// }

class lz_table {
private:
    /* data */
public:
    lz_table(/* args */);
    ~lz_table();
};

lz_table::lz_table(/* args */) {
}

lz_table::~lz_table() {
}

// #include "rand.h"

// #include <map>
// int main() {

//     std::cout << sizeof(bst_node) << "\n";

//     // Rand rng;

//     // int N = 30;

//     // trie *_trie = new trie();
//     // int total = 0;

//     // int max = 256;

//     // for (int q = 0; q < N; q++) {
//     //     int min = rng.rand_int_range(4, 10);
//     //     // auto src = rng.rand_bytes(min, max);
//     //     std::vector<uint8_t> src(rng.rand_int_range(min, max), 0);
//     //     int src_size = src.size();

//     //     for (int i = src_size - min; i < src_size; i++)
//     //         src[i] = rng.rand_byte(0, 6);

//     //     // for (int i = 0; i < src_size; i++)
//     //     //     std::cout << (int)src.at(i) << " ";
//     //     // std::cout << "\n";
//     //     // assert(src_size >= min && src_size <= max);
//     //     total += src_size;
//     //     _trie->insert(&src, src_size);
//     // }

//     // for (int q = 0; q < N; q++) {
//     //     int min = rng.rand_int_range(4, 10);
//     //     // auto src = rng.rand_bytes(min, max);
//     //     std::vector<uint8_t> src(rng.rand_int_range(min, max), 1);
//     //     int src_size = src.size();

//     //     for (int i = src_size - min; i < src_size; i++)
//     //         src[i] = rng.rand_byte(0, 6);

//     //     // for (int i = 0; i < src_size; i++)
//     //     //     std::cout << (int)src.at(i) << " ";
//     //     // std::cout << "\n";
//     //     // assert(src_size >= min && src_size <= max);
//     //     total += src_size;
//     //     _trie->insert(&src, src_size);
//     // }

//     // std::cout << "------------------------- DONE, total added: " << total << "\n";

//     // std::vector<uint8_t> prefix;
//     // prefix.reserve(256);
//     // std::vector<uint8_t> out;
//     // out.reserve((1 << 25));
//     // std::vector<uint8_t> sizes;
//     // sizes.reserve(N);
//     // recursive_print(_trie->get_map()->get_root(), &prefix, &sizes, &out);

//     // int out_size = out.size();
//     // int s_size = sizes.size();

//     // std::cout << "out_size: " << out_size << "\n";
//     // for (int i = 0; i < s_size; i++) {
//     //     std::cout << "size[" << i << "]: " << (int)sizes.at(i) << "\n";
//     // }
//     // std::cout << "\n";

//     // delete _trie;

//     // std::cout << "DELETE DONE\n";

//     // return 0;
// }