#include "01_trie.h"

#define FLAG_EOW 256
#define COUNT_DOWN 3
#define MAX_WORD_SIZE (1 << 8) // max possible word
#define LIMIT_NODES (1 << 24)

// ----------- externs -----------
Trie *NewTrie() {
    return new Trie();
}
void CloseTrie(Trie *t) {
    delete t;
}

int InsertPrefix(Trie *t, unsigned char *src, int size) {
    return t->insert(src, size);
}

// ----------- Trie -----------

Trie::Trie() {
    map = new avl_tree();
    nodes_created = 0;
}

Trie::~Trie() {

    delete map;
    // std::cout << "nodes_created: " << nodes_created << "\n";
    // std::cout << "size projected: " << nodes_created * sizeof(bst_node) << "\n";
}

avl_tree *Trie::get_map() {
    return map;
}

int Trie::insert(unsigned char *src, int size) {

    if (nodes_created >= LIMIT_NODES) return -1;

    avl_tree *_map = this->map;
    bst_node *node;

    for (int i = 0; i < size; i++) {
        node = _map->find(src[i]);
        if (node == nullptr) {
            node = new bst_node(src[i]);

            nodes_created++;
            _map->insert(node);
        }
        if (node->num_ref < (UINT16_MAX)) {
            node->num_ref++;
        }
        if (nodes_created >= LIMIT_NODES) return -1;
        _map = node->map;
        assert(_map != nullptr);
    }

    return 1;
}

void Trie::print() {
    std::vector<uint8_t> v;
    v.reserve(256);
    // root->print(&v);
}

// ----------- TESTS -----------

// struct avl_node {
//     avl_node *left;
//     avl_node *right;
//     avl_node *childs;
//     uint8_t key;
// };

// #include "rand.h"
// int main() {

//     // Rand rng;
//     // int N = 10;
//     // Trie *trie = NewTrie();
//     // std::vector<uint8_t> src;
//     // int c = 20;
//     // for (int q = 0; q < N; q++) {
//     //     // src = rng.rand_bytes(4, 6);
//     //     src = std::vector<uint8_t>(c, 0);
//     //     // src.push_back(1);
//     //     int src_size = src.size();
//     //     c--;
//     //     std::cout << "adding:\n";
//     //     for (int p = 0; p < src_size; p++)
//     //         std::cout << (int)src.at(p) << " ";
//     //     std::cout << "\n";

//     //     InsertWord(trie, reinterpret_cast<unsigned char *>(&src[0]), src_size);
//     // }

//     // CloseTrie(trie);
// }