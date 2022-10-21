#ifndef R_TRIE
#define R_TRIE

#ifdef __cplusplus

#include "00_avl_tree.h"

// for tests
#include <cassert>
#include <iostream>
#include <vector>

class Trie {
private:
    avl_tree *map;
    int nodes_created;

private:
public:
    Trie(/* args */);
    ~Trie();
    void print();

    int insert(unsigned char *src, int size);
    avl_tree *get_map();
};

#else
typedef struct Trie Trie;
#endif // __cplusplus

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

#if defined(__STDC__) || defined(__cplusplus)
extern Trie *NewTrie();
extern void CloseTrie(Trie *t);
extern int InsertPrefix(Trie *t, unsigned char *src, int size);
#else
extern Trie *NewTrie();
extern void CloseTrie(Trie *t);
extern int InsertPrefix(Trie *t, unsigned char *src, int size);
#endif

#ifdef __cplusplus
}
#endif // __cplusplus

#endif