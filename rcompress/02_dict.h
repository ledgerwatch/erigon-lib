#ifndef R_DICT
#define R_DICT

#include "01_trie.h"

#ifdef __cplusplus

#include "04_encoding_assets.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <iostream>
#include <list>
#include <map>
#include <tuple>
#include <vector>

typedef std::tuple<int, int, uint8_t> priority_tuple;

class Dict {
private:
public:
    std::array<uint32_t, 134217728> filter;
    std::vector<std::vector<uint8_t>> prefixes;
    std::vector<std::vector<uint8_t>> final_dict;

    std::vector<priority_tuple> to_prioritise;      // final dict shape
    std::vector<std::tuple<int, int>> prefix_quads; // == 4 prefix size
    std::vector<std::tuple<int, int>> prefix_large; // >= 5 prefix size
    std::vector<int> remapped;
    std::vector<uint8_t> max_match; // maximum match len so far for a given prefix_id
    std::vector<uint8_t> min_match; // maximum match len so far for a given prefix_id
public:
    Dict(/* args */);
    ~Dict();

public:
    int precompress(Trie *t, unsigned char *word, int w_size, int *precompressed);
    int count_matches(unsigned char *data, int data_size, int *sizes, int sizes_size, int *preCompressed, int preCompressed_size);
    int reduce_dict();
};

#else
typedef struct Dict Dict;
#endif // __cplusplus

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

#if defined(__STDC__) || defined(__cplusplus)
extern Dict *BuildStaticDict(Trie *t, int *created);
extern void DeleteDict(Dict *dict);
extern int Precompress(Dict *dict, Trie *t, unsigned char *word, int w_size, int *precompressed);
extern int CountMatches(Dict *dict, unsigned char *data, int data_size, int *sizes, int sizes_size, int *preCompressed, int preCompressed_size);
extern int ReduceDict(Dict *dict);
#else
extern Dict *BuildStaticDict(Trie *t, int *created);
extern void DeleteDict(Dict *dict);
extern int Precompress(Dict *dict, Trie *t, unsigned char *word, int w_size, int *precompressed);
extern int CountMatches(Dict *dict, unsigned char *data, int data_size, int *sizes, int sizes_size, int *preCompressed, int preCompressed_size);
extern int ReduceDict(Dict *dict);
#endif

#ifdef __cplusplus
}
#endif // __cplusplus

#endif