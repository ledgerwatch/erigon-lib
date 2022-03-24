#ifndef _CCOMPRESS_H
#define _CCOMPRESS_H 1

#include <inttypes.h>

#define LARGE_INPUT_SIZE 1024

#define NOT_COMPRESSED 0
#define COMPRESSED 1
#define SHARED 2
#define SHARED_COMPRESSED 3
#define ALL_SAME 4
#define ONE_BYTE 8

/* --------------- shared (encoding/decoding) --------------- */
typedef unsigned char byte;

// a typical binary node
typedef struct huff_node {
    // encoding: number of times this byte appears in overall input
    // decoding: always 0
    int weight;
    // value >= 0 && value < 256 - is a leaf node
    // value == -1 - is intermediate node
    int16_t value;
    struct huff_node *left_child;
    struct huff_node *right_child;
} huff_node;

// tree topology, used to construct a tree when decoding
// saved to compressed file
typedef struct topology {
    byte data[256 * 4]; // max size 320 bytes
    int data_idx;       // points to the byte in data
    int bit_idx;        // points to the bit in byte, 7 to 0 from right to left
} topology;

int MAX_WORD;        // size of the largest passible buffer
int NUM_WORDS;       // total number of words
int MAX_WORD_DECODE; // size of the largest passible buffer, to allocate memory when decoding

/* --------------- encoding --------------- */
// in encodeing function that start/end with "shared" means they share the same topology
// has nothing to do with "shared (encoding/decoding)" above

int TOTAL_BITS;

int UNCOMPRESSED_BYTES_SIZE;

int MIN_BIT_LEN;

int SHARED_MIN_BIT_LEN;
int SHARED_TOTAL_BITS;
int SHARED_SMALL_WORDS;

void reset_encode_vars_shared();
void count_freq_shared(byte *src, int size);
topology *create_new_codes_shared();
int huff_encode_shared(byte *src, byte *dst, int size);

int __compress(byte *src, byte *dst, int size);
int __compress_shared(byte *src, byte *dst, int size);

/* --------------- decoding --------------- */

void reset_decode_vars_shared();
huff_node *tree_from_topo_shared(byte *topo, int size);
void dfs_decode_shared(huff_node *node, int8_t *bit_length, uint16_t bits);
int huff_decode2(byte *src, byte *dst, int size, int min_bits, int is_shared);

void reset_decode_vars();
huff_node *tree_from_topo(byte *topo, int size);
void dfs_decode(huff_node *node, int8_t *bit_length, uint16_t bits);

int __decompress(byte *src, byte *dst, int compressed_size);
int __decompress_shared(byte *src, byte *dst, int compressed_size);

#endif // _CCOMPRESS_H