#pragma once

#include <algorithm>
#include <array>
#include <cassert>
#include <forward_list>
#include <iostream>
#include <tuple>
#include <vector>

#define R_MAX_ALPH_SIZE 284
#define R_FLAG_EOW 256
#define R_MAX_BIT_LEN 15
#define R_COPY_PREV R_MAX_BIT_LEN + 1
#define R_REPEAT_0_3 R_COPY_PREV + 1
#define R_REPEAT_0_11 R_REPEAT_0_3 + 1

#define R_MAX_PREFIXES 1064956
#define R_MAX_QUADS 4092

//      Extra               Extra               Extra

// Code Bits Length(s) Code Bits Lengths   Code Bits Length(s)

// ---- ---- ------     ---- ---- -------   ---- ---- -------

//  257   0     4       267   1   17,18     277   4   83-98

//  258   0     5       268   2   19-22     278   4   99-114

//  259   0     6       269   2   23-26     279   4   115-130

//  260   0     7       270   2   27-30     280   5   131-162

//  261   0     8       271   2   31-34     281   5   163-194

//  262   0     9       272   3   35-42     282   5   195-226

//  263   0    10       273   3   43-50     283   5   227-255

//  264   1  11,12      274   3   51-58

//  265   1  13,14      275   3   59-66

//  266   1  15,16      276   4   67-82

// 284 alphabet size

const std::array<int, 256> match_len_to_code = {
    0, 0, 0, 0,
    // 4-10
    257, 258, 259, 260, 261, 262, 263,
    // 11-18
    264, 264, 265, 265, 266, 266, 267, 267,
    // 19-34
    268, 268, 268, 268, 269, 269, 269, 269, 270, 270, 270, 270, 271, 271, 271, 271,
    // 35-66
    272, 272, 272, 272, 272, 272, 272, 272,
    273, 273, 273, 273, 273, 273, 273, 273,
    274, 274, 274, 274, 274, 274, 274, 274,
    275, 275, 275, 275, 275, 275, 275, 275,
    // 67-130
    276, 276, 276, 276, 276, 276, 276, 276, 276, 276, 276, 276, 276, 276, 276, 276,
    277, 277, 277, 277, 277, 277, 277, 277, 277, 277, 277, 277, 277, 277, 277, 277,
    278, 278, 278, 278, 278, 278, 278, 278, 278, 278, 278, 278, 278, 278, 278, 278,
    279, 279, 279, 279, 279, 279, 279, 279, 279, 279, 279, 279, 279, 279, 279, 279,
    // 131-255
    280, 280, 280, 280, 280, 280, 280, 280, 280, 280, 280, 280, 280, 280, 280, 280,
    280, 280, 280, 280, 280, 280, 280, 280, 280, 280, 280, 280, 280, 280, 280, 280,
    281, 281, 281, 281, 281, 281, 281, 281, 281, 281, 281, 281, 281, 281, 281, 281,
    281, 281, 281, 281, 281, 281, 281, 281, 281, 281, 281, 281, 281, 281, 281, 281,
    282, 282, 282, 282, 282, 282, 282, 282, 282, 282, 282, 282, 282, 282, 282, 282,
    282, 282, 282, 282, 282, 282, 282, 282, 282, 282, 282, 282, 282, 282, 282, 282,
    283, 283, 283, 283, 283, 283, 283, 283, 283, 283, 283, 283, 283, 283, 283, 283,
    283, 283, 283, 283, 283, 283, 283, 283, 283, 283, 283, 283, 283};

const std::array<uint8_t, 27> match_len_xbits = {
    0, 0, 0, 0, 0, 0, 0,
    1, 1, 1, 1,
    2, 2, 2, 2,
    3, 3, 3, 3,
    4, 4, 4, 4,
    5, 5, 5, 5};

const std::array<uint8_t, 27> match_len_mins = {
    4, 5, 6, 7, 8, 9, 10,
    11, 13, 15, 17,
    19, 23, 27, 31,
    35, 43, 51, 59,
    67, 83, 99, 115,
    131, 163, 195, 227};

// ---------------- dict distances/back references (used for encoding blocks) -------------

void init_prefix_id_codes();
int get_prefix_id_code(int rp_idx);

const std::array<uint8_t, 32> prefix_id_xbits = {
    1, 1, 2, 2, 3, 3, 4, 4,
    5, 5, 6, 6, 7, 7, 8, 8,
    9, 9, 10, 10, 11, 11, 12, 12,
    13, 13, 14, 15, 16, 17, 18, 19};

const std::array<int, 32> prefix_id_mins = {
    0, 2, 4, 8, 12, 20, 28, 44,
    60, 92, 124, 188, 252, 380, 508, 764,
    1020, 1532, 2044, 3068, 4092, 6140, 8188, 12284,
    16380, 24572, 32764, 49148, 81916, 147452, 278524, 540668};

// ---------------- dict distances/back references (used for encoding dict) -------------

void init_dict_dist_codes();
int get_dict_dist_code(int d);

const std::array<uint8_t, 30> dict_dist_xbits = {
    0, 0, 0, 0, 1, 1, 2, 2, 3, 3,
    4, 4, 5, 5, 6, 6, 7, 7, 8, 8,
    9, 9, 10, 10, 11, 11, 12, 12,
    13, 13};

const std::array<int, 30> dict_dist_mins = {
    1, 2, 3, 4, 5, 7, 9, 13, 17, 25,
    33, 49, 65, 97, 129, 193, 257, 385, 513, 769,
    1025, 1537, 2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577};

/**
 * @brief writes prefix codes (huffman codes) to provided destination
 *
 */
struct bit_writer {

    unsigned char *dst;
    int dst_idx;

    uint32_t rest;
    uint8_t rest_bits; // how many "busy" bits is in rest at the moment

    bit_writer(unsigned char *dst);
    ~bit_writer();

    void add_bits(uint16_t prefix, uint8_t bit_len);
    void add_times_0(int times);
    void add_times_x(int times, int bit_length);
    void encode_alphabet(std::vector<std::tuple<uint16_t, uint8_t>> *prefixes);
    void reset();
    void flush();
    void write(uint8_t _byte);
};

/**
 * @brief encoding data - contains all required info for encoding sush as:
 * frequencies, bit lengths and prefix codes
 *
 */
struct e_data {

    std::vector<std::tuple<int, uint16_t>> freq;         // freq and corresponding original code
    std::vector<std::tuple<uint16_t, uint8_t>> prefixes; // prefix and bit_len
    std::vector<int> buf;
    std::vector<uint8_t> bit_len_count;
    std::vector<uint8_t> encoded_alphabet;

    int size;
    uint8_t max_bit_len;

    e_data(int _size, uint8_t _max_bit_len);

    void reset();
    void print();
    void add_count(int code);
    void compute_prefix();
    std::tuple<uint16_t, uint8_t> get_prefix(int at);
    void get_bit_lens(std::vector<uint8_t> *dst);

    // testing purposes
    void cmp_freq(std::vector<int> *other);

    void compare_prefixes(std::vector<uint16_t> *other, std::vector<uint8_t> *bit_lens);
};

/**
 * @brief decoding data - contains required functionality for decoding
 *
 */
struct d_data {

    std::vector<std::tuple<uint16_t, uint8_t>> prefixes; // prefix and bit_len, testing only
    std::vector<std::tuple<int16_t, uint8_t>> map;       // maps prefix code to corresponding symbol
    unsigned char *src;                                  // src to decode
    int64_t offset;                                      // starting point of the data this decoder cares about
    int src_size;
    int next_start;
    int word_start;

    uint8_t min_bitlen;
    uint8_t max_bitlen;

    // void prefixes_from_bit_lens();
    void restore_prefixes();

    bool next(std::vector<int> *word_codes);
    bool match(std::vector<int> *word_codes);
    void decode_dict(std::vector<int16_t> *temp);

    d_data(unsigned char *src, int src_size);
    ~d_data();
};

typedef std::tuple<int, int, int> record; // src_idx, back_ref, match_len

int __encode_dict(std::vector<std::vector<uint8_t>> *dict, unsigned char *dst);
std::vector<std::vector<uint8_t>> __decode_dict(unsigned char *src, int src_size);