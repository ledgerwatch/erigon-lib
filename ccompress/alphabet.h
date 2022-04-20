#ifndef CCOMPRESS_ALPHABET_
#define CCOMPRESS_ALPHABET_

#include "defs.h"
#include "tree.h"

#include <array>
#include <iostream>
#include <memory>
#include <tuple>

class Alphabet {
private:
    std::array<int, LL_ALPHABET> ll;       // literal and lengths weights
    std::array<int, DISTANCE_ALPHABET> dd; // distances alphabet weights

    std::array<int, 260> lengths_map;
    std::array<int, MAX_DISTANCE + 1> distance_map;

    std::array<int, 30> ll_min_lengths;
    std::array<int, 30> dd_min_lengths;

    std::array<int, 30> ll_extra_bits;
    std::array<int, 30> dd_extra_bits;

public:
    Alphabet(/* args */) {
        /* --------- lengths alphabet --------- */
        int n = 257;
        for (int i = 3; i <= 10; i++)
            lengths_map[i] = n++;

        lengths_map[11] = 265;
        lengths_map[12] = 265;

        lengths_map[13] = 266;
        lengths_map[14] = 266;

        lengths_map[15] = 267;
        lengths_map[16] = 267;

        lengths_map[17] = 268;
        lengths_map[18] = 268;

        for (int i = 19; i <= 22; i++)
            lengths_map[i] = 269;

        for (int i = 23; i <= 26; i++)
            lengths_map[i] = 270;

        for (int i = 27; i <= 30; i++)
            lengths_map[i] = 271;

        for (int i = 31; i <= 34; i++)
            lengths_map[i] = 272;

        for (int i = 35; i <= 42; i++)
            lengths_map[i] = 273;

        for (int i = 43; i <= 50; i++)
            lengths_map[i] = 274;

        for (int i = 51; i <= 58; i++)
            lengths_map[i] = 275;

        for (int i = 59; i <= 66; i++)
            lengths_map[i] = 276;

        for (int i = 67; i <= 82; i++)
            lengths_map[i] = 277;

        for (int i = 83; i <= 98; i++)
            lengths_map[i] = 278;

        for (int i = 99; i <= 114; i++)
            lengths_map[i] = 279;

        for (int i = 115; i <= 130; i++)
            lengths_map[i] = 280;

        for (int i = 131; i <= 162; i++)
            lengths_map[i] = 281;

        for (int i = 163; i <= 194; i++)
            lengths_map[i] = 282;

        for (int i = 195; i <= 226; i++)
            lengths_map[i] = 283;

        for (int i = 227; i <= 257; i++)
            lengths_map[i] = 284;

        lengths_map[258] = 285;

        /* --------- distances alphabet --------- */
        n = 0;
        for (int i = 1; i <= 4; i++)
            distance_map[i] = n++;

        distance_map[5] = 4;
        distance_map[6] = 4;

        distance_map[7] = 5;
        distance_map[8] = 5;

        for (int i = 9; i <= 12; i++)
            distance_map[i] = 6;

        for (int i = 13; i <= 16; i++)
            distance_map[i] = 7;

        for (int i = 17; i <= 24; i++)
            distance_map[i] = 8;

        for (int i = 25; i <= 32; i++)
            distance_map[i] = 9;

        for (int i = 33; i <= 48; i++)
            distance_map[i] = 10;

        for (int i = 49; i <= 64; i++)
            distance_map[i] = 11;

        for (int i = 65; i <= 96; i++)
            distance_map[i] = 12;

        for (int i = 97; i <= 128; i++)
            distance_map[i] = 13;

        for (int i = 129; i <= 192; i++)
            distance_map[i] = 14;

        for (int i = 193; i <= 256; i++)
            distance_map[i] = 15;

        for (int i = 257; i <= 384; i++)
            distance_map[i] = 16;

        for (int i = 385; i <= 512; i++)
            distance_map[i] = 17;

        for (int i = 513; i <= 768; i++)
            distance_map[i] = 18;

        for (int i = 769; i <= 1024; i++)
            distance_map[i] = 19;

        for (int i = 1025; i <= 1536; i++)
            distance_map[i] = 20;

        for (int i = 1537; i <= 2048; i++)
            distance_map[i] = 21;

        for (int i = 2049; i <= 3072; i++)
            distance_map[i] = 22;

        for (int i = 3073; i <= 4096; i++)
            distance_map[i] = 23;

        for (int i = 4097; i <= 6144; i++)
            distance_map[i] = 24;

        for (int i = 6145; i <= 8192; i++)
            distance_map[i] = 25;

        for (int i = 8193; i <= 12288; i++)
            distance_map[i] = 26;

        for (int i = 12289; i <= 16384; i++)
            distance_map[i] = 27;

        for (int i = 16385; i <= 24576; i++)
            distance_map[i] = 28;

        for (int i = 24577; i <= MAX_DISTANCE; i++)
            distance_map[i] = 29;

        /* --------- lengths mins & extra bits --------- */
        n = 3;
        for (int i = 0; i < 8; i++) {
            ll_min_lengths[i] = n++;
            ll_extra_bits[i] = 0;
        }

        assert(n == 11);
        int bits = 1;
        for (int i = 8; i < 28; i += 4) {
            ll_min_lengths[i] = n;
            n += (1 << bits);
            ll_min_lengths[i + 1] = n;
            n += (1 << bits);
            ll_min_lengths[i + 2] = n;
            n += (1 << bits);
            ll_min_lengths[i + 3] = n;
            n += (1 << bits);

            ll_extra_bits[i] = bits;
            ll_extra_bits[i + 1] = bits;
            ll_extra_bits[i + 2] = bits;
            ll_extra_bits[i + 3] = bits;

            bits++;
        }

        ll_min_lengths[28] = 258;
        ll_extra_bits[28] = 0;

        // for (int i = 0; i < 30; i++)
        //     std::cout << "idx: " << i + 257 << ", value: " << ll_min_lengths[i] << "\n";

        /* --------- distance mins & extra bits --------- */
        n = 1;
        for (int i = 0; i < 4; i++) {
            dd_min_lengths[i] = n++;
            dd_extra_bits[i] = 0;
        }

        assert(n == 5);
        bits = 1;
        for (int i = 4; i < 30; i += 2) {
            dd_min_lengths[i] = n;
            n += (1 << bits);
            dd_min_lengths[i + 1] = n;
            n += (1 << bits);

            dd_extra_bits[i] = bits;
            dd_extra_bits[i + 1] = bits;
            bits++;
        }
        // for (int i = 0; i < 30; i++)
        //     std::cout << "idx: " << i << ", value: " << dd_min_lengths[i] << "\n";
    }

    ~Alphabet() {}

    void reset() {
        for (int i = 0; i < LL_ALPHABET; i++)
            this->ll[i] = 0;

        for (int i = 0; i < DISTANCE_ALPHABET; i++)
            this->dd[i] = 0;
    }

    void add_length(int length) {
        int idx = this->lengths_map[length];
        this->ll[idx]++;
    }

    void add_distance(int distance) {
        int idx = this->distance_map[distance];
        this->dd[idx]++;
    }

    void add_literal(int idx) { this->ll[idx]++; }

    int ll_weight_sum(std::vector<int> *ll_lengths) {

        assert(ll_lengths->size() == LL_ALPHABET);
        int weight;
        int sum = 0;
        for (int i = 0; i < LL_ALPHABET; i++) {
            weight = this->ll[i];
            if (weight > 0 && i < 257) {
                assert(ll_lengths->at(i) != 0);
                sum += (weight * ll_lengths->at(i));
            }
        }
        return sum;
    }

    void to_ll_nodes(std::vector<std::unique_ptr<__tree::huff_node>> *v) {
        int weight; // frequency
        for (int i = 0; i < LL_ALPHABET; i++) {
            weight = this->ll[i];
            if (weight > 0) {
                std::unique_ptr<__tree::huff_node> n(new __tree::huff_node(i, weight));
                v->push_back(std::move(n));
            }
        }
    }

    void to_dd_nodes(std::vector<std::unique_ptr<__tree::huff_node>> *v) {
        int weight; // frequency
        for (int i = 0; i < DISTANCE_ALPHABET; i++) {
            weight = this->dd[i];
            if (weight > 0) {
                std::unique_ptr<__tree::huff_node> n(new __tree::huff_node(i, weight));
                v->push_back(std::move(n));
            }
        }
    }

    int ll_map(int length) {
        return this->lengths_map[length];
    }

    int dd_map(int distance) {
        return this->distance_map[distance];
    }

    std::tuple<int, int> ll_get_extra_bits(int length, int code) {
        assert(code <= 285 && code >= 257);
        int min_length = ll_min_lengths[code - 257];
        assert(min_length <= length);
        int extra_bits = ll_extra_bits[code - 257];
        int dif = length - min_length;
        assert((1 << extra_bits) - 1 >= dif);
        return std::make_tuple(dif, extra_bits);
    }

    std::tuple<int, int> dd_get_extra_bits(int distance, int code) {
        assert(code < 30 && code >= 0);
        int min_distance = dd_min_lengths[code];
        assert(min_distance <= distance);
        int extra_bits = dd_extra_bits[code];
        int dif = distance - min_distance;
        assert((1 << extra_bits) - 1 >= dif);
        return std::make_tuple(dif, extra_bits);
    }

    std::tuple<int, int> ll_read_length(int code) {
        assert(code <= 285 && code >= 257);
        int min_length = ll_min_lengths[code - 257];
        int extra_bits = ll_extra_bits[code - 257];
        return std::make_tuple(min_length, extra_bits);
    }

    std::tuple<int, int> dd_read_distance(int code) {
        assert(code < 30 && code >= 0);
        int min_distance = dd_min_lengths[code];
        int extra_bits = dd_extra_bits[code];
        return std::make_tuple(min_distance, extra_bits);
    }
};

// alphabet::alphabet(/* args */) {
// }

// alphabet::~alphabet() {
// }

#endif