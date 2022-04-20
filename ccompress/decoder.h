#ifndef CCOMPRESS_DECODER_
#define CCOMPRESS_DECODER_

#include "alphabet.h"
#include "cross_file_map.h"
#include "defs.h"
#include "rand.h"

#include <array>
// #include <bitset>
#include <cassert>
#include <iostream>
#include <list>
#include <tuple>
#include <vector>

class DecodeTable {
private:
    std::array<std::list<std::tuple<int, int, int>>, 256> ll_table;
    std::array<std::list<std::tuple<int, int, int>>, 256> dd_table;

public:
    DecodeTable() {}
    ~DecodeTable() {
        this->reset();
    }

    void reset() {
        for (int i = 0; i < 256; i++)
            ll_table[i].clear();

        for (int i = 0; i < 256; i++)
            dd_table[i].clear();
    }

    void ll_insert(int code, int encoded, int bit_length) {
        int extra = bit_length - 8; // negative number means less then 8 bit length
        int rest, first_8;
        std::tuple<int, int, int> tup;
        if (bit_length > 8) {
            first_8 = encoded >> extra;
            rest = (((uint32_t)encoded) << (32 - extra)) >> (32 - extra);
            tup = std::make_tuple(rest, extra, code);
            ll_table[first_8].push_back(tup);
        } else {
            tup = std::make_tuple(0, extra, code);
            ll_table[encoded].push_front(tup);
        }
    }

    void dd_insert(int code, int encoded, int bit_length) {
        int extra = bit_length - 8; // negative number means less then 8 bit length
        int rest, first_8;
        std::tuple<int, int, int> tup;
        if (bit_length > 8) {
            first_8 = encoded >> extra;
            rest = (((uint32_t)encoded) << (32 - extra)) >> (32 - extra);
            tup = std::make_tuple(rest, extra, code);
            dd_table[first_8].push_back(tup);
        } else {
            tup = std::make_tuple(0, extra, code);
            dd_table[encoded].push_front(tup);
        }
    }

    int ll_match(int encoded, int rest, int extra) {
        int r, e, c; // rest and extra and actualcode
        for (auto it = ll_table[encoded].begin(); it != ll_table[encoded].end(); ++it) {
            std::tie(r, e, c) = *it;
            if (e == extra && r == rest) {
                return c;
            }
        }

        return -1;
    }

    int dd_match(int encoded, int rest, int extra) {
        int r, e, c; // rest and extra and actualcode
        if (dd_table[encoded].size() > 0) {
            for (auto it = dd_table[encoded].begin(); it != dd_table[encoded].end(); ++it) {
                std::tie(r, e, c) = *it;
                if (e == extra && r == rest) {
                    return c;
                }
            }
        }

        return -1;
    }

    void ll_print() {
        std::cout << "\nLiteral&Length Table"
                  << "\n";
        int rest, extra, code;
        for (int i = 0; i < 256; i++) {
            if (ll_table[i].size() > 0) {
                std::cout << "\nfirst_8: " << i << "\n";
                for (auto it = ll_table[i].begin(); it != ll_table[i].end(); ++it) {
                    std::tie(rest, extra, code) = *it;
                    std::cout << "rest: " << rest << "\n";
                    std::cout << "extra: " << extra << "\n";
                    std::cout << "code: " << code << "\n";
                }
                std::cout << "----------------------\n";
            }
        }
    }

    void dd_print() {
        std::cout << "\nDistance Table"
                  << "\n";
        int rest, extra, code;
        for (int i = 0; i < 256; i++) {
            if (dd_table[i].size() > 0) {
                std::cout << "\nfirst_8: " << i << "\n";
                for (auto it = dd_table[i].begin(); it != dd_table[i].end(); ++it) {
                    std::tie(rest, extra, code) = *it;
                    std::cout << "rest: " << rest << "\n";
                    std::cout << "extra: " << extra << "\n";
                    std::cout << "code: " << code << "\n";
                }
                std::cout << "----------------------\n";
            }
        }
    }
};

class Decoder {
private:
    DecodeTable *decode_table;
    Alphabet *alphabet;

    uint8_t *data;
    size_t data_size;
    size_t data_idx;

    int ll_min_bit_length; // minimal possible length of literal/length code
    int ll_max_bit_length; // maximal possible length of literal/length code

    int dd_min_bit_length; // minimal possible length of distance code
    int dd_max_bit_length; // maximal possible length of distance code

    // what is left from previous decode cycle
    // since decoded blocks are 64kb at most, some words does not fit in single block
    // or single word does not fit in a single block
    // e.g: 1 - if one word takes 73kb, we have to decode 2 blocks to get that word
    //      so 64kb*2 - 73kb = prev_left
    // e.g: 2 - if we have 4 words, all about 20kb size
    //      so 3 words fit in a single block and the 4th word partialy fits into the block
    //      and part of it in another block
    //      so we decode full block and take only 3 words from it
    //      whatever left from 3 words goes to prev_left
    // std::vector<uint8_t> prev_left;

    std::vector<uint8_t> prev_block; // previously decoded block
    // std::vector<uint8_t> curr_block; // the block currently being decoded

    std::vector<int> ll_lengths; // decoded literal/lengths bit lengths
    std::vector<int> dd_lengths; // decoded distance bit lengths

    std::array<int, LL_ALPHABET> ll_codes = {};       // new literal/length codes
    std::array<int, DISTANCE_ALPHABET> dd_codes = {}; // new distance codes

    std::array<int, 20> ll_bl_count = {}; // used to restore encoded codes from bit_lengths
    std::array<int, 20> dd_bl_count = {}; // used to restore encoded codes from bit_lengths

    std::array<int, 20> ll_next_codes = {}; // used to restore encoded codes from bit_lengths
    std::array<int, 20> dd_next_codes = {}; // used to restore encoded codes from bit_lengths

    uint32_t combined; // 32-bit integer of combined uint8_t bytes into single uint32_t integer
    int total_bits;    // how  many bits currently in "combined"

    // count how many times each bit_length appears
    void count_bl() {
        int ll_size = ll_lengths.size();
        int dd_size = dd_lengths.size();

        for (int i = 0; i < ll_size; i++)
            ll_bl_count[ll_lengths[i]]++;

        for (int i = 0; i < dd_size; i++)
            dd_bl_count[dd_lengths[i]]++;

        ll_bl_count[0] = 0, dd_bl_count[0] = 0;
    }

    // starting "code" for each bit_length
    // e.g bit_length = 7; starting point could be 1 << 7
    void next_codes() {
        int ll_code = 0, dd_code = 0;
        for (int bits = 1; bits < 20; bits++) {
            ll_code = (ll_code + ll_bl_count[bits - 1]) << 1;
            ll_next_codes[bits] = ll_code;

            dd_code = (dd_code + dd_bl_count[bits - 1]) << 1;
            dd_next_codes[bits] = dd_code;
        }
    }

public:
    Decoder(uint8_t *data, size_t data_size) : data(data), data_size(data_size) {
        this->decode_table = new DecodeTable();
        this->alphabet = new Alphabet();

        this->prev_block.reserve(UINT16_MAX);

        for (int i = 0; i < LL_ALPHABET; i++)
            ll_codes[i] = -1;

        for (int i = 0; i < DISTANCE_ALPHABET; i++)
            dd_codes[i] = -1;

        for (int i = 0; i < 20; i++)
            ll_bl_count[i] = 0, dd_bl_count[i] = 0,
            ll_next_codes[i] = 0, dd_next_codes[i] = 0;

        data_idx = 0;

        ll_min_bit_length = UINT16_MAX;
        ll_max_bit_length = 0;

        dd_min_bit_length = UINT16_MAX;
        dd_max_bit_length = 0;

        combined = 0;
        total_bits = 0;
    }
    ~Decoder() {
        delete this->decode_table;
        delete this->alphabet;
    }

    void reset() {
        this->decode_table->reset();

        for (int i = 0; i < LL_ALPHABET; i++)
            ll_codes[i] = -1;

        for (int i = 0; i < DISTANCE_ALPHABET; i++)
            dd_codes[i] = -1;

        for (int i = 0; i < 20; i++)
            ll_bl_count[i] = 0, dd_bl_count[i] = 0,
            ll_next_codes[i] = 0, dd_next_codes[i] = 0;

        ll_min_bit_length = UINT16_MAX;
        ll_max_bit_length = 0;

        dd_min_bit_length = UINT16_MAX;
        dd_max_bit_length = 0;
    }

    void reset_hard() {
        data_idx = 0;

        combined = 0;
        total_bits = 0;
    }

    void decode_alphabet() {

        int length, extra;
        std::vector<int> LD(316);
        int ld_idx = 0; // index in decoded lengths

        for (;;) {
            fill_combined();
            if (ld_idx == 316) break;
            length = combined >> (32 - ALPHABET_BITS);

            if (length < 20) {
                combined <<= ALPHABET_BITS;
                total_bits -= ALPHABET_BITS;
                LD[ld_idx++] = length;
                if (ld_idx == 316) break;
            } else if (length == COPY_X_3_6) {
                combined <<= ALPHABET_BITS;
                total_bits -= ALPHABET_BITS;

                extra = combined >> (32 - BITS_X_3_6);

                combined <<= BITS_X_3_6;
                total_bits -= BITS_X_3_6;

                int last = LD[ld_idx - 1];

                for (int k = 0; k < extra + MIN_X_3_6; k++) {
                    LD[ld_idx++] = last;
                    if (ld_idx == 316) break;
                }

            } else if (length == COPY_0_3_10) {
                combined <<= ALPHABET_BITS;
                total_bits -= ALPHABET_BITS;

                extra = combined >> (32 - BITS_0_3_10);

                combined <<= BITS_0_3_10;
                total_bits -= BITS_0_3_10;

                for (int k = 0; k < extra + MIN_0_3_10; k++) {
                    LD[ld_idx++] = 0;
                    if (ld_idx == 316) break;
                }

            } else if (length == COPY_0_11_138) {
                combined <<= ALPHABET_BITS;
                total_bits -= ALPHABET_BITS;

                extra = combined >> (32 - BITS_0_11_138);

                combined <<= BITS_0_11_138;
                total_bits -= BITS_0_11_138;

                for (int k = 0; k < extra + MIN_0_11_138; k++) {
                    LD[ld_idx++] = 0;
                    if (ld_idx == 316) break;
                }

            } else {
                std::cout << "Length: " << length << "\n";
                std::cout << "ld_idx: " << ld_idx << "\n";

                for (auto l : LD)
                    std::cout << l << " ";
                std::cout << "\n";
                assert(false);
            }
        }

        this->ll_lengths = std::vector<int>(LD.begin(), LD.begin() + 286);
        this->dd_lengths = std::vector<int>(LD.begin() + 286, LD.end());

        // for (auto c : this->ll_lengths)
        //     std::cout << c << " ";
        // std::cout << "\n";

        // for (auto c : this->dd_lengths)
        //     std::cout << c << " ";
        // std::cout << "\n";

        this->generate_codes();
        this->prepare_table();
    }

    int decode_block(std::array<uint8_t, UINT16_MAX> *block) {

        // std::cout << "\n------------ DECODE START -------------"
        //           << "\n";

        this->reset();

        fill_combined();

        int is_compressed = combined >> (32 - 2);
        combined <<= 2;
        total_bits -= 2;
        assert(is_compressed == NOT_COMPRESSED || is_compressed == COMPRESSED);

        int found_code;
        int j;

        int encoded, rest, extra;

        int ll_length;
        int dd_distance;

        int block_idx = 0;

        if (is_compressed == COMPRESSED) {
            // std::cout << "IS COMPRESS"
            //           << "\n";
            this->decode_alphabet();
            while (1) {
                fill_combined();

                if (total_bits < ll_min_bit_length && data_idx >= data_size)
                    return block_idx;

                j = ll_min_bit_length;
                while (total_bits >= ll_min_bit_length && j <= ll_max_bit_length) {
                    if (j > total_bits) break;
                    extra = j - 8; // negative number means less then 8 bit length
                    if (j > 8) {
                        encoded = combined >> 24;               // take 1st 8 bits from combined
                        rest = (combined << 8) >> (32 - extra); // and then take extra bits
                    } else {
                        // j <= 8
                        encoded = combined >> (32 - j); // take 1st j bits from combined
                        rest = 0;                       // and take no extra bits
                    }

                    // match the code against table created in 'decode_alphabet()'
                    found_code = decode_table->ll_match(encoded, rest, extra);

                    if (found_code != -1) {
                        // there is a code that matched bits

                        combined <<= (8 + extra);  // take out 8 bits + extra bits
                        total_bits -= (8 + extra); // subtract from total_bits

                        if (found_code == 256) {
                            // if (prev_block.size() == 0)
                            //     assert(block_idx == UINT16_MAX);
                            prev_block = std::move(std::vector<uint8_t>(block->begin(), block->begin() + block_idx));
                            return block_idx;
                        } else if (found_code > 256) {
                            assert(found_code >= 257 && found_code <= 285);

                            // decode length and a distance
                            std::tie(ll_length, dd_distance) = decode_ld_code(found_code);

                            if (block_idx - dd_distance < 0) {
                                assert(prev_block.size() == UINT16_MAX);

                                int start = UINT16_MAX + (block_idx - dd_distance);

                                for (; ll_length > 0; ll_length--) {
                                    if (start == UINT16_MAX) break;
                                    block->at(block_idx++) = prev_block.at(start++);
                                }

                                if (start == UINT16_MAX && ll_length > 0) {
                                    int k = 0;
                                    for (; ll_length > 0; ll_length--)
                                        block->at(block_idx++) = block->at(k++);
                                }

                            } else {
                                int k = block_idx - dd_distance;
                                for (; ll_length > 0; ll_length--)
                                    block->at(block_idx++) = block->at(k++);
                            }

                        } else {
                            assert(found_code <= 255 && found_code >= 0);

                            block->at(block_idx++) = found_code;
                        }
                        j = ll_min_bit_length;
                    } else {
                        j++;
                    }
                }
            }
        } else { // not compressed
            assert(is_compressed == NOT_COMPRESSED);
            // std::cout << "NO COMPRESS"
            //           << "\n";
            uint8_t byte;
            while (1) {
                fill_combined();
                if (total_bits < 8 && data_idx >= data_size)
                    break;

                byte = combined >> 24;
                combined <<= 8;
                total_bits -= 8;
                block->at(block_idx++) = byte;
                if (block_idx == UINT16_MAX)
                    break;
            }
            prev_block = std::move(std::vector<uint8_t>(block->begin(), block->begin() + block_idx));
            return block_idx;
        }

        return block_idx;
    }

    // fill uint32_t 'combined' number with uint8_t numbers from compressed data
    void fill_combined() {
        uint32_t byte;
        while (data_idx < data_size && total_bits + 8 <= 32) {
            byte = data[data_idx++];
            combined = (combined) | (byte << (32 - total_bits - 8));
            total_bits += 8;
        }
    }

    // decode length and distance pair
    std::tuple<int, int> decode_ld_code(int code) {

        int encoded, rest, extra;
        int min_distance, min_length;
        int extra_bits;
        int dif;
        int dd_distance = -1;
        int ll_length = -1;
        int found_code = code;

        fill_combined();

        std::tie(min_length, extra_bits) = this->alphabet->ll_read_length(found_code);

        dif = extra_bits == 0 ? 0 : (combined >> (32 - extra_bits));

        ll_length = (min_length + dif);

        combined <<= extra_bits;
        total_bits -= extra_bits;

        int k = dd_min_bit_length;
        while (total_bits >= dd_min_bit_length && k <= dd_max_bit_length) {
            extra = k - 8;
            if (k > 8) {
                encoded = combined >> 24;
                rest = (combined << 8) >> (32 - extra);
            } else { // k <= 8
                encoded = combined >> (32 - k);
                rest = 0;
            }

            found_code = decode_table->dd_match(encoded, rest, extra);

            if (found_code != -1) {

                combined <<= (8 + extra);
                total_bits -= (8 + extra);

                fill_combined();

                std::tie(min_distance, extra_bits) = this->alphabet->dd_read_distance(found_code);

                dif = extra_bits == 0 ? 0 : (combined >> (32 - extra_bits));
                dd_distance = min_distance + dif;

                combined <<= extra_bits;
                total_bits -= extra_bits;

                break;
            } else {
                k++;
            }
        }
        assert(dd_distance != -1);
        assert(ll_length != -1);
        return std::make_tuple(ll_length, dd_distance);
    }

    void generate_codes() {
        // this->ll_lengths = std::vector<int>(ll_l->begin(), ll_l->end());
        // this->dd_lengths = std::vector<int>(dd_l->begin(), dd_l->end());

        this->count_bl();
        this->next_codes();

        int len;
        for (int n = 0; n < LL_ALPHABET; n++) {
            len = ll_lengths[n];
            if (len != 0) {
                ll_codes[n] = ll_next_codes[len];
                ll_next_codes[len]++;
            }
        }

        for (int n = 0; n < DISTANCE_ALPHABET; n++) {
            len = dd_lengths[n];
            if (len != 0) {
                dd_codes[n] = dd_next_codes[len];
                dd_next_codes[len]++;
            }
        }
    }

    std::vector<int> get_ll_codes() {
        return std::vector<int>(this->ll_codes.begin(), this->ll_codes.end());
    }

    std::vector<int> get_dd_codes() {
        return std::vector<int>(this->dd_codes.begin(), this->dd_codes.end());
    }

    void prepare_table() {

        assert(ll_lengths.size() == LL_ALPHABET);
        assert(dd_lengths.size() == DISTANCE_ALPHABET);

        int len;
        for (int i = 0; i < LL_ALPHABET; i++) {
            len = ll_lengths[i];
            if (len != 0) {

                decode_table->ll_insert(i, ll_codes[i], len);

                if (len < ll_min_bit_length)
                    ll_min_bit_length = len;

                if (len > ll_max_bit_length)
                    ll_max_bit_length = len;
            }
        }

        for (int i = 0; i < DISTANCE_ALPHABET; i++) {
            len = dd_lengths[i];
            if (len != 0) {

                decode_table->dd_insert(i, dd_codes[i], len);

                if (len < dd_min_bit_length)
                    dd_min_bit_length = len;

                if (len > dd_max_bit_length)
                    dd_max_bit_length = len;
            }
        }

        // decode_table->ll_print();
        // decode_table->dd_print();
    }
};

#endif // CCOMPRESS_DECODER_

// std::tuple<int, int> decode(std::vector<uint8_t> *dst, int dst_start, int size_start) {

//     fill_combined();

//     int is_compressed = combined >> (32 - 2);
//     combined <<= 2;
//     total_bits -= 2;

//     int found_code;
//     int j;
//     int a, b, c;           // first, second and third bytes for size of the word
//     int size = size_start; // original word size
//     int dst_idx;

//     this->curr_block.clear();

//     int encoded, rest, extra;

//     int ll_length;
//     int dd_distance;

//     if (dst_start == 0)
//         dst->insert(dst->begin(), this->prev_left.begin(), this->prev_left.end());

//     dst_idx = dst_start;
//     if (size_start == 0)
//         dst_idx += this->prev_left.size();

//     if (is_compressed) {

//         this->decode_alphabet();

//         while (1) {
//             fill_combined();

//             if (total_bits < ll_min_bit_length && data_idx >= data_size)
//                 return std::make_tuple(dst_idx, -1);

//             j = ll_min_bit_length;
//             while (total_bits >= ll_min_bit_length && j <= ll_max_bit_length) {
//                 if (j > total_bits) break;
//                 extra = j - 8; // negative number means less then 8 bit length
//                 if (j > 8) {
//                     encoded = combined >> 24;               // take a 8 byte
//                     rest = (combined << 8) >> (32 - extra); // and then take extra bits
//                 } else {
//                     // j <= 8
//                     encoded = combined >> (32 - j); // take a j byte
//                     rest = 0;                       // and take no extra bits
//                 }

//                 // match the code against table created in 'decode_alphabet()'
//                 found_code = decode_table->ll_match(encoded, rest, extra);

//                 if (found_code != -1) {
//                     // there is a code that matched bits

//                     combined <<= (8 + extra);  // take out 8 bits + extra bits
//                     total_bits -= (8 + extra); // subtract from total_bits

//                     if (found_code == 256) { // 256 is a end of a block code
//                         // set previous block to the current block
//                         this->prev_block = std::move(this->curr_block);
//                         // clear the current block
//                         this->curr_block.clear();

//                         assert(prev_block.size() <= UINT16_MAX);

//                         // this is some kind of a flag representing the inital decode level
//                         // and the size of the first word in the block
//                         if (size == 0) {
//                             // first 3 bytes in a block is the size of the a word in it
//                             a = dst->at(0), b = dst->at(1), c = dst->at(2);
//                             size = (a << 16) | (b << 8) | c;
//                         }

//                         if (size > dst_idx) {
//                             // if the size of the word greater then whatever we decoded
//                             // then we have to decode another block to get the full word
//                             return this->decode(dst, dst_idx, size);
//                         } else {
//                             // if the size of the a word is not greater

//                             // clear whatever left from prev_left, we'll set it later
//                             this->prev_left.clear();

//                             // starting point of the next word (+3 byte size of the first word)
//                             int sp = size + 3;

//                             // how many words we decoded?
//                             // since we know the size of a word, there has to be 1 word
//                             int words = 1;
//                             for (;;) {
//                                 // if we can't get another 3 bytes from dst, break the loop
//                                 if (sp + 1 > dst_idx || sp + 2 > dst_idx) break;

//                                 // combine 3 bytes to get the next word size
//                                 a = dst->at(sp), b = dst->at(sp + 1), c = dst->at(sp + 2);
//                                 size = (a << 16) | (b << 8) | c; // next word size

//                                 // if next word wasn't fully decoded (it is in the next block)
//                                 if (sp + size + 3 > dst_idx) break;

//                                 // increament starting point (word size + 3 byte for its size)
//                                 sp += (size + 3);
//                                 words++;
//                             }

//                             // if starting point of the next word is less then we decoded
//                             // (e.g we have the word that wasn't fully decoded)
//                             // leave it to the next decode cycle
//                             if (sp < dst_idx)
//                                 this->prev_left.insert(
//                                     this->prev_left.begin(),
//                                     dst->begin() + sp,
//                                     dst->begin() + dst_idx);

//                             return std::make_tuple(sp, words);
//                         }
//                     } // end found_code == 256

//                     if (found_code > 256) { // length code followed by distance code

//                         // decode length and a distance
//                         std::tie(ll_length, dd_distance) = decode_ld_code(found_code);

//                         if ((int)curr_block.size() - dd_distance < 0) {
//                             // encoded sequence of repeated bytes is in previous block

//                             int prev_block_size = prev_block.size();

//                             assert(prev_block_size > 0);
//                             int start = prev_block_size + (curr_block.size() - dd_distance);
//                             assert(start < prev_block_size);
//                             assert(dd_distance <= UINT16_MAX);
//                             assert(start > 0);

//                             for (; ll_length > 0; ll_length--) {
//                                 if (start == prev_block_size) break;

//                                 curr_block.push_back(prev_block.at(start));
//                                 dst->at(dst_idx++) = prev_block.at(start++);
//                             }

//                             // if we reached the end of the prev_block, but still have the length
//                             if (start == prev_block_size && ll_length > 0) {
//                                 std::cout << "GOT HERE: (start == prev_block_size && ll_length > 0)"
//                                           << "\n";
//                                 int k = 0;
//                                 for (; ll_length > 0; ll_length--) {
//                                     curr_block.push_back(curr_block.at(k));
//                                     dst->at(dst_idx++) = curr_block.at(k++);
//                                 }
//                             }
//                         } else {
//                             // encoded sequence of repeated bytes is in current block

//                             int k = dst_idx - dd_distance;
//                             for (; ll_length > 0; ll_length--) {
//                                 curr_block.push_back(dst->at(k));
//                                 dst->at(dst_idx++) = dst->at(k++);
//                             }
//                         }

//                     } else {
//                         // assert(found_code <= 255 && found_code >= 0);
//                         dst->at(dst_idx++) = found_code;
//                         curr_block.push_back(found_code);
//                     }
//                     j = ll_min_bit_length;
//                 } else {
//                     j++;
//                 }
//             }
//         }
//     } else {
//         int bytes_added = 0;
//         uint8_t byte;
//         while (1) {
//             fill_combined();
//             if (total_bits < 8 && data_idx >= data_size) break;

//             byte = combined >> 24;
//             combined <<= 8;
//             total_bits -= 8;
//             dst->at(dst_idx++) = byte;
//             curr_block.push_back(byte);
//             bytes_added++;
//             if (bytes_added == UINT16_MAX)
//                 break;
//         }
//         this->prev_left.clear();

//         a = dst->at(0), b = dst->at(1), c = dst->at(2);
//         size = (a << 16) | (b << 8) | c;

//         int sp = size + 3;

//         int words = 1;
//         for (;;) {
//             // if we can't get another 3 bytes from dst, break the loop
//             if (sp + 1 > dst_idx || sp + 2 > dst_idx) break;

//             // combine 3 bytes to get the next word size
//             a = dst->at(sp), b = dst->at(sp + 1), c = dst->at(sp + 2);
//             size = (a << 16) | (b << 8) | c; // next word size

//             // if next word wasn't fully decoded (it is in the next block)
//             if (sp + size + 3 > dst_idx) break;

//             // increament starting point (word size + 3 byte for its size)
//             sp += (size + 3);
//             words++;
//         }

//         if (sp < dst_idx)
//             this->prev_left.insert(
//                 this->prev_left.begin(),
//                 dst->begin() + sp,
//                 dst->begin() + dst_idx);

//         return std::make_tuple(sp, words);
//     }

//     // int found_code;
//     // int j;
//     // int a, b, c;           // first, second and third bytes for size of the word
//     // int size = size_start; // original word size
//     // int dst_idx;
//     // this->curr_block.clear();

//     // int encoded, rest, extra;

//     // int ll_length;
//     // int dd_distance;

//     // if (dst_start == 0)
//     //     dst->insert(dst->begin(), this->prev_left.begin(), this->prev_left.end());

//     // dst_idx = dst_start;
//     // if (size_start == 0)
//     //     dst_idx += this->prev_left.size();

//     // this->decode_alphabet();

//     // while (1) {
//     //     fill_combined();

//     //     if (total_bits < ll_min_bit_length && data_idx >= data_size)
//     //         return std::make_tuple(dst_idx, -1);

//     //     j = ll_min_bit_length;
//     //     while (total_bits >= ll_min_bit_length && j <= ll_max_bit_length) {
//     //         if (j > total_bits) break;
//     //         extra = j - 8; // negative number means less then 8 bit length
//     //         if (j > 8) {
//     //             encoded = combined >> 24;               // take a 8 byte
//     //             rest = (combined << 8) >> (32 - extra); // and then take extra bits
//     //         } else {
//     //             // j <= 8
//     //             encoded = combined >> (32 - j); // take a j byte
//     //             rest = 0;                       // and take no extra bits
//     //         }

//     //         // match the code against table created in 'decode_alphabet()'
//     //         found_code = decode_table->ll_match(encoded, rest, extra);

//     //         if (found_code != -1) {
//     //             // there is a code that matched bits

//     //             combined <<= (8 + extra);  // take out 8 bits + extra bits
//     //             total_bits -= (8 + extra); // subtract from total_bits

//     //             if (found_code == 256) { // 256 is a end of a block code
//     //                 // set previous block to the current block
//     //                 this->prev_block = std::move(this->curr_block);
//     //                 // clear the current block
//     //                 this->curr_block.clear();

//     //                 assert(prev_block.size() <= UINT16_MAX);

//     //                 // this is some kind of a flag representing the inital decode level
//     //                 if (size == 0) {
//     //                     // first 3 bytes in a block is the size of the a word in it
//     //                     a = dst->at(0), b = dst->at(1), c = dst->at(2);
//     //                     size = (a << 16) | (b << 8) | c;
//     //                 }

//     //                 if (size > dst_idx) {
//     //                     // if the size of the word greater then whatever we decoded
//     //                     // then we have to decode another block to get the full word
//     //                     return this->decode(dst, dst_idx, size);
//     //                 } else {
//     //                     // if the size of the a word is not greater

//     //                     // clear whatever left from prev_left, we'll set it later
//     //                     this->prev_left.clear();

//     //                     // starting point of the next word (+3 byte size of the first word)
//     //                     int sp = size + 3;

//     //                     // how many words we decoded?
//     //                     // since we know the size of a word, there has to be 1 word
//     //                     int words = 1;
//     //                     for (;;) {
//     //                         // if we can't get another 3 bytes from dst, break the loop
//     //                         if (sp + 1 > dst_idx || sp + 2 > dst_idx) break;

//     //                         // combine 3 bytes to get the next word size
//     //                         a = dst->at(sp), b = dst->at(sp + 1), c = dst->at(sp + 2);
//     //                         size = (a << 16) | (b << 8) | c; // next word size

//     //                         // if next word wasn't fully decoded (it is in the next block)
//     //                         if (sp + size + 3 > dst_idx) break;

//     //                         // increament starting point (word size + 3 byte for its size)
//     //                         sp += (size + 3);
//     //                         words++;
//     //                     }

//     //                     // if starting point of the next word is less then we decoded
//     //                     // (e.g we have the word that wasn't fully decoded)
//     //                     // leave it to the next decode cycle
//     //                     if (sp < dst_idx)
//     //                         this->prev_left.insert(
//     //                             this->prev_left.begin(),
//     //                             dst->begin() + sp,
//     //                             dst->begin() + dst_idx);

//     //                     return std::make_tuple(sp, words);
//     //                 }
//     //             } // end found_code == 256

//     //             if (found_code > 256) { // length code followed by distance code

//     //                 // decode length and a distance
//     //                 std::tie(ll_length, dd_distance) = decode_ld_code(found_code);

//     //                 if ((int)curr_block.size() - dd_distance < 0) {
//     //                     // encoded sequence of repeated bytes is in previous block

//     //                     int prev_block_size = prev_block.size();

//     //                     assert(prev_block_size > 0);
//     //                     int start = prev_block_size + (curr_block.size() - dd_distance);
//     //                     assert(start < prev_block_size);
//     //                     assert(dd_distance <= UINT16_MAX);
//     //                     assert(start > 0);

//     //                     for (; ll_length > 0; ll_length--) {
//     //                         if (start == prev_block_size) break;

//     //                         curr_block.push_back(prev_block.at(start));
//     //                         dst->at(dst_idx++) = prev_block.at(start++);
//     //                     }

//     //                     // if we reached the end of the prev_block, but still have the length
//     //                     if (start == prev_block_size && ll_length > 0) {
//     //                         int k = 0;
//     //                         for (; ll_length > 0; ll_length--) {
//     //                             curr_block.push_back(dst->at(k));
//     //                             dst->at(dst_idx++) = dst->at(k++);
//     //                         }
//     //                     }
//     //                 } else {
//     //                     // encoded sequence of repeated bytes is in current block

//     //                     int k = dst_idx - dd_distance;
//     //                     for (; ll_length > 0; ll_length--) {
//     //                         curr_block.push_back(dst->at(k));
//     //                         dst->at(dst_idx++) = dst->at(k++);
//     //                     }
//     //                 }

//     //             } else {
//     //                 // assert(found_code <= 255 && found_code >= 0);
//     //                 dst->at(dst_idx++) = found_code;
//     //                 curr_block.push_back(found_code);
//     //             }
//     //             j = ll_min_bit_length;
//     //         } else {
//     //             j++;
//     //         }
//     //     }
//     // }

//     // for (; data_idx < data_size;) {

//     //     fill_combined();

//     //     j = ll_min_bit_length;
//     //     while (total_bits >= ll_min_bit_length && j <= ll_max_bit_length) {
//     //         if (j > total_bits) break;
//     //         extra = j - 8;
//     //         if (j > 8) {
//     //             encoded = combined >> 24;
//     //             rest = (combined << 8) >> (32 - extra);
//     //         } else { // j <= 8
//     //             encoded = combined >> (32 - j);
//     //             rest = 0;
//     //         }

//     //         found_code = decode_table->ll_match(encoded, rest, extra);
//     //         if (found_code != -1) {

//     //             combined <<= (8 + extra);
//     //             total_bits -= (8 + extra);

//     //             if (found_code == 256) { // end of the block

//     //                 this->prev_block = std::move(this->curr_block);
//     //                 this->curr_block.clear();

//     //                 assert(prev_block.size() <= UINT16_MAX);

//     //                 if (size == 0) {
//     //                     a = dst->at(0), b = dst->at(1), c = dst->at(2);
//     //                     size = (a << 16) | (b << 8) | c;
//     //                 }

//     //                 if (size > dst_idx) {
//     //                     return this->decode(dst, dst_idx, size);
//     //                 } else {
//     //                     this->prev_left.clear();
//     //                     int st = size + 3; // starting point of the next word + 3 byte (size of word)
//     //                     int words = 1;
//     //                     for (;;) {
//     //                         if (st + 1 > dst_idx || st + 2 > dst_idx) break;
//     //                         a = dst->at(st), b = dst->at(st + 1);
//     //                         c = dst->at(st + 2);
//     //                         size = (a << 16) | (b << 8) | c; // next word size
//     //                         if (st + size + 3 > dst_idx) break;

//     //                         st += (size + 3);
//     //                         words++;
//     //                     }
//     //                     if (st < dst_idx)
//     //                         this->prev_left.insert(
//     //                             this->prev_left.begin(),
//     //                             dst->begin() + st,
//     //                             dst->begin() + dst_idx);

//     //                     return std::make_tuple(st, words);
//     //                 }
//     //             }

//     //             if (found_code > 256) { // length code followed by distance code

//     //                 std::tie(ll_length, dd_distance) = decode_ld_code(found_code);
//     //                 assert(dd_distance <= MAX_DISTANCE);

//     //                 if ((int)curr_block.size() - dd_distance < 0) {
//     //                     assert(curr_block.size() >= 0);
//     //                     assert((int)curr_block.size() - dd_distance < 0);
//     //                     // std::cout << "CURRENT BLOCK SIZE: " << curr_block.size() << "\n";
//     //                     // std::cout << "DD_DISTANCE: " << dd_distance << "\n";
//     //                     int prev_block_size = prev_block.size();

//     //                     assert(prev_block_size > 0);
//     //                     int start = prev_block_size + (curr_block.size() - dd_distance);
//     //                     assert(start < prev_block_size);
//     //                     assert(dd_distance <= UINT16_MAX);
//     //                     assert(start > 0);
//     //                     // assert(dst_idx - start)

//     //                     for (; ll_length > 0; ll_length--) {
//     //                         if (start == prev_block_size) break;

//     //                         curr_block.push_back(prev_block.at(start));
//     //                         dst->at(dst_idx++) = prev_block.at(start++);
//     //                     }

//     //                     if (start == prev_block_size && ll_length > 0) {
//     //                         int k = 0;
//     //                         for (; ll_length > 0; ll_length--) {
//     //                             curr_block.push_back(dst->at(k));
//     //                             dst->at(dst_idx++) = dst->at(k++);
//     //                         }
//     //                     }
//     //                 } else {
//     //                     int k = dst_idx - dd_distance;
//     //                     for (; ll_length > 0; ll_length--) {
//     //                         curr_block.push_back(dst->at(k));
//     //                         dst->at(dst_idx++) = dst->at(k++);
//     //                     }
//     //                 }

//     //             } else { // literal code
//     //                 dst->at(dst_idx++) = found_code;
//     //                 curr_block.push_back(found_code);
//     //             }

//     //             j = ll_min_bit_length;
//     //         } else {
//     //             j++;
//     //         }
//     //     }
//     // }

//     // std::cout << "TOTAL BITS: " << total_bits << "\n";
//     // std::cout << "COMBINED: " << combined << "\n";

//     // std::cout << "SHOULDN'T HAVE HAPPENED"
//     //           << "\n";
//     // std::cout << "HARE AT THE END IN DECODER->DECODE"
//     //           << "\n";
//     // // exit(1);
//     // return std::make_tuple(-1, dst_idx);
// }