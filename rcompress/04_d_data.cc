#include "04_encoding_assets.h"

d_data::d_data(unsigned char *_src, int _src_size) {
    src = _src;
    src_size = _src_size;

    min_bitlen = 255;
    max_bitlen = 0;
    next_start = 1;
}

d_data::~d_data() {
}

void d_data::restore_prefixes() {

    prefixes.reserve(R_MAX_ALPH_SIZE);
    map = std::vector<std::tuple<int16_t, uint8_t>>((1 << 15), std::make_tuple(-1, 0));

    prefixes.clear();
    // std::vector<uint8_t> bit_lens;
    // bit_lens.reserve(R_MAX_BIT_LEN);

    uint32_t rest = 0;
    uint8_t rest_bits = 0;
    uint8_t a;
    int bl_code; // bit length code
    int xtra;
    // uint32_t combined = (rest) | (prefix << (32 - bit_len - rest_bits));
    int i = next_start;

    // restore bit lens first
    for (;;) {
        if (prefixes.size() == R_MAX_ALPH_SIZE) break;
        while (i < src_size && rest_bits + 8 <= 32) {
            a = src[i];
            rest = rest | (a << (32 - rest_bits - 8));
            rest_bits += 8;
            i++;
        }

        bl_code = rest >> (32 - 5);

        // if (bl_code > R_REPEAT_0_11)
        //     std::cout << "i: " << i << ", " << bl_code << ": " << (int)rest_bits << "\n";

        assert(bl_code >= 0 && bl_code <= R_REPEAT_0_11);

        if (bl_code == R_REPEAT_0_11) {
            if (rest_bits < (5 + 7)) continue;

            rest <<= 5;
            xtra = rest >> 25;
            for (int j = 0; j < xtra + 11; j++)
                // prefixes.push_back(0);
                prefixes.push_back(std::make_tuple(0, 0));
            rest <<= 7;
            rest_bits -= (5 + 7);

        } else if (bl_code == R_REPEAT_0_3) {
            if (rest_bits < (5 + 3)) continue;

            rest <<= 5;
            xtra = rest >> 29;
            for (int j = 0; j < xtra + 3; j++)
                prefixes.push_back(std::make_tuple(0, 0));
            rest <<= 3;
            rest_bits -= (5 + 3);

        } else if (bl_code == R_COPY_PREV) {
            if (rest_bits < (5 + 2)) continue;

            rest <<= 5;
            xtra = rest >> 30;
            for (int j = 0; j < xtra + 3; j++) {
                prefixes.push_back(prefixes.at(prefixes.size() - 1));
            }

            rest <<= 2;
            rest_bits -= (5 + 2);

        } else {
            assert(bl_code >= 0 && bl_code <= R_MAX_BIT_LEN);

            // prefixes.push_back(bl_code);
            prefixes.push_back(std::make_tuple(0, bl_code));
            rest <<= 5;
            rest_bits -= 5;
        }
    }

    i -= (int)(rest_bits / 8);

    next_start = i;
    word_start = i;

    // restore prefixes lens first
    std::vector<uint8_t> bit_len_count(R_MAX_BIT_LEN + 1, 0);
    std::vector<uint16_t> next_codes(R_MAX_BIT_LEN + 1, 0);

    int max_bits = R_MAX_BIT_LEN;
    uint16_t code = 0;
    int len;

    for (int i = 0; i < R_MAX_ALPH_SIZE; i++)
        bit_len_count.at(std::get<1>(prefixes.at(i)))++;

    bit_len_count.at(0) = 0;

    for (int bits = 1; bits <= max_bits; bits++) {
        code = (code + bit_len_count.at(bits - 1)) << 1;
        next_codes.at(bits) = code;
    }

    for (int n = 0; n < R_MAX_ALPH_SIZE; n++) {
        // len = bit_lens.at(n);
        len = std::get<1>(prefixes.at(n));
        if (len != 0) {
            // dst->push_back(next_codes.at(len));
            std::get<0>(prefixes.at(n)) = next_codes.at(len);
            next_codes.at(len)++;
        } else {
            std::get<0>(prefixes.at(n)) = 0;
        }
    }

    for (int i = 0; i < R_MAX_ALPH_SIZE; i++) {
        auto [prefix, bitlen] = prefixes.at(i);
        if (bitlen > 0) {
            if (bitlen < min_bitlen)
                min_bitlen = bitlen;
            if (bitlen > max_bitlen)
                max_bitlen = bitlen;

            assert(std::get<0>(map.at(prefix)) == -1);

            map.at(prefix) = std::make_tuple((int16_t)i, bitlen);
        }
    }

    // std::cout << "MIN_BITLEN: " << (int)min_bitlen << ", MAX_BITLEN: " << (int)max_bitlen << "\n";
}

bool d_data::next(std::vector<int> *word_codes) {

    if (next_start == src_size) return false;

    word_codes->clear();

    uint32_t rest = 0;
    uint8_t rest_bits = 0;
    uint8_t a;

    int i = next_start;

    int prefix_code = 0;
    int xbits;
    int diff;
    int match_code;

    for (;;) {
        while (i < src_size && rest_bits + 8 <= 32) {
            a = src[i];
            rest = rest | (a << (32 - rest_bits - 8));
            rest_bits += 8;
            i++;
        }

        for (int j = min_bitlen; j <= max_bitlen; j++) {

            prefix_code = rest >> (32 - j);
            auto [code, bitlen] = map.at(prefix_code);

            if (code >= 0 && bitlen == j) {
                rest_bits -= j;
                rest <<= j;

                if (code == R_FLAG_EOW) {

                    next_start = i - (int)(rest_bits / 8);
                    return true;
                }

                word_codes->push_back(code);

                if (code > R_FLAG_EOW) {
                    assert(code < R_MAX_ALPH_SIZE);

                    while (i < src_size && rest_bits + 8 <= 32) {
                        a = src[i];
                        rest = rest | (a << (32 - rest_bits - 8));
                        rest_bits += 8;
                        i++;
                    }

                    xbits = match_len_xbits[code - 257];
                    if (xbits > 0) {
                        diff = rest >> (32 - xbits);
                        rest_bits -= xbits;
                        rest <<= xbits;

                        word_codes->push_back(diff);
                    }

                    while (i < src_size && rest_bits + 8 <= 32) {
                        a = src[i];
                        rest = rest | (a << (32 - rest_bits - 8));
                        rest_bits += 8;
                        i++;
                    }

                    match_code = rest >> (32 - 5);
                    rest_bits -= 5;
                    rest <<= 5;
                    word_codes->push_back(match_code);

                    xbits = prefix_id_xbits[match_code];
                    diff = rest >> (32 - xbits);
                    rest_bits -= xbits;
                    rest <<= xbits;

                    word_codes->push_back(diff);
                }

                break;
            }
        }
    }

    next_start = i - (int)(rest_bits / 8);

    return true;
}

bool d_data::match(std::vector<int> *word_codes) {

    if (next_start == src_size) return false;

    word_codes->clear();

    uint32_t rest = 0;
    uint8_t rest_bits = 0;
    uint8_t a;

    int i = next_start;

    int prefix_code = 0;
    int xbits;
    int diff;
    int match_code;

    for (;;) {
        while (i < src_size && rest_bits + 8 <= 32) {
            a = src[i];
            rest = rest | (a << (32 - rest_bits - 8));
            rest_bits += 8;
            i++;
        }

        for (int j = min_bitlen; j <= max_bitlen; j++) {

            prefix_code = rest >> (32 - j);
            auto [code, bitlen] = map.at(prefix_code);

            if (code >= 0 && bitlen == j) {
                rest_bits -= j;
                rest <<= j;

                if (code == R_FLAG_EOW)
                    return true;

                word_codes->push_back(code);

                if (code > R_FLAG_EOW) {
                    assert(code < R_MAX_ALPH_SIZE);

                    while (i < src_size && rest_bits + 8 <= 32) {
                        a = src[i];
                        rest = rest | (a << (32 - rest_bits - 8));
                        rest_bits += 8;
                        i++;
                    }

                    xbits = match_len_xbits[code - 257];
                    if (xbits > 0) {
                        diff = rest >> (32 - xbits);
                        rest_bits -= xbits;
                        rest <<= xbits;

                        word_codes->push_back(diff);
                    }

                    while (i < src_size && rest_bits + 8 <= 32) {
                        a = src[i];
                        rest = rest | (a << (32 - rest_bits - 8));
                        rest_bits += 8;
                        i++;
                    }

                    match_code = rest >> (32 - 5);
                    rest_bits -= 5;
                    rest <<= 5;
                    word_codes->push_back(match_code);

                    xbits = prefix_id_xbits[match_code];
                    diff = rest >> (32 - xbits);
                    rest_bits -= xbits;
                    rest <<= xbits;

                    word_codes->push_back(diff);
                }

                break;
            }
        }
    }

    return true;
}

void d_data::decode_dict(std::vector<int16_t> *word_codes) {

    if (next_start == src_size) return;

    word_codes->clear();

    uint32_t rest = 0;
    uint8_t rest_bits = 0;
    uint8_t a;

    int i = next_start;

    int prefix_code = 0;
    int xbits;

    int decoded = 0;

    for (;;) {

        if (i >= src_size && rest_bits < min_bitlen) break;

        while (i < src_size && rest_bits + 8 <= 32) {
            a = src[i];
            rest = rest | (a << (32 - rest_bits - 8));
            rest_bits += 8;
            i++;
        }

        for (int j = min_bitlen; j <= max_bitlen; j++) {

            prefix_code = rest >> (32 - j);
            auto [code, bitlen] = map.at(prefix_code);

            if (code >= 0 && bitlen == j) {
                rest_bits -= j;
                rest <<= j;

                if (code > R_FLAG_EOW) {

                    assert(code < R_MAX_ALPH_SIZE);

                    while (i < src_size && rest_bits + 8 <= 32) {
                        a = src[i];
                        rest = rest | (a << (32 - rest_bits - 8));
                        rest_bits += 8;
                        i++;
                    }

                    int back_ref_code, dist, match_len;

                    int diff = 0;
                    xbits = match_len_xbits[code - 257];

                    if (i >= src_size && rest_bits < xbits) return;

                    if (xbits > 0) {
                        diff = rest >> (32 - xbits);
                        rest_bits -= xbits;
                        rest <<= xbits;
                        // word_codes->push_back(diff);
                    }

                    match_len = match_len_mins[code - 257] + diff;

                    while (i < src_size && rest_bits + 8 <= 32) {
                        a = src[i];
                        rest = rest | (a << (32 - rest_bits - 8));
                        rest_bits += 8;
                        i++;
                    }

                    if (i >= src_size && rest_bits < 5) return;

                    back_ref_code = rest >> (32 - 5);
                    rest_bits -= 5;
                    rest <<= 5;

                    diff = 0;
                    xbits = dict_dist_xbits[back_ref_code];

                    if (i >= src_size && rest_bits < xbits) return;

                    if (xbits > 0) {
                        diff = rest >> (32 - xbits);
                        rest_bits -= xbits;
                        rest <<= xbits;
                    }

                    dist = dict_dist_mins[back_ref_code] + diff;

                    int idx = word_codes->size() - dist;
                    for (int q = 0; q < match_len; q++)
                        word_codes->push_back(word_codes->at(idx++));
                } else {

                    word_codes->push_back(code);
                }

                break;
            }
        }
    }
}