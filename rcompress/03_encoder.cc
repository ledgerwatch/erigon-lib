#include "03_encoder.h"
#include "05_decoder.h" // testing only

// ----------- helper functions -----------

// ----------- externs -----------

Encoder *NewEncoder(Dict *dict) {

    return new Encoder(dict);
}

void DeleteEncoder(Encoder *enc) {
    delete enc;
}

// int CountMatches(Encoder *enc, unsigned char *data, int data_size, int *sizes, int sizes_size, int *preCompressed, int preCompressed_size) {
//     return enc->count_matches(data, data_size, sizes, sizes_size, preCompressed, preCompressed_size);
// }

// int ReduceDict(Encoder *enc) {

//     return enc->reduce_dict();
// }

int EncodeBlock(Encoder *enc, unsigned char *data, int data_size, int *sizes, int sizes_size, int *preCompressed, int preCompressed_size, unsigned char *dst) {
    return enc->encode_block(data, data_size, sizes, sizes_size, preCompressed, preCompressed_size, dst);
}

int EncodeDict(Encoder *enc, unsigned char *dst) {
    return enc->encode_dict(dst);
}

// ----------- ENCODER -----------

Encoder::Encoder(Dict *d) {
    init_prefix_id_codes();
    dict = d;
    lits_and_matches = new e_data(R_MAX_ALPH_SIZE, R_MAX_BIT_LEN);
    _bit_writer = new bit_writer(nullptr);
    // rest = 0;
    // rest_bits = 0;

    // dst_idx = 4;

    total_bytes = 0;
    total_dict_ref = 0;

    estim_compressed = 0;
}

Encoder::~Encoder() {

    // std::cout << "DELETING ENCODER\n";
    // std::cout << "TOTAL BY ENCODER: " << total_bytes << "\n";
    // std::cout << "ESTIMATED COMPRESSED BLOCK: " << estim_compressed << "\n";
    delete lits_and_matches;
}

void Encoder::reset() {
    lits_and_matches->reset();
    _bit_writer->reset();
    // rest = 0;
    // rest_bits = 0;

    // dst_idx = 1;
}

// void Encoder::write_header(uint32_t header) {
//     dst[0] = header >> 24;
//     dst[1] = (header & 0x00FF0000) >> 16;
//     dst[2] = (header & 0x0000FF00) >> 8;
//     dst[3] = (header & 0x000000FF);
// }

// void Encoder::write(uint8_t _byte) {
//     dst[dst_idx++] = _byte;
// }

// void Encoder::flush() {
//     int full_bytes = rest_bits / 8;
//     int _rest_bits = rest_bits % 8;
//     // std::cout << "full_bytes: " << full_bytes << "\n";
//     // std::cout << "_rest_bits: " << _rest_bits << "\n";
//     for (int i = 0; i < full_bytes; i++) {
//         write(rest >> 24);
//         rest <<= 8;
//     }

//     if (_rest_bits > 0)
//         write(rest >> 24);

//     rest = 0;
//     rest_bits = 0;
// }

// void Encoder::add_bits(uint16_t prefix, uint8_t bit_len) {

//     assert(rest >= 0);
//     assert(rest_bits >= 0);
//     assert(rest_bits < 8);
//     assert(bit_len > 0);

//     int bit_len_sum = rest_bits + bit_len;
//     uint32_t combined = (rest) | (prefix << (32 - bit_len - rest_bits));
//     int full_bytes = bit_len_sum / 8;
//     int _rest_bits = bit_len_sum % 8;

//     for (int i = 0; i < full_bytes; i++) {
//         // encoded_alphabet.push_back(combined >> 24);
//         write(combined >> 24);
//         combined <<= 8;
//     }

//     rest_bits = _rest_bits;
//     rest = combined;
// }

// void Encoder::add_times_0(int times) {
//     if (times >= 3 && times <= 10) {
//         add_bits(R_REPEAT_0_3, 5);
//         add_bits(times - 3, 3);
//     } else if (times >= 11 && times <= 138) {
//         add_bits(R_REPEAT_0_11, 5);
//         add_bits(times - 11, 7);
//     } else if (times > 138) {
//         add_bits(R_REPEAT_0_11, 5);
//         add_bits(138 - 11, 7);
//         times -= 138;
//         add_times_0(times);
//     } else {
//         assert(times < 3);
//         for (int k = 0; k < times; k++)
//             add_bits(0, 5);
//     }
// }
// void Encoder::add_times_x(int times, int bit_length) {
//     if (times >= 3 && times <= 6) {
//         add_bits(R_COPY_PREV, 5);
//         add_bits(times - 3, 2);
//     } else if (times > 6) {
//         add_bits(R_COPY_PREV, 5);
//         add_bits(6 - 3, 2);
//         times -= 6;
//         add_times_x(times, bit_length);
//     } else {
//         assert(times < 3);
//         if (times > 0) {
//             for (int k = 0; k < times; k++)
//                 add_bits(bit_length, 5);
//         }
//     }
// }

// void Encoder::encode_alphabet(std::vector<std::tuple<uint16_t, uint8_t>> *prefixes) {

//     int size = prefixes->size();
//     assert(size >= 2);

//     int bl, j, times;

//     for (int i = 0; i < size;) {
//         bl = std::get<1>(prefixes->at(i));

//         if (bl == 0) {
//             times = 1;
//             for (j = i + 1; j < size; j++) {
//                 if (std::get<1>(prefixes->at(j)) != 0) break;
//                 times++;
//             }

//             add_times_0(times);
//             i = j;
//         } else {

//             add_bits(bl, 5);

//             times = 0;
//             for (j = i + 1; j < size; j++) {
//                 if (std::get<1>(prefixes->at(j)) != bl) break;
//                 times++;
//             }

//             add_times_x(times, bl);

//             i = j;
//         }
//     }

//     flush();
// }

int Encoder::encode_block(unsigned char *data, int data_size, int *sizes, int sizes_size, int *preCompressed, int preCompressed_size, unsigned char *_dst) {

    _bit_writer->dst = _dst;
    reset();

    this->dst = _dst;
    int literals, match_len, prefix_id;
    int p_size;
    int pre_idx = 0;
    int start = 0;
    int w_size;
    int rp_idx = 0;
    std::vector<uint8_t> word;

    // on the first run compute frequencies of litterals, match_lens and a prefix_ids
    // after that compute prefix codes (huffman codes) for encoding
    for (int i = 0; i < sizes_size; i++) {
        w_size = sizes[i];
        word = std::vector<uint8_t>(data + start, data + start + w_size);

        // for (int i = 0; i < w_size; i++) {
        //     assert(word.at(i) == data[i + start]);
        // }

        int w_idx = start;

        if (w_size > 0) {

            total_bytes += w_size;
            int q = 0;
            while (1) {

                literals = preCompressed[pre_idx++];
                match_len = preCompressed[pre_idx++];
                prefix_id = preCompressed[pre_idx++];

                if (rp_idx < (int)dict->remapped.size())
                    rp_idx = dict->remapped.at(prefix_id);

                for (; literals && q < w_size; literals--, q++) {
                    assert(word.at(q) == (uint8_t)data[w_idx++]);
                    // count each word.at(q) here
                    // lits_and_matches_count[word.at(q)]++;
                    lits_and_matches->add_count(word.at(q));
                }

                if (match_len == 0) {
                    assert(prefix_id == 0);
                    break;
                }

                if (rp_idx >= 0) {
                    p_size = dict->final_dict.at(rp_idx).size();

                    assert(p_size >= 4);

                    assert(match_len >= 4);

                    // map match_len to code (match_len_code)
                    // count match_len_code here
                    auto m_code = match_len_to_code[match_len];
                    // lits_and_matches_count[m_code]++;
                    lits_and_matches->add_count(m_code);

                    int f = w_idx;
                    for (int p = 0; p < match_len; p++) {
                        int exp = data[f++];
                        int got = dict->final_dict.at(rp_idx).at(p);

                        if (exp != got) {

                            std::cout << "WORD NUM: " << i << "\n";
                            std::cout << "W_IDX: " << w_idx << ", F: " << f << "\n";

                            std::cout << "P_SIZE: " << p_size << "\n";
                            for (int k = 0; k < p_size; k++)
                                std::cout << (int)dict->final_dict.at(rp_idx).at(k) << " ";
                            std::cout << "\n";

                            for (int k = 0; k < p_size; k++)
                                std::cout << (int)dict->prefixes.at(prefix_id).at(k) << " ";
                            std::cout << "\n";

                            std::cout << "WORD:\n";
                            for (int k = 0; k < p_size; k++)
                                std::cout << (int)word.at(k) << " ";
                            std::cout << "\n";

                            std::cout << "expected: " << exp << ", got: " << got << ", at idx: " << p << "\n";
                        }
                        assert(exp == got);
                    }
                } else {

                    int f = w_idx;
                    for (int p = 0; p < match_len; p++)
                        lits_and_matches->add_count(data[f++]);
                }

                w_idx += match_len;
                q += match_len;
            }

            lits_and_matches->add_count(256);
            start += w_size;
        } else {
            lits_and_matches->add_count(256);
        }
        assert(w_size == (int)word.size());
    }

    lits_and_matches->compute_prefix();

    _bit_writer->encode_alphabet(&lits_and_matches->prefixes);

    pre_idx = 0;
    start = 0;

    int saved_idx = _bit_writer->dst_idx;
    // on the second run encode every word to dst
    for (int i = 0; i < sizes_size; i++) {
        w_size = sizes[i];
        word = std::vector<uint8_t>(data + start, data + start + w_size);

        for (int i = 0; i < w_size; i++) {
            assert(word.at(i) == data[i + start]);
        }

        int w_idx = start;

        if (w_size > 0) {

            int q = 0;
            while (1) {

                literals = preCompressed[pre_idx++];
                match_len = preCompressed[pre_idx++];
                prefix_id = preCompressed[pre_idx++];

                if (rp_idx < (int)dict->remapped.size())
                    rp_idx = dict->remapped.at(prefix_id);
                // rp_idx = dict->remapped.at(prefix_id);

                for (; literals && q < w_size; literals--, q++) {
                    assert(word.at(q) == (uint8_t)data[w_idx++]);
                    auto [prefix_code, bitlen] = lits_and_matches->get_prefix(word.at(q));
                    _bit_writer->add_bits(prefix_code, bitlen);
                }

                if (match_len == 0) {
                    assert(prefix_id == 0);
                    break;
                }

                if (rp_idx >= 0) {
                    p_size = dict->final_dict.at(rp_idx).size();

                    assert(p_size >= 4);

                    assert(match_len >= 4);

                    // map match_len to code (match_len_code)
                    auto m_code = match_len_to_code[match_len];
                    assert(m_code >= 257);
                    assert(m_code < 284);

                    // encode m_code here
                    auto [prefix_code, bitlen] = lits_and_matches->get_prefix(m_code);
                    // add_bits(prefix_code, bitlen);
                    _bit_writer->add_bits(prefix_code, bitlen);

                    int xbits = match_len_xbits[m_code - 257];
                    if (xbits > 0) {
                        int diff = match_len - match_len_mins[m_code - 257];
                        assert(diff <= (1 << xbits) - 1);
                        _bit_writer->add_bits(diff, xbits);
                    }

                    // map prefix_idx to code (prefix_id_code)
                    auto p_code = get_prefix_id_code(rp_idx);
                    assert(p_code <= 31);
                    assert(p_code >= 0);

                    _bit_writer->add_bits(p_code, 5);

                    _bit_writer->add_bits(rp_idx - prefix_id_mins[p_code], prefix_id_xbits[p_code]);

                    int f = w_idx;
                    for (int p = 0; p < match_len; p++) {
                        int exp = data[f++];
                        int got = dict->final_dict.at(rp_idx).at(p);

                        if (exp != got) {

                            std::cout << "WORD NUM: " << i << "\n";
                            std::cout << "W_IDX: " << w_idx << ", F: " << f << "\n";

                            std::cout << "P_SIZE: " << p_size << "\n";
                            for (int k = 0; k < p_size; k++)
                                std::cout << (int)dict->final_dict.at(rp_idx).at(k) << " ";
                            std::cout << "\n";

                            for (int k = 0; k < p_size; k++)
                                std::cout << (int)dict->prefixes.at(prefix_id).at(k) << " ";
                            std::cout << "\n";

                            std::cout << "WORD:\n";
                            for (int k = 0; k < p_size; k++)
                                std::cout << (int)word.at(k) << " ";
                            std::cout << "\n";

                            std::cout << "expected: " << exp << ", got: " << got << ", at idx: " << p << "\n";
                        }
                        assert(exp == got);
                    }
                } else {

                    int f = w_idx;
                    for (int p = q; p < q + match_len; p++) {
                        assert(word.at(p) == (uint8_t)data[f++]);
                        auto [prefix_code, bitlen] = lits_and_matches->get_prefix(word.at(p));

                        _bit_writer->add_bits(prefix_code, bitlen);
                    }
                }

                w_idx += match_len;
                q += match_len;
            }
            auto [prefix_code, bitlen] = lits_and_matches->get_prefix(R_FLAG_EOW);

            _bit_writer->add_bits(prefix_code, bitlen);
            _bit_writer->flush();

            start += w_size;
        } else {
            auto [prefix_code, bitlen] = lits_and_matches->get_prefix(R_FLAG_EOW);

            _bit_writer->add_bits(prefix_code, bitlen);
            _bit_writer->flush();
        }
        assert(w_size == (int)word.size());
    }

    _bit_writer->flush();

    return _bit_writer->dst_idx;

    // --------------- TESTING PART ---------------

    // decode block here word by word and compare

    auto _d_data = new d_data(dst, _bit_writer->dst_idx);
    _d_data->restore_prefixes();
    int p1, b1;
    int p2, b2;
    for (int i = 0; i < R_MAX_ALPH_SIZE; i++) {
        std::tie(p1, b1) = lits_and_matches->prefixes.at(i);
        std::tie(p2, b2) = _d_data->prefixes.at(i);

        if (p1 != p2 || b1 != b2) {
            std::cout << "p1: " << p1 << ", p2: " << p2 << "\n";
            std::cout << "b1: " << b1 << ", b2: " << b2 << "\n";
            std::cout << "at idx: " << i << "\n";
        }

        assert(p1 == p2);
        assert(b1 == b2);
    }

    // for (int i = saved_idx; i < saved_idx + 10; i++) {
    //     std::cout << std::bitset<8>(dst[i]);
    // }
    // std::cout << "\n";

    // auto [_a, ba] = lits_and_matches->prefixes.at(73);
    // auto [_b, bb] = lits_and_matches->prefixes.at(128);
    // std::cout << 73 << " -> " << std::bitset<16>(_a) << ": " << (int)ba << "\n";
    // std::cout << 128 << " -> " << std::bitset<16>(_b) << ": " << (int)bb << "\n";

    std::vector<int> word_codes;
    word_codes.reserve(1 << 20);
    int next_word = 0;
    int exp_size;
    int got_size;
    start = 0;
    std::vector<uint8_t> result;
    result.reserve(1 << 20);
    while (_d_data->next(&word_codes)) {
        if (next_word == sizes_size) break;
        result.clear();
        w_size = sizes[next_word++];

        // for (int q = 0; q < (int)word_codes.size(); q++) {
        //     std::cout << word_codes.at(q) << " ";
        // }
        // std::cout << "\n";

        word = std::vector<uint8_t>(data + start, data + start + w_size);
        exp_size = word.size();

        if (exp_size > 0) {
            start += w_size;
        }

        int match_code, match_diff, match_len, match_idx;
        // std::cout << "word_codes size: " << word_codes.size() << "\n";
        for (int i = 0; i < (int)word_codes.size();) {
            int code = word_codes.at(i);
            // std::cout << code << " ";
            if (code > R_FLAG_EOW) {
                assert(code < R_MAX_ALPH_SIZE);
                int xbits = match_len_xbits[code - 257];
                // std::cout << "\ncode: " << code << ", xbits: " << xbits << "\n";

                if (xbits > 0) {
                    int diff = word_codes.at(i + 1);
                    match_len = match_len_mins[code - 257] + diff;
                    match_code = word_codes.at(i + 2);
                    // std::cout << "match_code: " << match_code << "\n";
                    match_diff = word_codes.at(i + 3);
                    // std::cout << "match_diff: " << match_diff << "\n";
                    i += 4;
                    match_idx = prefix_id_mins[match_code] + match_diff;
                    // std::cout << "match_idx: " << match_idx << "\n";
                } else {
                    match_len = match_len_mins[code - 257];
                    match_code = word_codes.at(i + 1);
                    // std::cout << "match_code: " << match_code << "\n";
                    match_diff = word_codes.at(i + 2);
                    // std::cout << "match_diff: " << match_diff << "\n";
                    i += 3;
                    match_idx = prefix_id_mins[match_code] + match_diff;
                    // std::cout << "match_idx: " << match_idx << "\n";
                }
                // std::cout << "MATCH_LEN: " << match_len << "\n";
                assert(match_len <= 255);
                for (int q = 0; q < match_len; q++) {
                    result.push_back(dict->final_dict.at(match_idx).at(q));
                }

            } else {
                result.push_back(code);
                i++;
            }
        }
        // std::cout << "\n";

        got_size = result.size();

        if (exp_size != got_size) {
            std::cout << "-----------------------------> EXP_SIZE: " << exp_size << ", GOT_SIZE: " << got_size << "\n";
        }
        assert(exp_size == got_size);
        int min_size = exp_size < got_size ? exp_size : got_size;
        // std::cout << "MIN_SIZE: " << min_size << "\n";
        for (int q = 0; q < min_size; q++) {
            int exp = word.at(q);
            int got = result.at(q);

            if (exp != got) {
                std::cout << "EXPECTED: " << exp << ", GOT: " << got << ", at idx: " << q << "\n";
            }
            assert(exp == got);
        }
        // assert(exp_size == got_size);
    }

    std::cout << "NEXT_WORD: " << next_word << ", SIZES_SIZE: " << sizes_size << "\n";

    std::cout << "DATA_SIZE: " << data_size << "\n";
    std::cout << "DST_IDX: " << _bit_writer->dst_idx << "\n";
    delete _d_data;
    estim_compressed += _bit_writer->dst_idx;

    return _bit_writer->dst_idx;
}

// #include "dict_de_encoder.h"

int Encoder::encode_dict(unsigned char *dst) {

    // return 0;

    // std::cout << "STARTING ENCODE DICT\n";

    int encoded_size = __encode_dict(&dict->final_dict, dst);

    return encoded_size;

    // --------------- TESTING PART ---------------

    std::cout << "ENCODED DICT SIZE: " << encoded_size << "\n";

    auto decoded_dict = __decode_dict(dst, encoded_size);

    int e_size = dict->final_dict.size();
    int g_size = decoded_dict.size();

    int min_size = e_size > g_size ? g_size : e_size;

    if (e_size != g_size) {
        std::cout << "expected dict size: " << e_size << ", got size: " << g_size << "\n";
    }

    if (g_size > e_size) {
        std::cout << "Extra sizes: \n";
        for (int i = e_size; i < g_size; i++) {
            std::cout << decoded_dict.at(i).size() << "\n";
        }
    }

    for (int i = 0; i < min_size; i++) {

        auto v1 = dict->final_dict.at(i);
        auto v2 = decoded_dict.at(i);

        int exp_size = v1.size();
        int got_size = v2.size();
        if (exp_size != got_size) {
            std::cout << "expected_size: " << exp_size << ", got_size: " << got_size << ", at idx: " << i << "\n";
        }

        int min = exp_size > got_size ? got_size : exp_size;

        for (int q = 0; q < min; q++) {
            int exp = v1.at(q);
            int got = v2.at(q);

            if (exp != got) {
                std::cout << "expected byte: " << exp << ", got byte: " << got << ", at idx: " << q << "\n";
            }
            assert(exp == got);
        }

        assert(v1.size() == v2.size());
    }

    // assert(e_size == g_size);

    std::cout << "DECODE DICT_DONE\n";

    return encoded_size;
}

// bool cmp_with_priority(const priority_tuple &lhs, const priority_tuple &rhs) {

//     if (std::get<2>(lhs) == std::get<2>(rhs)) {
//         return std::get<0>(lhs) > std::get<0>(rhs);
//     } else if (std::get<2>(lhs) == 2 && std::get<2>(rhs) == 3) {
//         return std::get<0>(lhs) > std::get<0>(rhs);
//     } else if (std::get<2>(lhs) == 3 && std::get<2>(rhs) == 2) {
//         return std::get<0>(lhs) > std::get<0>(rhs);
//     } else {
//         return std::get<2>(lhs) > std::get<2>(rhs);
//     }
// }

// int Encoder::reduce_dict() {

//     std::cout << "REDUCE DICT CALLED\n";

//     std::sort(prefix_quads.begin(), prefix_quads.end(), std::greater<std::tuple<int, int>>());
//     std::sort(prefix_large.begin(), prefix_large.end(), std::greater<std::tuple<int, int>>());

//     int len_4 = 0;
//     int len_g = 0;
//     int N = 10;
//     int M = N;
//     for (int i = 0; i < (int)prefix_quads.size(); i++) {
//         auto [p_count, p_idx] = prefix_quads.at(i);
//         if (p_count == 2) {
//             N = i;
//             break;
//         }

//         std::get<0>(to_prioritise.at(p_idx)) += p_count;
//         std::get<2>(to_prioritise.at(p_idx)) = 2;
//     }
//     std::cout << "QUAD MORE > 2: " << N << "\n";

//     for (int i = 0; i < (int)prefix_large.size(); i++) {
//         auto [p_count, p_idx] = prefix_large.at(i);
//         if (p_count == 1) {
//             N = i;
//             break;
//         }

//         std::get<0>(to_prioritise.at(p_idx)) += p_count;
//         if (std::get<2>(to_prioritise.at(p_idx)) == 2)
//             std::get<2>(to_prioritise.at(p_idx)) = 3;
//         else
//             std::get<2>(to_prioritise.at(p_idx)) = 1;
//     }

//     std::cout << "LARGE MORE > 1: " << N << "\n";

//     for (int i = 0; i < (int)to_prioritise.size(); i++) {
//         auto [p_count, p_idx, to_keep] = to_prioritise.at(i);
//         if (!to_keep) {
//             dict->prefixes.at(p_idx).clear();
//         } else {
//             // if (to_keep == 2) {
//             //     max_match.at(p_idx) = 4;
//             // }

//             if (max_match.at(p_idx) >= 4 && dict->prefixes.at(p_idx).size() > max_match.at(p_idx)) {
//                 dict->prefixes.at(p_idx).resize(max_match.at(p_idx));
//             }
//         }
//     }

//     int expected_prefixes = 0;
//     std::sort(to_prioritise.begin(), to_prioritise.end(), cmp_with_priority);
//     for (int i = 0; i < (int)to_prioritise.size(); i++) {
//         auto [p_count, p_idx, to_keep] = to_prioritise.at(i);
//         int max_match_size = max_match.at(p_idx);
//         remapped.at(p_idx) = i;
//         // new_dict.push_back(dict->prefixes.at(p_idx));
//         if (p_count == 0) break;
//         std::cout << p_count << ", " << p_idx << ", " << (int)to_keep << ", max_match: " << max_match_size << "\n";
//         expected_prefixes++;
//     }

//     std::cout << "EXPECTED PREFIXES: " << expected_prefixes << "\n";

//     std::cout << "\n";

//     // move quads to the front

//     int total_size = 0;
//     for (int i = 0; i < (int)dict->prefixes.size(); i++) {
//         total_size += dict->prefixes.at(i).size();
//     }
//     std::cout << "DICT TOTAL_SIZE: " << total_size << "\n";

//     std::cout << "TOTAL_DICT_REF: " << total_dict_ref << "\n";
//     return 0;
// }

//        Extra                  Extra                    Extra
//   Code Bits Prefix_ID    Code Bits    Prefix_ID    Code Bits   Prefix_ID
//   ---- ---- -------      ---- ----    -------      ---- ----   -------
//    0    3   0..7           8   10     2050..3073     16   10   10242..11265
//    1    4   8..23          9   10     3074..4097     17   10   11266..12289
//    2    5   24..55         10  10     4098..5121     18   10   12290..13313
//    3    6   56..129        11  10     5122..6145     19   10   13314..14337
//    4    7   130..257       12  10     6146..7169     20   10   14334..15357
//    5    8   258..513       13  10     7170..8193     21   10   15358..16381
//    6    9   514..1025      14  10     8194..9217     22   10   16382..17405
//    7    10  1026..2049     15  10     9218..10241    23   10   17406..18429
//                                                      24   10   18430..19453
//                                                      25   10   19454..20477
//                                                      26   10   20478..21501
//                                                      27   10   21502..22525
//                                                      28   10   22526..23549
//                                                      29   10   23550..24573
//                                                      30   10   24574..25597
//                                                      31   10   25598..26622

// ----------- TESTS -----------

// int Encoder::count_matches(unsigned char *data, int data_size, int *sizes, int sizes_size, int *preCompressed, int preCompressed_size) {

//     int literals, match_len, prefix_id;
//     int pre_idx = 0;
//     int start = 0;
//     int w_size;
//     std::vector<uint8_t> word;

//     int words = 0;
//     for (int i = 0; i < sizes_size; i++) {
//         w_size = sizes[i];
//         word = std::vector<uint8_t>(data + start, data + start + w_size);
//         int w_idx = start;
//         words++;
//         // std::cout << "------------------------ word_size: " << w_size << "\n";
//         if (w_size > 0) {

//             total_bytes += w_size;
//             int q = 0;
//             while (1) {

//                 literals = preCompressed[pre_idx++];
//                 match_len = preCompressed[pre_idx++];
//                 prefix_id = preCompressed[pre_idx++];

//                 for (; literals && q < w_size; literals--, q++) {
//                     assert(word.at(q) == (uint8_t)data[w_idx++]);
//                 }

//                 if (match_len == 0) {
//                     assert(prefix_id == 0);
//                     break;
//                 }
//                 assert(match_len >= 4);

//                 // add prefix_id here
//                 if (match_len == 4) {
//                     std::get<0>(prefix_quads.at(prefix_id))++;
//                 } else {
//                     std::get<0>(prefix_large.at(prefix_id))++;
//                 }

//                 if (match_len > max_match.at(prefix_id)) {
//                     max_match.at(prefix_id) = match_len;
//                 }

//                 int f = w_idx;
//                 for (int p = 0; p < match_len; p++) {
//                     assert(dict->prefixes.at(prefix_id).at(p) == (uint8_t)data[f++]);
//                 }

//                 w_idx += match_len;
//                 q += match_len;
//             }

//             start += w_size;
//         }
//         assert(w_size == (int)word.size());
//     }

//     std::cout << "COUNT MATCHES WORDS: " << words << "\n";
//     return 0;
// }

// p_size = dict->prefixes.at(prefix_id).size();
// if (p_size > 0) {
//     assert(p_size >= 4);

//     if (match_len == 0) {
//         assert(prefix_id == 0);
//         break;
//     }
//     assert(match_len >= 4);

//     // map match_len to code (match_len_code)
//     // count match_len_code here

//     // map prefix_idx to code (prefix_id_code)
//     // count

//     int f = w_idx;
//     for (int p = 0; p < match_len; p++) {
//         int exp = data[f];

//         int got = dict->final_dict.at(rp_idx).at(p);
//         if (exp != got) {
//             std::cout << "expected: " << exp << ", got: " << got << ", at rp_idx: " << p << "\n";
//         }
//         assert(exp == got);
//         f++;
//     }
// } else {

//     int f = w_idx;
//     for (int p = 0; p < match_len; p++) {
//         // count next `match_len` symbols
//     }
// }