#include "02_dict.h"

// ----------- helper functions -----------

void set_bit(uint32_t n, std::array<uint32_t, 134217728> *filter) {
    int start = n / 32;
    int bit_index = n % 32; // from left to right, [0..32)
    uint32_t m = filter->at(start) | (1 << (31 - bit_index));
    filter->at(start) = m;
}

uint8_t get_bit(uint32_t n, std::array<uint32_t, 134217728> *filter) {
    int start = n / 32;
    int bit_index = n % 32; // from left to right, [0..32)
    uint8_t m = (filter->at(start) >> (31 - bit_index)) & 1;
    return m;
}

void recursive_fetch(bst_node *node, std::vector<uint8_t> *prefix, std::vector<std::vector<uint8_t>> *out, std::vector<int> *word_order, int *order_num, std::array<uint32_t, 134217728> *filter) {

    if (node == nullptr)
        return;

    recursive_fetch(node->left, prefix, out, word_order, order_num, filter); // the lowest key in this map

    node->order_num = *order_num;
    prefix->push_back(node->key);
    word_order->push_back((*order_num));

    bst_node *root = node->map->get_root();
    if (root != nullptr) {

        // go to the lowest key of this node's map
        recursive_fetch(root, prefix, out, word_order, order_num, filter);

    } else {

        int p_size = prefix->size();

        if (p_size >= 4) {
            (*order_num)++;
            uint32_t a, b, c, d, n;
            a = prefix->at(0), b = prefix->at(1);
            c = prefix->at(2), d = prefix->at(3);
            n = (a << 24) | (b << 16) | (c << 8) | d;
            // set nth bit in fitler to 1
            set_bit(n, filter);

            out->push_back(std::vector<uint8_t>(prefix->begin(), prefix->end()));
        }
    }

    prefix->pop_back();
    word_order->pop_back();

    recursive_fetch(node->right, prefix, out, word_order, order_num, filter);
}

// ----------- externs -----------

Dict *BuildStaticDict(Trie *t, int *created) {
    Dict *dict = new Dict();

    std::vector<uint8_t> prefix;
    prefix.reserve(256);

    std::vector<int> word_order;
    word_order.reserve(1024);
    int order_num = 0;

    recursive_fetch(
        t->get_map()->get_root(),
        &prefix,
        &dict->prefixes,
        &word_order,
        &order_num,
        &dict->filter);

    int prefixes_size = dict->prefixes.size();

    dict->to_prioritise.resize(prefixes_size);
    dict->prefix_quads.resize(prefixes_size);
    dict->prefix_large.resize(prefixes_size);
    dict->max_match.resize(prefixes_size);
    dict->min_match.resize(prefixes_size);
    dict->remapped.resize(prefixes_size);
    dict->final_dict.reserve(prefixes_size);

    for (int i = 0; i < prefixes_size; i++) {
        dict->to_prioritise.at(i) = std::make_tuple(0, i, 0);
        dict->prefix_quads.at(i) = std::make_tuple(0, i);
        dict->prefix_large.at(i) = std::make_tuple(0, i);
        dict->max_match.at(i) = 0;
        dict->min_match.at(i) = 255;
        dict->remapped.at(i) = -1;
    }

    (*created) = prefixes_size;

    return dict;
}

void DeleteDict(Dict *d) {
    delete d;
}

int Precompress(Dict *dict, Trie *t, unsigned char *word, int w_size, int *precompressed) {
    return dict->precompress(t, word, w_size, precompressed);
}

int CountMatches(Dict *dict, unsigned char *data, int data_size, int *sizes, int sizes_size, int *preCompressed, int preCompressed_size) {
    return dict->count_matches(data, data_size, sizes, sizes_size, preCompressed, preCompressed_size);
}

int ReduceDict(Dict *dict) {
    return dict->reduce_dict();
}

// ----------- DICT -----------

Dict::Dict(/* args */) {
    prefixes.reserve(1 << 24);
    filter = {};
}

Dict::~Dict() {
}

// this has to be read-only or every thread has to have its own prefix_count
int Dict::precompress(Trie *t, unsigned char *word, int w_size, int *precompressed) {

    auto t_map = t->get_map();
    uint32_t a, b, c, d, n;
    uint32_t last_order_num;
    int match_len = 0;

    int i;
    int literals = 0;
    int pre_idx = 0;

    for (i = 0; i < w_size - 3;) {
        a = word[i], b = word[i + 1];
        c = word[i + 2], d = word[i + 3];
        n = (a << 24) | (b << 16) | (c << 8) | d;

        if (get_bit(n, &filter)) {
            int match_len = 0;
            int j = i;
            bst_node *node = t_map->find(word[j]);
            while (node != nullptr && j < w_size) {

                assert(node->key == word[j]);
                last_order_num = node->order_num;
                match_len++;
                j++;
                t_map = node->map;
                node = t_map->find(word[j]);
            }

            assert(match_len >= 4);
            assert(match_len <= 255);

            t_map = t->get_map();

            assert(last_order_num < (int)prefixes.size());
            // prefix_count[last_order_num]++;
            j = i;
            for (int q = 0; q < match_len; q++) {
                assert(word[j++] == prefixes.at(last_order_num).at(q));
            }

            precompressed[pre_idx++] = literals;
            precompressed[pre_idx++] = match_len;
            precompressed[pre_idx++] = last_order_num;

            i += match_len;
            literals = 0;

        } else {
            i++;
            literals++;
        }
    }

    for (; i < w_size;) {
        i++;
        literals++;
    }

    precompressed[pre_idx++] = literals;
    precompressed[pre_idx++] = 0;
    precompressed[pre_idx++] = 0;

    return pre_idx;

    // ---------------- TESTING PART

    std::vector<uint8_t> reconstructed;
    reconstructed.reserve(pre_idx);
    int prefix_idx;
    int j = 0;
    for (int i = 0; i < pre_idx; i += 3) {
        literals = precompressed[i];
        match_len = precompressed[i + 1];
        prefix_idx = precompressed[i + 2];

        while (literals) {
            reconstructed.push_back(word[j]);
            j++;
            literals--;
        }
        for (int k = 0; k < match_len; k++) {
            reconstructed.push_back(prefixes.at(prefix_idx).at(k));
        }
        j += match_len;
    }
    int r_size = reconstructed.size();
    if (w_size != r_size) {
        std::cout << "EXPECTED SIZE: " << w_size << ", GOT SIZE: " << r_size << "\n";
    }
    assert(w_size == r_size);
    for (int i = 0; i < w_size; i++) {
        int exp = word[i];
        int got = reconstructed.at(i);
        if (exp != got) {
            std::cout << "Expected byte: " << exp << ", Got byte: " << got << ", at idx: " << i << "\n";
        }
        assert(exp == got);
    }

    // std::cout << "PRE_IDX IN C++: " << pre_idx << "\n";

    return pre_idx;
}

int Dict::count_matches(unsigned char *data, int data_size, int *sizes, int sizes_size, int *preCompressed, int preCompressed_size) {

    int literals, match_len, prefix_id;
    int pre_idx = 0;
    int start = 0;
    int w_size;
    std::vector<uint8_t> word;

    for (int i = 0; i < sizes_size; i++) {
        w_size = sizes[i];
        word = std::vector<uint8_t>(data + start, data + start + w_size);
        for (int i = 0; i < w_size; i++) {
            assert(word.at(i) == data[i + start]);
        }

        int w_idx = start;

        // std::cout << "------------------------ word_size: " << w_size << "\n";
        if (w_size > 0) {

            int q = 0;
            while (1) {

                literals = preCompressed[pre_idx++];
                match_len = preCompressed[pre_idx++];
                prefix_id = preCompressed[pre_idx++];

                for (; literals && q < w_size; literals--, q++) {
                    assert(word.at(q) == (uint8_t)data[w_idx++]);
                }

                if (match_len == 0) {
                    assert(prefix_id == 0);
                    break;
                }
                assert(match_len >= 4);

                if (match_len == 4) {
                    std::get<0>(prefix_quads.at(prefix_id))++;
                } else {
                    std::get<0>(prefix_large.at(prefix_id))++;
                }

                // preoritize by min len? and count?

                // if (match_len > max_match.at(prefix_id)) {
                //     max_match.at(prefix_id) = match_len;
                // }

                if (match_len > max_match.at(prefix_id)) {
                    max_match.at(prefix_id) = match_len;
                }

                if (match_len < min_match.at(prefix_id)) {
                    min_match.at(prefix_id) = match_len;
                }

                int f = w_idx;
                for (int p = 0; p < match_len; p++) {
                    assert(prefixes.at(prefix_id).at(p) == (uint8_t)data[f++]);
                }

                w_idx += match_len;
                q += match_len;
            }

            start += w_size;
        }
        assert(w_size == (int)word.size());
    }

    return 0;
}

// bool cmp_with_priority(const priority_tuple &lhs, const priority_tuple &rhs) {

//     if (std::get<0>(lhs) == std::get<0>(rhs))
//         return std::get<2>(lhs) > std::get<2>(rhs);

//     if (std::get<2>(lhs) == std::get<2>(rhs))
//         return std::get<0>(lhs) > std::get<0>(rhs);

//     if (std::get<2>(lhs) == 2 && std::get<2>(rhs) == 3)
//         return std::get<0>(lhs) > std::get<0>(rhs);

//     if (std::get<2>(lhs) == 3 && std::get<2>(rhs) == 2)
//         return std::get<0>(lhs) > std::get<0>(rhs);

//     return std::get<2>(lhs) > std::get<2>(rhs);
// }

// bool cmp_with_min_match(const priority_tuple &lhs, const priority_tuple &rhs) {

//     if (std::get<0>(lhs) == std::get<0>(rhs))
//         return std::get<2>(lhs) < std::get<2>(rhs);

//     if (std::get<2>(lhs) == std::get<2>(rhs))
//         return std::get<0>(lhs) > std::get<0>(rhs);

//     return std::get<2>(lhs) < std::get<2>(rhs);
// }

bool cmp_greater(const priority_tuple &lhs, const priority_tuple &rhs) {

    return std::get<0>(lhs) > std::get<0>(rhs);
}

int Dict::reduce_dict() {

    std::sort(prefix_quads.begin(), prefix_quads.end(), std::greater<std::tuple<int, int>>());
    std::sort(prefix_large.begin(), prefix_large.end(), std::greater<std::tuple<int, int>>());

    int quads, large;

    for (int i = 0; i < R_MAX_QUADS && i < (int)prefix_quads.size(); i++) {
        auto [p_count, p_idx] = prefix_quads.at(i);
        if (p_count <= 2) {
            quads = i;
            break;
        }

        std::get<0>(to_prioritise.at(p_idx)) += p_count;
        std::get<2>(to_prioritise.at(p_idx)) = min_match.at(p_idx);
    }

    // std::cout << "QUAD MORE > 2: " << quads << "\n";

    for (int i = 0; i < (int)prefix_large.size(); i++) {
        auto [p_count, p_idx] = prefix_large.at(i);
        if (p_count <= 1) {
            large = i;
            break;
        }

        std::get<0>(to_prioritise.at(p_idx)) += p_count;
        std::get<2>(to_prioritise.at(p_idx)) = min_match.at(p_idx);
    }

    // std::cout << "LARGE MORE > 1: " << large << "\n";

    if (large > 0 || quads > 0) {
        for (int i = 0; i < (int)to_prioritise.size(); i++) {
            auto [p_count, p_idx, min_match] = to_prioritise.at(i);
            if (!min_match) {
                assert(min_match == 0);
                prefixes.at(p_idx).clear();
            } else {
                // assert(min_match == 1 || min_match == 2 || min_match == 3);
                if (max_match.at(p_idx) >= 4 && prefixes.at(p_idx).size() > max_match.at(p_idx)) {
                    prefixes.at(p_idx).resize(max_match.at(p_idx));
                }
            }
        }

        // std::cout << "SIZE to_prioritise: " << to_prioritise.size() << "\n";

        int expected_dict_size = 0;
        int expected_prefixes = 0;
        // std::sort(to_prioritise.begin(), to_prioritise.end(), cmp_with_min_match);
        std::sort(to_prioritise.begin(), to_prioritise.end(), cmp_greater);

        int d_idx = 0;
        int next_min = 4;
        int _min_match, _max_match, _d_idx, _p_count, _p_idx;
        bool printed = false;

        for (int i = 0; i < R_MAX_PREFIXES && i < (int)to_prioritise.size(); i++) {
            auto [p_count, p_idx, min_match] = to_prioritise.at(i);

            if (p_count < 2) break;

            // if (!min_match) break;
            assert(min_match >= 4);
            int max_match_size = max_match.at(p_idx);

            if (max_match_size == 4 && min_match == 4 && d_idx > 4091) {
                continue;
            }

            if (max_match_size == 5 && max_match_size == 5 && d_idx > 540667) {
                continue;
            }

            assert(final_dict.size() == d_idx);
            remapped.at(p_idx) = d_idx;
            final_dict.push_back(prefixes.at(p_idx));

            expected_prefixes++;
            expected_dict_size += prefixes.at(p_idx).size();
            prefixes.at(p_idx).clear();

            d_idx++;
        }


    } else {
        to_prioritise.resize(0);
    }

    int total_size = 0;
    for (int i = 0; i < (int)prefixes.size(); i++) {
        total_size += prefixes.at(i).size();
        prefixes.at(i).clear();
    }

    prefixes.clear();

    return 0;
}
