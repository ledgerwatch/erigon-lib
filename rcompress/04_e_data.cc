#include "04_encoding_assets.h"

/**
 * @brief Finds an optimal length-limited Huffman code no longer then max_bitlen-bits for a given frequency of bytes.
 *
 * @param freq [in] sorted frequency of bytes in ascending order without zeros. [out] corresponding bit lens
 * @param max_bitlen max bit length per symbol
 */
void package_merge(std::vector<int> *freq, int max_bitlen) {

    int freq_size = freq->size();
    if (freq_size <= 2) {
        for (int i = 0; i < freq_size; i++)
            freq->at(i) = 1;
    }

    std::vector<int> prev;                // keeps prev row, after that reused for merging
    std::vector<int> pairs;               // pairwise sums
    std::vector<uint32_t> flags(1024, 0); // to keep track of merged items
    prev.reserve(1024);
    pairs.reserve(1024);

    uint16_t mask = 0;

    int i = 0, j = 0;
    int prev_size, pairs_size;
    int relevant = freq_size * 2 - 2; // Kraft-McMillan inequality
    int depth = max_bitlen;
    int num_merged;
    int symbol;

    for (i = 0; i < freq_size; i++)
        prev.push_back(freq->at(i));

    assert(std::is_sorted(prev.begin(), prev.end()));

    for (int runs = 1; runs < depth; runs++) {

        pairs.clear();
        mask = 1 << runs;

        prev_size = prev.size();
        for (j = 0; j < prev_size - 1; j += 2)
            pairs.push_back((prev.at(j) + prev.at(j + 1)));

        prev.clear();
        prev.push_back(freq->at(0));
        prev.push_back(freq->at(1));
        pairs_size = pairs.size();

        for (i = 2, j = 0; i < freq_size && j < pairs_size;) {

            int orgnl = freq->at(i);
            int sum = pairs.at(j);

            if (orgnl <= sum) {
                prev.push_back(orgnl);
                i++;
            } else {
                // orignal freq > sum
                flags.at(prev.size()) |= mask;
                prev.push_back(pairs.at(j));
                j++;
            }
        }

        if (i < freq_size) {
            assert(j == pairs_size);

            for (; i < freq_size; i++)
                prev.push_back(freq->at(i));
        }

        if (j < pairs_size) {
            assert(i == freq_size);

            for (; j < pairs_size; j++) {
                flags.at(prev.size()) |= mask;
                prev.push_back(pairs.at(j));
            }
        }

        assert(std::is_sorted(prev.begin(), prev.end()));
    }

    for (i = 0; i < freq_size; i++)
        freq->at(i) = 0;

    while (depth > 0 && relevant > 0) {
        freq->at(0)++;
        freq->at(1)++;
        num_merged = 0;
        symbol = 2;
        depth--;
        mask = 1 << depth;
        for (int i = symbol; i < relevant; i++) {
            if ((flags.at(i) & mask) == 0) {
                freq->at(symbol)++;
                symbol++;
            } else {
                num_merged++;
            }
        }
        relevant = 2 * num_merged;
    }
}

// --------------------- e_data ---------------------

e_data::e_data(int _size, uint8_t _max_bit_len) {
    size = _size;
    max_bit_len = _max_bit_len;

    freq = std::vector<std::tuple<int, uint16_t>>(size, std::make_tuple(0, 0));
    prefixes = std::vector<std::tuple<uint16_t, uint8_t>>(size, std::make_tuple(0, 0));
    buf.reserve(size);
    bit_len_count = std::vector<uint8_t>(max_bit_len + 1, 0);
    encoded_alphabet.reserve(size);

    for (int i = 0; i < size; i++) {
        std::get<1>(freq.at(i)) = i;
    }
}

void e_data::reset() {
    for (int i = 0; i < size; i++) {
        freq.at(i) = std::make_tuple(0, i);
        prefixes.at(i) = std::make_tuple(0, 0);
    }
    buf.clear();
    encoded_alphabet.clear();
    for (int i = 0; i < max_bit_len + 1; i++)
        bit_len_count.at(i) = 0;
}

void e_data::print() {
    std::cout << "SIZE: " << size << "\n";
    std::cout << "Freq:\n";
    for (int i = 0; i < size; i++) {
        std::cout << "(" << std::get<0>(freq.at(i)) << ": " << (int)std::get<1>(freq.at(i)) << "),";
    }
    std::cout << "\n";
    std::cout << "Prefixes:\n";
    for (int i = 0; i < size; i++) {
        std::cout << "(" << i << ": " << (int)std::get<1>(prefixes.at(i)) << "),";
    }

    std::cout << "\n";
}

void e_data::cmp_freq(std::vector<int> *other) {
    assert((int)other->size() == size);
    for (int i = 0; i < size; i++)
        assert(other->at(i) == std::get<0>(freq.at(i)));
}

void e_data::add_count(int code) {
    assert(code < size);
    std::get<0>(freq.at(code))++;
}

void e_data::compute_prefix() {

    int idx = 0;
    int start;
    uint16_t code = 0;
    int len;
    int max_bits = max_bit_len;

    std::sort(freq.begin(), freq.end()); // sort in ascending order

    // skip all 0 values in sorted frequencies
    for (idx = 0; idx < size; idx++)
        if (std::get<0>(freq.at(idx)) > 0)
            break;

    start = idx; // save the point where freq.at(idx) > 0

    if (idx > 0)
        assert(std::get<0>(freq.at(idx - 1)) == 0);
    assert(std::get<0>(freq.at(idx)) > 0);

    for (; idx < size; idx++)
        buf.push_back(std::get<0>(freq.at(idx)));

    // get the optimal bit_len for each freq
    package_merge(&buf, max_bit_len);
    // optimal bit_len now is in buf

    // std::cout << "BUF:\n";
    // for (int i = 0; i < (int)buf.size(); i++) {
    //     std::cout << (int)buf.at(i) << " ";
    // }
    // std::cout << "\n";

    idx = start;
    start = 0;
    // std::cout << "SETTING BIT_LEN: size: " << size << "\n";
    for (; idx < size; idx++, start++) {
        assert(std::get<0>(freq.at(idx)) > 0);
        assert(buf.at(start) >= 1);           // min bit_len
        assert(buf.at(start) <= max_bit_len); // max bit_len

        int c = std::get<1>(freq.at(idx));           // get the actual code from sorted freq
        std::get<1>(prefixes.at(c)) = buf.at(start); // set the code as 2nd value
        bit_len_count.at(buf.at(start))++;           // how many "same width" bit_len we have?
    }
    // std::cout << "\n";

    // std::cout << "IN COMPRESS\n";
    // for (int i = 0; i <= max_bits; i++)
    //     std::cout << (int)bit_len_count.at(i) << " ";
    // std::cout << "\n";

    buf.clear();
    for (int i = 0; i <= max_bits; i++)
        buf.push_back(0);

    for (int bits = 1; bits <= max_bits; bits++) {
        code = (code + bit_len_count.at(bits - 1)) << 1; // starting point for "same width" bit_lens
        // assert(code < (1 << 15));
        buf.at(bits) = code;
    }

    for (int n = 0; n < size; n++) {
        len = std::get<1>(prefixes.at(n));
        if (len != 0) {
            std::get<0>(prefixes.at(n)) = buf.at(len); // set the prefix code
            buf.at(len)++;                             // increment starting point of each "same width"
        }
    }

    // testing for uniqueness
    std::vector<uint8_t> flags((1 << 16), 0);
    for (int n = 0; n < size; n++) {
        len = std::get<1>(prefixes.at(n));
        // std::cout << "len: " << len << "\n";
        if (len != 0) {
            code = std::get<0>(prefixes.at(n));
            assert(flags.at(code) == 0);
            assert(code < (1 << 15));
            flags.at(code) = 1;
        }
    }
}

std::tuple<uint16_t, uint8_t> e_data::get_prefix(int at) {
    return prefixes.at(at);
}

void e_data::get_bit_lens(std::vector<uint8_t> *dst) {
    dst->clear();
    for (int i = 0; i < size; i++)
        dst->push_back(std::get<1>(prefixes.at(i)));
}

// used for testing only
void e_data::compare_prefixes(std::vector<uint16_t> *other, std::vector<uint8_t> *bit_lens) {
    assert((int)other->size() == size);

    for (int i = 0; i < size; i++) {
        auto exp_pcode = std::get<0>(prefixes.at(i));
        auto exp_bitlen = std::get<1>(prefixes.at(i));
        auto got_pcode = other->at(i);
        auto got_bitlen = bit_lens->at(i);
        // std::cout << (int)exp_bitlen << ": " << (int)got_bitlen << "\n";
        assert(exp_bitlen == got_bitlen);

        // std::cout << "BYTE: " << i << ", EXP: " << std::bitset<16>(exp_pcode) << ", GOT: " << std::bitset<16>(got_pcode) << "\n";

        if (exp_pcode != got_pcode) {
            std::cout << "Expected: " << exp_pcode << ", Got: " << got_pcode << "\n";
            std::cout << "At idx: " << i << "\n";
        }
        assert(exp_pcode == got_pcode);
    }
}