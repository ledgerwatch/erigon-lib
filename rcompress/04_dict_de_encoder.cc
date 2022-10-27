#include "04_encoding_assets.h"

#define R_SHIFT_BITS 16
#define R_SEED 0x14E60CD
#define R_TABLE_SIZE (1 << R_SHIFT_BITS)
#define R_HASH_BITS (32 - R_SHIFT_BITS)
#define R_HASH_FUNC(x) (R_SEED * x) >> R_HASH_BITS

struct LUT {
    std::array<std::forward_list<int>, R_TABLE_SIZE> table;

    LUT() {}
    ~LUT() {}

    void insert(uint32_t n, int src_idx);
    record find_longest_match(uint32_t n, std::vector<uint16_t> *src, int src_idx, int src_size);
};

void LUT::insert(uint32_t n, int src_idx) {

    uint32_t hash = R_HASH_FUNC(n);

    table.at(hash).push_front(src_idx);
}

record LUT::find_longest_match(uint32_t n, std::vector<uint16_t> *src, int src_idx, int src_size) {

    uint32_t hash = R_HASH_FUNC(n);
    int idx;
    int j, k, temp;
    int match_len = -1;
    int back_ref = -1, _src_idx = -1;

    uint32_t a, b, c, d, m;

    for (auto it = table.at(hash).begin(); it != table.at(hash).end(); ++it) {

        idx = *it;

        if (src_idx - idx > (1 << 15)) {
            return std::make_tuple(_src_idx, back_ref, match_len);
        }

        a = src->at(idx), b = src->at(idx + 1);
        c = src->at(idx + 2), d = src->at(idx + 3);

        m = (a << 24) | (b << 16) | (c << 8) | d;

        if (m == n) {
            j = src_idx + 4, k = idx + 4, temp = 4;
            assert(j > k);
            while (j < src_size && temp < 255 && src->at(j) == src->at(k)) {
                if (src->at(j) == 256 || src->at(j) == 256) break;
                j++, k++, temp++;
            }

            if (temp >= match_len) {
                _src_idx = src_idx;
                back_ref = idx;
                match_len = temp;
            }
        }
    }
    return std::make_tuple(_src_idx, back_ref, match_len);
}

int __encode_dict(std::vector<std::vector<uint8_t>> *dict, unsigned char *dst) {

    init_dict_dist_codes();
    int prefixes = dict->size();

    if (prefixes == 0) {
        return 0;
    }

    LUT *lut = new LUT();

    e_data *_e_data = new e_data(R_MAX_ALPH_SIZE, R_MAX_BIT_LEN);
    bit_writer *_bit_writer = new bit_writer(dst);

    std::vector<record> records;
    records.reserve(4096);
    std::vector<uint16_t> temp;
    temp.reserve(1 << 24);

    uint32_t a, b, c, d, n;

    for (int i = 0; i < prefixes; i++) {

        int prefix_size = dict->at(i).size();
        assert(prefix_size <= 255);
        assert(prefix_size >= 4);

        for (int j = 0; j < prefix_size; j++)
            temp.push_back(dict->at(i).at(j));

        temp.push_back(R_FLAG_EOW);
    }

    int temp_size = temp.size();

    std::vector<uint16_t> match_lens1;
    std::vector<uint16_t> match_lens2;
    for (int i = 0; i < (int)temp.size() - 3;) {
        a = temp.at(i), b = temp.at(i + 1);
        c = temp.at(i + 2), d = temp.at(i + 3);

        if (a == R_FLAG_EOW || b == R_FLAG_EOW || c == R_FLAG_EOW || d == R_FLAG_EOW) {
            i++;
            continue;
        }

        n = (a << 24) | (b << 16) | (c << 8) | d;

        auto _record = lut->find_longest_match(n, &temp, i, temp_size);
        auto [src_idx, back_ref, match_len] = _record;
        lut->insert(n, i);
        if (match_len == -1) {
            i++;
        } else {
            assert(src_idx == i);
            int dist = src_idx - back_ref;
            int p = i - dist;
            for (int q = i; q < i + match_len; q++)
                assert(temp.at(q) == temp.at(p++));
            match_lens1.push_back(match_len);
            records.push_back(_record);

            i += match_len;
        }
    }


    int next_record = 0;
    int i = 0;
    if (records.size() > 0) {


        for (; i < temp_size;) {

            if (next_record < (int)records.size()) {
                auto [src_idx, back_ref, match_len] = records.at(next_record);
                assert(match_len >= 4);
                assert(match_len <= 255);
                if (i == src_idx) {
                    match_lens2.push_back(match_len);
                    assert(src_idx == i);
                    int dist = src_idx - back_ref;
                    int p = src_idx - dist;
                    for (int q = i; q < i + match_len; q++)
                        assert(temp.at(q) == temp.at(p++));

                    // add match_len code
                    auto m_code = match_len_to_code[match_len];
                    assert(m_code >= 257);
                    assert(m_code < R_MAX_ALPH_SIZE);
                    _e_data->add_count(m_code);
                    // add back_ref

                    i += match_len;
                    next_record++;
                } else {
                    // add literal
                    _e_data->add_count(temp.at(i));
                    i++;
                }
            } else {
                // add literl
                _e_data->add_count(temp.at(i));
                i++;
            }
        }

        assert(match_lens1.size() == match_lens2.size());
        int m_size = match_lens1.size();
        for (int i = 0; i < m_size; i++) {
            assert(match_lens1.at(i) == match_lens2.at(i));
        }

        _e_data->compute_prefix();
        _bit_writer->encode_alphabet(&_e_data->prefixes);
    }

    next_record = 0;
    i = 0;
    if (records.size() > 0) {

        for (; i < temp_size;) {

            if (next_record < (int)records.size()) {
                auto [src_idx, back_ref, match_len] = records.at(next_record);

                if (i == src_idx) {

                    assert(match_len >= 4);
                    assert(match_len < R_MAX_ALPH_SIZE);

                    auto m_code = match_len_to_code[match_len];
                    assert(m_code >= 257);
                    assert(m_code < 284);

                    auto [prefix_code, bitlen] = _e_data->get_prefix(m_code);
                    _bit_writer->add_bits(prefix_code, bitlen);

                    int xbits = match_len_xbits[m_code - 257];
                    if (xbits > 0) {
                        int diff = match_len - match_len_mins[m_code - 257];
                        assert(diff <= (1 << xbits) - 1);
                        // add_bits(diff, xbits);
                        _bit_writer->add_bits(diff, xbits);

                    }

                    // map prefix_idx to code (prefix_id_code)
                    int dist = src_idx - back_ref;
                    auto back_ref_code = get_dict_dist_code(dist);

                    assert(back_ref_code <= 29);
                    assert(back_ref_code >= 0);

                    _bit_writer->add_bits(back_ref_code, 5);

                    xbits = dict_dist_xbits[back_ref_code];
                    if (xbits > 0) {
                        int diff = dist - dict_dist_mins[back_ref_code];

                        assert(diff <= (1 << xbits) - 1);
                        _bit_writer->add_bits(diff, xbits);
                    }

                    i += match_len;
                    next_record++;
                } else {
                    auto [prefix_code, bitlen] = _e_data->get_prefix(temp.at(i));
                    _bit_writer->add_bits(prefix_code, bitlen);
                    i++;
                }
            } else {
                auto [prefix_code, bitlen] = _e_data->get_prefix(temp.at(i));
                _bit_writer->add_bits(prefix_code, bitlen);
                i++;
            }
        }

        _bit_writer->flush();
    }

    int dst_idx = _bit_writer->dst_idx;

    delete _e_data;
    delete lut;
    delete _bit_writer;

    return dst_idx;
}

std::vector<std::vector<uint8_t>> __decode_dict(unsigned char *src, int src_size) {
    std::vector<std::vector<uint8_t>> restored;

    if (src_size == 0) return restored;

    d_data *_d_data = new d_data(src, src_size);

    _d_data->restore_prefixes();

    std::vector<int16_t> temp;
    temp.reserve(1 << 24);
    std::vector<uint8_t> result;
    result.reserve(256);
    _d_data->decode_dict(&temp);

    for (int i = 0; i < (int)temp.size(); i++) {
        int16_t code = temp.at(i);
        if (code == R_FLAG_EOW) {
            restored.push_back(result);
            result.clear();
        } else {
            assert(code <= 255);
            assert(code >= 0);
            result.push_back(code);
        }
    }

    return restored;
}

// #include "rand.h"
// int main() {

//     Rand rng;
//     int N = 100;

//     std::vector<std::vector<uint8_t>> dict;
//     dict.reserve(N);
//     int size = 0;
//     for (int i = 0; i < N; i++) {
//         auto src = rng.rand_bytes(4, 255);
//         src[0] = 0;
//         src[1] = 0;
//         src[2] = 0;
//         src[3] = 0;

//         size += src.size();
//         dict.push_back(src);
//     }
//     std::vector<uint8_t> dst(size + 1024, 0);
//     int e_size = encode_dict(&dict, reinterpret_cast<unsigned char *>(&dst[0]));

//     std::cout << "SIZE: " << size << ", E_SIZE: " << e_size << "\n";

//     auto decoded_dict = decode_dict(reinterpret_cast<unsigned char *>(&dst[0]), e_size);

//     assert(dict.size() == decoded_dict.size());

//     // decoded_dict.at(3).at(3) = 155;

//     int d_size = dict.size();

//     for (int i = 0; i < d_size; i++) {

//         auto v1 = dict.at(i);
//         auto v2 = decoded_dict.at(i);

//         int exp_size = v1.size();
//         int got_size = v2.size();
//         if (exp_size != got_size) {
//             std::cout << "expected_size: " << exp_size << ", got_size: " << got_size << ", at idx: " << i << "\n";
//         }

//         int min = exp_size > got_size ? got_size : exp_size;

//         for (int q = 0; q < min; q++) {
//             int exp = v1.at(q);
//             int got = v2.at(q);

//             if (exp != got) {
//                 std::cout << "expected byte: " << exp << ", got byte: " << got << ", at idx: " << q << "\n";
//             }
//             assert(exp == got);
//         }

//         assert(v1.size() == v2.size());
//     }

//     return 0;
// }