#ifndef CCOMPRESS_ENCODER_
#define CCOMPRESS_ENCODER_

#include "defs.h"

#include <bitset>
#include <cassert>
#include <fstream>
#include <iostream>
#include <vector>

class Encoder {
private:
    std::array<uint8_t, UINT16_MAX> buf;
    int buf_idx;

    uint32_t rest_bits;  // whatever bits left from previous cycle
    int rest_bits_count; // how many bits currently is in "rest_bits"

    std::fstream *file;

public:
    std::vector<int> ll_lengths; // literal/length bit lengths
    std::vector<int> dd_lengths; // distance bit lengths

    std::array<int, LL_ALPHABET> ll_codes = {};       // new literal/length codes
    std::array<int, DISTANCE_ALPHABET> dd_codes = {}; // new distance codes

    std::array<int, 20> ll_bl_count = {}; // literal/length bit lengths bit length count
    std::array<int, 20> dd_bl_count = {}; // distance bit length count

    std::array<int, 20> ll_next_codes = {}; // starting point of the next (to be encoded) code
    std::array<int, 20> dd_next_codes = {}; // starting point of the next (to be encoded) code

    Encoder(std::fstream *file) : file(file) {
        this->buf = {};
        this->buf_idx = 0;
        this->rest_bits = 0;
        this->rest_bits_count = 0;

        this->ll_lengths.resize(LL_ALPHABET);
        this->dd_lengths.resize(DISTANCE_ALPHABET);
    }

    ~Encoder() {
        this->flush_buf(true); // REDO THIS
    }

    void write_to_file() {}

    void reset() {
        for (int i = 0; i < LL_ALPHABET; i++)
            ll_lengths[i] = 0, ll_codes[i] = -1;

        for (int i = 0; i < DISTANCE_ALPHABET; i++)
            dd_lengths[i] = 0, dd_codes[i] = -1;

        for (int i = 0; i < 20; i++)
            ll_bl_count[i] = 0, dd_bl_count[i] = 0,
            ll_next_codes[i] = 0, dd_next_codes[i] = 0;
    }

    void count_bl() {

        for (auto bl : this->ll_lengths)
            this->ll_bl_count[bl]++;

        for (auto bl : this->dd_lengths)
            this->dd_bl_count[bl]++;

        ll_bl_count[0] = 0, dd_bl_count[0] = 0;
    }

    void next_codes() {
        int ll_code = 0, dd_code = 0;
        for (int bits = 1; bits < 20; bits++) {
            ll_code = (ll_code + ll_bl_count[bits - 1]) << 1;
            ll_next_codes[bits] = ll_code;

            dd_code = (dd_code + dd_bl_count[bits - 1]) << 1;
            dd_next_codes[bits] = dd_code;
        }
    }

    void generate_codes() {
        int len;
        for (int n = 0; n < LL_ALPHABET; n++) {
            len = ll_lengths.at(n);
            if (len != 0) {
                ll_codes.at(n) = ll_next_codes.at(len);
                ll_next_codes.at(len)++;
            }
        }

        for (int n = 0; n < DISTANCE_ALPHABET; n++) {
            len = dd_lengths.at(n);
            if (len != 0) {
                dd_codes.at(n) = dd_next_codes.at(len);
                dd_next_codes.at(len)++;
            }
        }
    }

    void add_times_0(int times) {
        if (times >= 3 && times <= 10) {
            add_bits(COPY_0_3_10, ALPHABET_BITS);
            add_bits(times - MIN_0_3_10, BITS_0_3_10);
        } else if (times >= 11 && times <= 138) {
            add_bits(COPY_0_11_138, ALPHABET_BITS);
            add_bits(times - MIN_0_11_138, BITS_0_11_138);
        } else if (times > 138) {
            add_bits(COPY_0_11_138, ALPHABET_BITS);
            add_bits(138 - MIN_0_11_138, BITS_0_11_138);
            times -= 138;
            add_times_0(times);
        } else {
            assert(times < 3);
            for (int k = 0; k < times; k++)
                add_bits(0, ALPHABET_BITS);
        }
    }

    void add_times_x(int times, int bit_length) {
        if (times >= 3 && times <= 6) {
            add_bits(COPY_X_3_6, ALPHABET_BITS);
            add_bits(times - MIN_X_3_6, BITS_X_3_6);
        } else if (times > 6) {
            add_bits(COPY_X_3_6, ALPHABET_BITS);
            add_bits(6 - MIN_X_3_6, BITS_X_3_6);
            times -= 6;
            add_times_x(times, bit_length);
        } else {
            assert(times < 3);
            if (times > 0) {
                for (int k = 0; k < times; k++)
                    add_bits(bit_length, ALPHABET_BITS);
            }
        }
    }

    void sum_0_bits(int times, int *sum) {
        if (times >= 3 && times <= 10) {
            (*sum) += ALPHABET_BITS;
            (*sum) += BITS_0_3_10;
        } else if (times >= 11 && times <= 138) {
            (*sum) += ALPHABET_BITS;
            (*sum) += BITS_0_11_138;
        } else if (times > 138) {
            (*sum) += ALPHABET_BITS;
            (*sum) += BITS_0_11_138;
            times -= 138;
            sum_0_bits(times, sum);
        } else {
            assert(times < 3);
            for (int k = 0; k < times; k++)
                (*sum) += ALPHABET_BITS;
        }
    }

    void sum_x_bits(int times, int *sum) {
        if (times >= 3 && times <= 6) {
            (*sum) += ALPHABET_BITS;
            (*sum) += BITS_X_3_6;
        } else if (times > 6) {
            (*sum) += ALPHABET_BITS;
            (*sum) += BITS_X_3_6;
            times -= 6;
            sum_x_bits(times, sum);
        } else {
            assert(times < 3);
            if (times > 0) {
                for (int k = 0; k < times; k++)
                    (*sum) += ALPHABET_BITS;
            }
        }
    }

    int sum_alphabet_bits() {
        int bit_length;
        int j;
        int times;
        int sum = 0;

        for (int i = 0; i < LL_ALPHABET;) {
            bit_length = this->ll_lengths[i];

            if (bit_length == 0) {
                times = 0;
                for (j = i; j < LL_ALPHABET; j++) {
                    if (this->ll_lengths[j] != 0) break;
                    times++;
                }

                sum_0_bits(times, &sum);

                i = j == i ? i + 1 : j;
            } else {

                sum += ALPHABET_BITS;

                times = 0;
                for (j = i + 1; j < LL_ALPHABET; j++) {
                    if (this->ll_lengths[j] != bit_length) break;
                    times++;
                }

                sum_x_bits(times, &sum);

                i = j;
            }
        }

        for (int i = 0; i < DISTANCE_ALPHABET;) {
            bit_length = this->dd_lengths[i];

            if (bit_length == 0) {
                times = 0;
                for (j = i; j < DISTANCE_ALPHABET; j++) {
                    if (this->dd_lengths[j] != 0) break;
                    times++;
                }

                sum_0_bits(times, &sum);

                i = j == i ? i + 1 : j;
            } else {

                sum += ALPHABET_BITS;

                times = 0;
                for (j = i + 1; j < DISTANCE_ALPHABET; j++) {
                    if (this->dd_lengths[j] != bit_length) break;
                    times++;
                }

                sum_x_bits(times, &sum);

                i = j;
            }
        }

        return sum;
    }

    void encode_lengths() {

        // add_bits(0b10, 2);  // TODO

        int bit_length;
        int j;
        int times;
        for (int i = 0; i < LL_ALPHABET;) {
            bit_length = this->ll_lengths[i];

            if (bit_length == 0) {
                times = 0;
                for (j = i; j < LL_ALPHABET; j++) {
                    if (this->ll_lengths[j] != 0) break;
                    times++;
                }

                add_times_0(times);

                i = j == i ? i + 1 : j;
            } else {

                add_bits(bit_length, 5);

                times = 0;
                for (j = i + 1; j < LL_ALPHABET; j++) {
                    if (this->ll_lengths[j] != bit_length) break;
                    times++;
                }

                add_times_x(times, bit_length);

                i = j;
            }
        }

        for (int i = 0; i < DISTANCE_ALPHABET;) {
            bit_length = this->dd_lengths[i];

            if (bit_length == 0) {
                times = 0;
                for (j = i; j < DISTANCE_ALPHABET; j++) {
                    if (this->dd_lengths[j] != 0) break;
                    times++;
                }

                add_times_0(times);

                i = j == i ? i + 1 : j;
            } else {

                add_bits(bit_length, 5);

                times = 0;
                for (j = i + 1; j < DISTANCE_ALPHABET; j++) {
                    if (this->dd_lengths[j] != bit_length) break;
                    times++;
                }

                add_times_x(times, bit_length);

                i = j;
            }
        }
    }

    int worth_compress(int size, std::vector<uint8_t> *prev_block, Records *records, Alphabet *alphabet) {

        int prev_block_size = prev_block == NULL ? 0 : prev_block->size();
        int total_bits = 0;
        int total_bytes;

        record *r;
        int distance, length, ld_code;
        int bit_length;

        int dif, extra_bits;

        for (int i = 0; i < records->size(); i++) {
            r = records->at(i);
            if (r == NULL) break;
            assert(r->where != r->start);
            if (r->where - r->start < 0) {
                assert(prev_block_size > 0);
                distance = (prev_block_size - r->start) + r->where;
                assert(distance <= MAX_DISTANCE);
            } else {
                distance = r->where - r->start;
            }
            length = r->length;

            assert(length <= 258 && length >= 3);
            assert(distance <= MAX_DISTANCE && distance >= 1);
            ld_code = alphabet->ll_map(length);
            bit_length = ll_lengths[ld_code];
            total_bits += bit_length;

            std::tie(dif, extra_bits) = alphabet->ll_get_extra_bits(length, ld_code);
            total_bits += extra_bits;

            ld_code = alphabet->dd_map(distance);
            bit_length = dd_lengths[ld_code];
            total_bits += bit_length;

            std::tie(dif, extra_bits) = alphabet->dd_get_extra_bits(distance, ld_code);
            total_bits += extra_bits;
        }

        total_bits += alphabet->ll_weight_sum(&this->ll_lengths);

        total_bits += sum_alphabet_bits();

        total_bytes = total_bits / 8;
        if (total_bits % 8 > 0) total_bytes += 1;

        // std::cout << "----------- TO COMPRESS ------------"
        //           << "\n";
        // std::cout << "TOTAL_BYTES: " << total_bytes << "\n";
        // std::cout << "ORIGINAL SIZE: " << size << "\n";

        return total_bytes < size ? 1 : -1;
    }

    void encode(std::vector<uint8_t> *src, int size, std::vector<uint8_t> *prev_block, Records *records, Alphabet *alphabet) {
        // std::cout << "\n------------ ENCODE START -------------"
        //           << "\n";
        // std::cout << "buf_idx: " << buf_idx << "\n";

        int to_compress = worth_compress(size, prev_block, records, alphabet);
        if (to_compress == 1) {
            add_bits(COMPRESSED, 2);
            encode_lengths();

            // std::cout << "IS COMPRESS"
            //           << "\n";
            int r_idx = 0;
            record *r = records->at(r_idx);
            int records_size = records->size();

            int prev_block_size = prev_block == NULL ? 0 : prev_block->size();

            int distance, length, ld_code;

            int new_code, bit_length;

            int dif, extra_bits;

            int src_size = src->size();

            for (int i = 0; i < size;) {

                if (r != NULL && r->where == i) {
                    assert(r->where != r->start);
                    if (r->where - r->start < 0) {
                        assert(prev_block_size > 0);
                        distance = (prev_block_size - r->start) + r->where;

                    } else {
                        distance = r->where - r->start;
                    }
                    length = r->length;
                    // std::cout << "length in encode: " << length << "\n";
                    assert(length <= 258 && length >= 3);
                    assert(distance <= MAX_DISTANCE && distance >= 1);

                    ld_code = alphabet->ll_map(length);
                    new_code = ll_codes[ld_code];
                    bit_length = ll_lengths[ld_code];

                    assert(new_code != -1 && bit_length != 0);
                    add_bits(new_code, bit_length);

                    std::tie(dif, extra_bits) = alphabet->ll_get_extra_bits(length, ld_code);
                    if (extra_bits > 0) {
                        add_bits(dif, extra_bits);
                    }

                    ld_code = alphabet->dd_map(distance);
                    new_code = dd_codes[ld_code];
                    bit_length = dd_lengths[ld_code];

                    assert(new_code != -1);
                    assert(bit_length != 0);
                    add_bits(new_code, bit_length);

                    std::tie(dif, extra_bits) = alphabet->dd_get_extra_bits(distance, ld_code);
                    if (extra_bits > 0) {
                        add_bits(dif, extra_bits);
                    }

                    i += r->length;
                    r_idx++;
                    r = r_idx < records_size ? records->at(r_idx) : NULL;

                } else {
                    assert(i < src_size);
                    new_code = ll_codes[src->at(i)];
                    bit_length = ll_lengths[src->at(i)];
                    assert(new_code != -1 && bit_length != 0);
                    add_bits(new_code, bit_length);
                    i++;
                }
            }

            new_code = ll_codes[256];
            bit_length = ll_lengths[256];
            assert(new_code != -1 && bit_length != 0);
            add_bits(new_code, bit_length);
        } else {
            std::cout << "NO COMPRESS"
                      << "\n";

            add_bits(NOT_COMPRESSED, 2);
            for (int i = 0; i < size; i++)
                add_bits(src->at(i), 8);
        }
    }

    void flush_buf(bool force) {

        // std::cout << ""
        if (force) {
            if (this->rest_bits_count > 0) {
                int full_bytes = this->rest_bits_count / 8;
                int rest = this->rest_bits_count % 8;
                for (int i = 0; i < full_bytes; i++) {
                    buf.at(buf_idx++) = this->rest_bits >> 24;
                    this->rest_bits <<= 8;
                    this->flush_buf(false);
                }

                if (rest > 0)
                    buf.at(buf_idx++) = this->rest_bits >> 24;
            }
            // append to the file all bytes in the buffer up to buf_idx;
            // this->__file.insert(this->__file.end(), this->buf.begin(), this->buf.begin() + this->buf_idx);
            this->file->write(reinterpret_cast<const char *>(&this->buf[0]), this->buf_idx);
            if (this->file->fail()) {
                std::cout << "FILE FAIL FLUSH BUF TRUE"
                          << "\n";
                exit(1);
            }
            this->buf_idx = 0;
        } else {
            if (this->buf_idx == UINT16_MAX) {
                // std::cout << this->file->tellp() << '\n';
                // append to the file all bytes in the buffer up to end;
                // this->__file.insert(this->__file.end(), this->buf.begin(), this->buf.end());
                this->file->write(reinterpret_cast<const char *>(&this->buf[0]), this->buf_idx);
                if (this->file->fail()) {
                    std::cout << "FILE FAIL FLUSH BUF FALSE"
                              << "\n";
                    exit(1);
                }
                this->buf_idx = 0;
            }
        }
    }

    void add_bits(uint32_t new_code, int bit_length) {
        // std::cout << "code to add: " << new_code << ", bit_length: " << bit_length << "\n";
        assert(rest_bits >= 0);
        assert(rest_bits <= UINT32_MAX);
        assert(rest_bits_count >= 0);
        assert(rest_bits_count < 32);

        int sum = this->rest_bits_count + bit_length;
        uint32_t combined = (this->rest_bits) | (new_code << (32 - bit_length - this->rest_bits_count));
        int full_bytes = sum / 8;
        int rest = sum % 8;

        for (int i = 0; i < full_bytes; i++) {
            buf.at(buf_idx++) = combined >> 24;
            combined <<= 8;
            flush_buf(false);
        }
        this->rest_bits_count = rest;
        this->rest_bits = combined;
    }

    void print(){/* not implemented */};

    void print_code_lengths() {
        std::cout << "\nLiteral\\Length BitLengths: \n";
        for (auto bl : this->ll_lengths)
            std::cout << bl << " ";
        std::cout << "\n";

        std::cout << "\nDistance BitLengths: ";
        for (auto bl : this->dd_lengths)
            std::cout << bl << " ";
        std::cout << "\n";
    }

    void print_next_codes() {
        std::cout << "\nLiteral\\Length NextCode: ";
        for (auto n_code : this->ll_next_codes)
            std::cout << n_code << " ";
        std::cout << "\n";

        std::cout << "\nDistance NextCode: ";
        for (auto n_code : this->dd_next_codes)
            std::cout << n_code << " ";
        std::cout << "\n";
    }

    void print_new_codes() {
        std::cout << "\nLiteral\\Length Codes: ";
        for (int i = 0; i < LL_ALPHABET; i++)
            std::cout << ll_codes[i] << " ";
        std::cout << "\n";

        std::cout << "\nDistance Codes: ";
        for (int i = 0; i < DISTANCE_ALPHABET; i++)
            std::cout << dd_codes[i] << " ";
        std::cout << "\n";
    }
};

#endif
