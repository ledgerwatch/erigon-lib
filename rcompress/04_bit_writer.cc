#include "04_encoding_assets.h"

// void bit_writer::write_header(uint32_t header) {
//     dst[0] = header >> 24;
//     dst[1] = (header & 0x00FF0000) >> 16;
//     dst[2] = (header & 0x0000FF00) >> 8;
//     dst[3] = (header & 0x000000FF);
// }

bit_writer::bit_writer(unsigned char *dst) {
    this->dst = dst;
    dst_idx = 1;

    rest = 0;
    rest_bits = 0;
}
bit_writer::~bit_writer() {
}

void bit_writer::reset() {
    rest = 0;
    rest_bits = 0;

    dst_idx = 1;
}

void bit_writer::write(uint8_t _byte) {
    dst[dst_idx++] = _byte;
}

void bit_writer::flush() {
    int full_bytes = rest_bits / 8;
    int _rest_bits = rest_bits % 8;

    for (int i = 0; i < full_bytes; i++) {
        write(rest >> 24);
        rest <<= 8;
    }

    if (_rest_bits > 0)
        write(rest >> 24);

    rest = 0;
    rest_bits = 0;
}

void bit_writer::add_bits(uint16_t prefix, uint8_t bit_len) {

    assert(rest >= 0);
    assert(rest_bits >= 0);
    assert(rest_bits < 8);
    assert(bit_len > 0);

    int bit_len_sum = rest_bits + bit_len;
    uint32_t combined = (rest) | (prefix << (32 - bit_len - rest_bits));
    int full_bytes = bit_len_sum / 8;
    int _rest_bits = bit_len_sum % 8;

    for (int i = 0; i < full_bytes; i++) {
        // encoded_alphabet.push_back(combined >> 24);
        write(combined >> 24);
        combined <<= 8;
    }

    rest_bits = _rest_bits;
    rest = combined;
}

void bit_writer::add_times_0(int times) {
    if (times >= 3 && times <= 10) {
        add_bits(R_REPEAT_0_3, 5);
        add_bits(times - 3, 3);
    } else if (times >= 11 && times <= 138) {
        add_bits(R_REPEAT_0_11, 5);
        add_bits(times - 11, 7);
    } else if (times > 138) {
        add_bits(R_REPEAT_0_11, 5);
        add_bits(138 - 11, 7);
        times -= 138;
        add_times_0(times);
    } else {
        assert(times < 3);
        for (int k = 0; k < times; k++)
            add_bits(0, 5);
    }
}
void bit_writer::add_times_x(int times, int bit_length) {
    if (times >= 3 && times <= 6) {
        add_bits(R_COPY_PREV, 5);
        add_bits(times - 3, 2);
    } else if (times > 6) {
        add_bits(R_COPY_PREV, 5);
        add_bits(6 - 3, 2);
        times -= 6;
        add_times_x(times, bit_length);
    } else {
        assert(times < 3);
        if (times > 0) {
            for (int k = 0; k < times; k++)
                add_bits(bit_length, 5);
        }
    }
}

void bit_writer::encode_alphabet(std::vector<std::tuple<uint16_t, uint8_t>> *prefixes) {

    int size = prefixes->size();
    assert(size >= 2);

    int bl, j, times;

    for (int i = 0; i < size;) {
        bl = std::get<1>(prefixes->at(i));

        if (bl == 0) {
            times = 1;
            for (j = i + 1; j < size; j++) {
                if (std::get<1>(prefixes->at(j)) != 0) break;
                times++;
            }

            add_times_0(times);
            i = j;
        } else {

            add_bits(bl, 5);

            times = 0;
            for (j = i + 1; j < size; j++) {
                if (std::get<1>(prefixes->at(j)) != bl) break;
                times++;
            }

            add_times_x(times, bl);

            i = j;
        }
    }

    flush();
}