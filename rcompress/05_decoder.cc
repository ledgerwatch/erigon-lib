#include "05_decoder.h"

// ---------------- externs

Decoder *NewDecoder(uint64_t num_words, int num_blocks, unsigned char *compressed_dict, int dict_size, int max_word_size) {

    Decoder *decoder = new Decoder(num_words, num_blocks, compressed_dict, dict_size, max_word_size);

    return decoder;
}

void DeleteDecoder(Decoder *decoder) {
    delete decoder;
}

int PrepareNextBlock(Decoder *decoder, unsigned char *src, int src_size, int64_t offset) {

    return decoder->prepare_next_block(src, src_size, offset);
}

// int HasNext(Decoder *decoder) {
//     // if (decoder->has_next())
//     //     return 1;
//     // else
//     //     return 0;

//     return decoder->has_next() ? 1 : 0;
// }

// int64_t Next(Decoder *decoder, int64_t offset, signed short *dst) {
//     return decoder->next(offset, dst);
// }

// int Match(Decoder *decoder, unsigned char *prefix, int prefix_size) {
//     return decoder->match(prefix, prefix_size);
// }

int64_t NextAt(Decoder *decoder, int64_t offset, int block_num, short *dst) {
    return decoder->decode_at(offset, block_num, dst);
}

// ---------------- DECODER

Decoder::Decoder(uint64_t num_words, int num_blocks, unsigned char *compressed_dict, int dict_size, int max_word_size) {
    init_dict_dist_codes();
    this->num_words = num_words;
    this->num_blocks = num_blocks;
    this->current_block = 0;
    this->word_codes.reserve(max_word_size);

    this->dict = __decode_dict(compressed_dict, dict_size);
    this->block_decoders.reserve(num_blocks);
}

Decoder::~Decoder() {
    for (auto d : block_decoders)
        delete d;
}

int Decoder::prepare_next_block(unsigned char *src, int src_size, int64_t offset) {

    auto _d_data = new d_data(src, src_size);
    _d_data->offset = offset;

    _d_data->restore_prefixes();

    block_decoders.push_back(_d_data);

    return _d_data->word_start + _d_data->offset;
}

// bool Decoder::has_next() {

//     if (current_block < num_blocks - 1) return true;

//     if (current_block + 1 == num_blocks) {

//         auto decoder = block_decoders.at(current_block);

//         return decoder->next_start == decoder->src_size ? false : true;
//     }

//     return false;
// }

// int64_t Decoder::next(int64_t offset, signed short *dst) {

//     if (current_block >= num_blocks)
//         return -1;

//     int64_t next_word_offset;
//     d_data *decoder;
//     d_data *next_decoder = nullptr;

//     // for (int i = 0; i < (int)block_decoders.size(); i++) {
//     //     auto _d_data = block_decoders.at(i);

//     //     if (_d_data->offset > offset) {
//     //         current_block = i - 1;
//     //         break;
//     //     }
//     // }

//     decoder = block_decoders.at(current_block);
//     if (current_block < (int)block_decoders.size()) {
//         next_decoder = block_decoders.at(current_block + 1);
//     }

//     // // decoder->next_start =

//     // auto d_offset = decoder->offset + decoder->next_start;
//     // std::cout << "OFFSET: " << offset << "\n";
//     // std::cout << "DECODER OFFSET: " << decoder->offset << "\n";
//     // std::cout << "DECODER_NEXT START: " << decoder->next_start << "\n";
//     // std::cout << "ALL TOGETHER: " << d_offset << "\n";

//     // assert(offset == decoder->offset + decoder->next_start);

//     if (decoder->next(&word_codes)) {
//         int dst_idx = 0;
//         int match_code, match_diff, match_len, match_idx;
//         for (int i = 0; i < (int)word_codes.size();) {
//             int code = word_codes.at(i);

//             if (code > R_FLAG_EOW) {
//                 assert(code < R_MAX_ALPH_SIZE);
//                 int xbits = match_len_xbits[code - 257];

//                 if (xbits > 0) {
//                     int diff = word_codes.at(i + 1);
//                     match_len = match_len_mins[code - 257] + diff;
//                     match_code = word_codes.at(i + 2);

//                     match_diff = word_codes.at(i + 3);

//                     i += 4;
//                     match_idx = prefix_id_mins[match_code] + match_diff;

//                 } else {
//                     match_len = match_len_mins[code - 257];
//                     match_code = word_codes.at(i + 1);

//                     match_diff = word_codes.at(i + 2);

//                     i += 3;
//                     match_idx = prefix_id_mins[match_code] + match_diff;
//                 }

//                 assert(match_len <= 255);
//                 for (int q = 0; q < match_len; q++) {
//                     dst[dst_idx++] = dict.at(match_idx).at(q);
//                 }

//             } else {
//                 dst[dst_idx++] = (unsigned char)code;
//                 i++;
//             }
//         }

//         dst[dst_idx] = -1; // eow

//         next_word_offset = decoder->d_offset + decoder->next_start;

//         if (next_decoder != nullptr) {
//             if (next_word_offset == next_decoder->s_offset) {
//                 return next_decoder->d_offset;
//             }
//         }

//         return next_word_offset;
//     } else {
//         if (current_block < num_blocks) {
//             current_block++;
//             return this->next(offset, dst);
//         }

//         return -1;
//     }
//     return 0;
// }

// int Decoder::match(unsigned char *prefix, int prefix_size) {

//     if (current_block >= num_blocks)
//         return 0;

//     auto decoder = block_decoders.at(current_block);

//     std::vector<uint8_t> decoded_part(prefix_size, 0);

//     bool matched = 1;

//     if (decoder->match(&word_codes)) {
//         int dst_idx = 0;
//         int match_code, match_diff, match_len, match_idx;
//         for (int i = 0; i < (int)word_codes.size() && dst_idx < prefix_size;) {
//             int code = word_codes.at(i);

//             if (code > R_FLAG_EOW) {
//                 assert(code < R_MAX_ALPH_SIZE);
//                 int xbits = match_len_xbits[code - 257];

//                 if (xbits > 0) {
//                     int diff = word_codes.at(i + 1);
//                     match_len = match_len_mins[code - 257] + diff;
//                     match_code = word_codes.at(i + 2);

//                     match_diff = word_codes.at(i + 3);

//                     i += 4;
//                     match_idx = prefix_id_mins[match_code] + match_diff;

//                 } else {
//                     match_len = match_len_mins[code - 257];
//                     match_code = word_codes.at(i + 1);

//                     match_diff = word_codes.at(i + 2);

//                     i += 3;
//                     match_idx = prefix_id_mins[match_code] + match_diff;
//                 }

//                 assert(match_len <= 255);
//                 for (int q = 0; q < match_len && dst_idx < prefix_size; q++) {
//                     decoded_part.at(dst_idx++) = dict.at(match_idx).at(q);
//                 }

//             } else {
//                 decoded_part.at(dst_idx++) = (unsigned char)code;
//                 i++;
//             }
//         }

//     } else {
//         if (current_block < num_blocks) {
//             current_block++;
//             return this->match(prefix, prefix_size);
//         }
//     }

//     for (int i = 0; i < prefix_size; i++) {
//         if (decoded_part.at(i) != prefix[i]) {
//             matched = 0;
//             break;
//         }
//     }

//     return matched;
// }

int64_t Decoder::decode_at(int64_t offset, int block_num, short *dst) {

    assert(block_num < (int)block_decoders.size());

    auto decoder = block_decoders.at(block_num);

    if (offset > decoder->word_start)
        decoder->next_start = offset - decoder->offset;
    if (offset < decoder->word_start || decoder->next_start == 0)
        decoder->next_start = decoder->word_start;

    if (decoder->next(&word_codes)) {
        int dst_idx = 0;
        int match_code, match_diff, match_len, match_idx;
        for (int i = 0; i < (int)word_codes.size();) {
            int code = word_codes.at(i);

            if (code > R_FLAG_EOW) {
                assert(code < R_MAX_ALPH_SIZE);
                int xbits = match_len_xbits[code - 257];

                if (xbits > 0) {
                    int diff = word_codes.at(i + 1);
                    match_len = match_len_mins[code - 257] + diff;
                    match_code = word_codes.at(i + 2);

                    match_diff = word_codes.at(i + 3);

                    i += 4;
                    match_idx = prefix_id_mins[match_code] + match_diff;

                } else {
                    match_len = match_len_mins[code - 257];
                    match_code = word_codes.at(i + 1);

                    match_diff = word_codes.at(i + 2);

                    i += 3;
                    match_idx = prefix_id_mins[match_code] + match_diff;
                }

                assert(match_len <= 255);
                for (int q = 0; q < match_len; q++) {
                    dst[dst_idx++] = dict.at(match_idx).at(q);
                }

            } else {
                dst[dst_idx++] = (unsigned char)code;
                i++;
            }
        }

        dst[dst_idx++] = -1;

        return decoder->offset + decoder->next_start;
    } else {

        std::cout << "GOT HERE\n";
        return -1;
    }
}