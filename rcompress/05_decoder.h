#ifndef R_DECODER
#define R_DECODER

#include <stdint.h>

#ifdef __cplusplus

#include "04_encoding_assets.h"

#include <bitset>
#include <cassert>
#include <iostream>
#include <tuple>
#include <vector>

class Decoder {
private:
    uint64_t num_words;
    int num_blocks;
    int current_block;

    std::vector<std::vector<uint8_t>> dict;
    std::vector<d_data *> block_decoders;

    std::vector<int> word_codes;

public:
    Decoder(uint64_t num_words, int num_blocks, unsigned char *compressed_dict, int cmp_dict_size, int max_word_size);
    ~Decoder();

    int prepare_next_block(unsigned char *src, int src_size, int64_t offset);

    // bool has_next();
    // int64_t next(int64_t offset, short *dst);
    // int match(unsigned char *prefix, int prefix_size);
    int64_t decode_at(int64_t offset, int block_num, short *dst);
};

#else
typedef struct Decoder Decoder;
#endif // __cplusplus

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

#if defined(__STDC__) || defined(__cplusplus)
extern Decoder *NewDecoder(uint64_t num_words, int num_blocks, unsigned char *compressed_dict, int cmp_dict_size, int max_word_size);
extern void DeleteDecoder(Decoder *decoder);

extern int PrepareNextBlock(Decoder *decoder, unsigned char *src, int src_size, int64_t offset);

// extern int HasNext(Decoder *decoder);
// extern int64_t Next(Decoder *decoder, int64_t offset, short *dst);
// extern int Match(Decoder *decoder, unsigned char *prefix, int prefix_size);

extern int64_t NextAt(Decoder *decoder, int64_t offset, int block_num, short *dst);
#else
extern Decoder *NewDecoder(uint64_t num_words, int num_blocks, unsigned char *compressed_dict, int cmp_dict_size, int max_word_size);
extern void DeleteDecoder(Decoder *decoder);

extern int PrepareNextBlock(Decoder *decoder, unsigned char *src, int src_size, int64_t s_offset, int64_t d_offset);

// extern int HasNext(Decoder *decoder);
// extern int64_t Next(Decoder *decoder, int64_t offset, short *dst);
// extern int Match(Decoder *decoder, unsigned char *prefix, int prefix_size);

extern int64_t NextAt(Decoder *decoder, int64_t offset, int block_num, short *dst);
#endif

#ifdef __cplusplus
}
#endif // __cplusplus

#endif