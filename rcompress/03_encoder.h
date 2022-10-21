#ifndef R_ENCODER
#define R_ENCODER

#ifdef __cplusplus

#include "02_dict.h"

#include "04_encoding_assets.h"

#include <array>
#include <cassert>
#include <iostream>
#include <tuple>
#include <vector>

class Encoder {
private:
    e_data *lits_and_matches;
    Dict *dict;
    bit_writer *_bit_writer;

    // int dst_idx;

    int total_bytes;
    int total_dict_ref;
    int estim_compressed;

    // uint32_t rest;
    // uint8_t rest_bits; // how many "busy" bits is in rest at the moment

public:
    unsigned char *dst;

public:
    Encoder(Dict *dict);
    ~Encoder();

private:
    // void add_bits(uint16_t prefix, uint8_t bit_len);
    // void add_times_0(int times);
    // void add_times_x(int times, int bit_length);
    // // void write(uint8_t _byte);
    // void write_header(uint32_t header);

public:
    int encode_dict(unsigned char *dst);
    int encode_block(unsigned char *data, int data_size, int *sizes, int sizes_size, int *preCompressed, int preCompressed_size, unsigned char *dst);
    // void encode_alphabet(std::vector<std::tuple<uint16_t, uint8_t>> *prefixes);
    void reset();
    // void flush();
    // void write(uint8_t _byte);
};

#else
typedef struct Encoder Encoder;
#endif // __cplusplus

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

#if defined(__STDC__) || defined(__cplusplus)
extern Encoder *NewEncoder(Dict *dict);
extern void DeleteEncoder(Encoder *enc);
extern int EncodeBlock(Encoder *enc, unsigned char *data, int data_size, int *sizes, int sizes_size, int *preCompressed, int preCompressed_size, unsigned char *dst);
extern int EncodeDict(Encoder *enc, unsigned char *dst);
#else
extern Encoder *NewEncoder(Dict *dict);
extern void DeleteEncoder(Encoder *enc);
extern int EncodeBlock(Encoder *enc, unsigned char *data, int data_size, int *sizes, int sizes_size, int *preCompressed, int preCompressed_size, unsigned char *dst);
extern int EncodeDict(Encoder *enc, unsigned char *dst);
#endif

#ifdef __cplusplus
}
#endif // __cplusplus

#endif