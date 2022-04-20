#ifndef COMPRESSOR_H_
#define COMPRESSOR_H_

#ifdef __cplusplus
#include "alphabet.h"
#include "decoder.h"
#include "dict.h"
#include "encoder.h"
#include "rand.h"

#include <array>
#include <fstream>
#include <vector>

class CCompressor {
private:
    Dict *dict;
    Alphabet *alphabet;
    Records *records;
    Encoder *encoder;

    Rand rng;

    uint32_t seed;

    std::array<uint8_t, UINT16_MAX> block;
    int block_idx;

    std::vector<uint8_t> curr_block;
    std::vector<uint8_t> prev_block;

    std::fstream file;

    void create_records();
    void count_freq();
    void compute_lengths();

    void check_compress();

public:
    CCompressor(const char *out_file);
    ~CCompressor();

    int prev_block_count;
    int total_words;
    int total_blocks;

    std::vector<std::vector<uint8_t>> blocks;

    void add_word(unsigned char *word, int word_size);
    void compress();
    void flush_buf();
    int records_size();
    void print_code_lengths();
    void print_next_codes();
    void print_new_codes();
};

#else
typedef struct CCompressor CCompressor;
#endif

#ifdef __cplusplus
extern "C" {
#endif

#if defined(__STDC__) || defined(__cplusplus)
extern CCompressor *CNewCompressor(const char *out_file);
extern void CCloseCompressor(CCompressor *cmp);
extern void CAddWord(CCompressor *cmp, unsigned char *word, int size);
extern void CCompress(CCompressor *cmp);
#else
extern CCompressor *CNewCompressor(const char *out_file);
extern void CCloseCompressor(CCompressor *cmp);
extern void CAddWord(CCompressor *cmp, unsigned char *word, int size);
extern void CCompress(CCompressor *cmp);
#endif

#ifdef __cplusplus
}
#endif

#endif // COMPRESSOR_H_