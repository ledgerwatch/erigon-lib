#ifndef CCOMPRESS_DECOMPRESSOR_
#define CCOMPRESS_DECOMPRESSOR_

#ifdef __cplusplus
#include "cross_file_map.h"
#include "decoder.h"
#include "defs.h"

#include <array>
#include <deque>
#include <fstream>
#include <string>
#include <vector>

class CDecompressor {
private:
    Decoder *decoder;

    // unsigned char *mmap_data; // compressed file
    // size_t data_size;         // size of the compressed file

    // std::fstream compressed_file;

    m_file *file_data;

public:
    CDecompressor(const char *file_name);
    ~CDecompressor();

    std::array<uint8_t, UINT16_MAX> block;

    std::vector<uint8_t> decode();
    int decode_words();
    std::vector<uint8_t> next();
    bool has_next();

    std::deque<std::vector<uint8_t>> dst;

    // what is left from previous decode cycle
    // since decoded blocks are 64kb at most, some words does not fit in single block
    // or single word does not fit in a single block
    // e.g1: if one word takes 73kb, we have to decode 2 blocks to get that word
    //      so 64kb*2 - 73kb = prev_left
    // e.g2: if we have 4 words, all about 20kb size
    //      so 3 words fit in a single block and the 4th word partialy fits into the block
    //      and part of it in another block
    //      so we decode full block and take only 3 words from it
    //      whatever left from 3 words goes to prev_left
    std::vector<uint8_t> prev_left;

    std::vector<std::vector<uint8_t>> blocks; // used for testing

    int total_words;  // total words in compressed file
    int total_blocks; // total blocks in compressed file

    int three_blocks_count;
    int rest_blocks;
    int blocks_decoded;

    int words_decoded;
    int words_returned;

    size_t f_size();
    void reset_hard();
};

#else
typedef struct CDecompressor CDecompressor;
#endif //  __cplusplus

#ifdef __cplusplus
extern "C" {
#endif

#if defined(__STDC__) || defined(__cplusplus)
extern CDecompressor *CNewDecompressor(const char *file_name);
extern void CCloseDecompressor(CDecompressor *dcmp);
extern int CNext(CDecompressor *dcmp, unsigned char *dst);
extern int CHasNext(CDecompressor *dcmp);
extern int CSkip(CDecompressor *dcmp);
extern int CMatch(CDecompressor *dcmp, unsigned char *word, int size);
extern int CMatchPrefix(CDecompressor *dcmp, unsigned char *prefix, int size);
extern size_t CSize(CDecompressor *dcmp);
extern void CReset(CDecompressor *dcmp);
#else
extern CCompressor *CNewCompressor(const char *file_name);
extern void CCloseDecompressor(CDecompressor *dcmp);
extern int CNext(CDecompressor *dcmp, unsigned char *dst);
extern int CHasNext(CDecompressor *dcmp);
extern int CSkip(CDecompressor *dcmp);
extern int CMatch(CDecompressor *dcmp, unsigned char *word, int size);
extern int CMatchPrefix(CDecompressor *dcmp, unsigned char *prefix, int size);
extern size_t CSize(CDecompressor *dcmp);
extern void CReset(CDecompressor *dcmp);
#endif

#ifdef __cplusplus
}
#endif

#endif