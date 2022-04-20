#include "compressor.h"
#include "alphabet.h"
#include "defs.h"
#include "dict.h"
#include "encoder.h"
#include "timing.h"
#include "tree.h"

#include <iostream>
#include <string>
#include <vector>

CCompressor *CNewCompressor(const char *out_file) {
    CCompressor *cmp = new CCompressor(out_file);
    return cmp;
}

void CCloseCompressor(CCompressor *cmp) {
    delete cmp;
}

void CAddWord(CCompressor *cmp, unsigned char *word, int size) {
    cmp->add_word(word, size);
}

void CCompress(CCompressor *cmp) {
    cmp->flush_buf();
}

CCompressor::CCompressor(const char *out_file) {

    this->file.open(out_file, std::fstream::binary | std::fstream::trunc | std::fstream::out);
    if (!this->file.is_open()) {
        std::cout << "Failed to open file: " << out_file << "\n";
        exit(1);
    }

    this->file.seekp(24, std::ios_base::beg); // leave space for 24 byte header
    if (this->file.fail()) {
        std::cout << "Failed to seek write position: "
                  << "this->file.seekp(24, std::ios_base::beg)"
                  << "\n";
        exit(1);
    }
    this->alphabet = new Alphabet();
    this->dict = new Dict();
    this->records = new Records();
    this->encoder = new Encoder(&this->file);

    this->seed = this->rng.rand_odd_32();

    this->total_words = 0;
    this->total_blocks = 0;

    this->block = {};
    this->block_idx = 0;

    this->curr_block.reserve(UINT16_MAX);

    this->prev_block.reserve(UINT16_MAX);
}

CCompressor::~CCompressor() {

    delete this->alphabet;
    delete this->dict;
    delete this->records;
    delete this->encoder;

    this->file.close();
}

void CCompressor::check_compress() {
    if (this->block_idx == UINT16_MAX) {
        this->compress();
        this->prev_block.clear();
        for (int i = 0; i < UINT16_MAX; i++)
            prev_block.push_back(block.at(i));
        assert(prev_block.size() == UINT16_MAX);
        // prev_block.at(i) = block.at(i);
        this->block_idx = 0;
    }
}

void CCompressor::flush_buf() {
    if (this->block_idx > 0) {
        this->compress();
        this->encoder->flush_buf(true);
        this->block_idx = 0;
    }

    this->file.seekp(0, std::ios_base::beg);
    if (this->file.fail()) {
        std::cout << "Failed to seek write position: "
                  << "this->file.seekp(0, std::ios_base::beg)"
                  << "\n";
        exit(1);
    }

    unsigned char header[24];

    int header_idx = 0;
    // std::cout << "COMPRESSOR Total_words: " << this->total_words << "\n";
    header[header_idx++] = this->total_words >> 24;
    header[header_idx++] = this->total_words >> 16;
    header[header_idx++] = this->total_words >> 8;
    header[header_idx++] = this->total_words & 0x000000FF;
    // std::cout << "COMPRESSOR Total_blocks: " << this->total_blocks << "\n";
    header[header_idx++] = this->total_blocks >> 24;
    header[header_idx++] = this->total_blocks >> 16;
    header[header_idx++] = this->total_blocks >> 8;
    header[header_idx++] = this->total_blocks & 0x000000FF;

    this->file.write((const char *)header, 23);
    if (this->file.fail()) {
        std::cout << "FILE FAIL FLUSH BUF COMPRESS"
                  << "\n";
        exit(1);
    }
    this->file.flush();
    // this->file.close();
}

void CCompressor::add_word(unsigned char *word, int word_size) {

    assert(word_size <= (0x00FFFFFF));

    int first, second, third;

    first = (word_size >> 16);
    second = (word_size >> 8);
    third = word_size & 0x000000FF;

    this->block.at(this->block_idx++) = first;
    this->check_compress();

    this->block.at(this->block_idx++) = second;
    this->check_compress();

    this->block.at(this->block_idx++) = third;
    this->check_compress();

    u_int8_t byte;
    for (int i = 0; i < word_size; i++) {
        byte = word[i];
        this->block.at(this->block_idx++) = byte;
        this->check_compress();
    }

    this->total_words++;
}

void CCompressor::compress() {

    // auto start = timing::time_now();

    this->alphabet->reset();
    this->records->reset();
    this->dict->reset();
    this->encoder->reset();

    this->curr_block.clear();
    this->curr_block.insert(this->curr_block.begin(), this->block.begin(), this->block.begin() + this->block_idx);
    int size = this->curr_block.size();

    this->blocks.push_back(this->curr_block);

    std::vector<uint8_t> *pb = this->prev_block.size() > 0 ? &this->prev_block : NULL;

    // create records of back references to repeated sequences
    // so it could be encoded during encoding process
    this->create_records();

    // cout frequency of each byte in a src
    // excluding records
    this->count_freq();

    // compute required amount of bits (bit_lengths) for each byte based on frequency
    this->compute_lengths();

    // count how many times each bit_length occurs
    // e.g: 7-bit codes occur L times
    //      9-bit codes occur M times
    //      8-bit codes occur N times
    this->encoder->count_bl();

    // generate starting point at which first code with N bits starts
    // e.g: A, B, C all take 3 bit length
    // the starting point could be 000
    this->encoder->next_codes();

    // generate codes for each bit_length
    // e.g: A, B, C all take 3 bit length
    // so, A -> 000, B -> 001, C -> 010;
    this->encoder->generate_codes();

    // this->print_code_lengths();
    // this->print_new_codes();

    // rewrite src based on newly generated codes including back references
    // if it makes sense, if not leave as it is
    this->encoder->encode(&this->curr_block, size, pb, this->records, this->alphabet);

    this->total_blocks++;

    // auto stop = timing::time_now();

    // timing::duration(start, stop, "COMPRESS");
    // std::cout << "================================ COMPRESS DONE"
    //           << "\n";
}

void CCompressor::create_records() {

    int a, b, c, d;
    int h; // hash
    // int count = 0;
    std::vector<uint8_t> *pb = this->prev_block.size() > 0 ? &this->prev_block : NULL;
    std::vector<uint8_t> *src = &this->curr_block;
    int size = src->size();

    for (int i = 0; i < size - 3;) {
        assert(i < size - 3);
        assert(i + 2 < size);
        a = src->at(i), b = src->at(i + 1), c = src->at(i + 2);
        d = (a << 16) | (b << 8) | c;

        h = HASH_FUNC(this->seed, d);

        record *r = dict->match_longest(src, pb, h, i, d);
        if (r != NULL) {
            this->dict->insert(h, i);

            assert(r->start != r->where);
            // std::cout << "a: " << a << ", b: " << b << ", c: " << c << "\n";
            // std::cout << "start: " << r->start << ", where: " << r->where << ", length: " << r->length << "\n";
            this->records->push_back(r);
            // count++;
            i += r->length;
        } else {
            this->dict->insert(h, i);
            i++;
        }
    }

    // std::cout << "records count: " << count << "\n";
}

void CCompressor::count_freq() {

    std::vector<uint8_t> *src = &this->curr_block;
    int size = src->size();
    int prev_block_size = this->prev_block.size();
    this->records->push_back(NULL);

    int distance, r_idx = 0;
    record *r = records->at(r_idx);

    for (int i = 0; i < size;) {
        assert(i < size);
        if (r != NULL && r->where == i) {
            // repetition encounter
            // we need to count length and a distance
            // distance = (r->where - r-start)
            // distance goes to separate alphabet

            if (r->where - r->start < 0) {
                assert(prev_block_size > 0);

                distance = (prev_block_size - r->start) + r->where;

                assert(distance <= MAX_DISTANCE);
                assert(prev_block_size > distance);
                assert(prev_block_size + (r->where - distance) == r->start);
            } else {
                distance = r->where - r->start;
            }

            this->alphabet->add_length(r->length);
            this->alphabet->add_distance(distance);

            i += r->length;
            r_idx++;
            r = this->records->at(r_idx);
        } else {
            this->alphabet->add_literal(src->at(i));
            i++;
        }
    }
    this->alphabet->add_literal(256); // End of block
}

void CCompressor::compute_lengths() {

    std::vector<std::unique_ptr<__tree::huff_node>> ll_nodes;
    std::vector<std::unique_ptr<__tree::huff_node>> dd_nodes;
    ll_nodes.reserve(LL_ALPHABET);
    dd_nodes.reserve(DISTANCE_ALPHABET);

    this->alphabet->to_ll_nodes(&ll_nodes);
    this->alphabet->to_dd_nodes(&dd_nodes);

    std::unique_ptr<__tree::huff_node> ll_root;
    std::unique_ptr<__tree::huff_node> dd_root;

    int ll_bit_length = 0, dd_bit_length = 0;

    if (ll_nodes.size() == 1) ll_bit_length = 1;
    if (dd_nodes.size() == 1) dd_bit_length = 1;

    if (ll_nodes.size() > 0)
        ll_root = build_tree(std::move(ll_nodes));

    if (dd_nodes.size() > 0)
        dd_root = build_tree(std::move(dd_nodes));

    if (ll_root != nullptr)
        __tree::dfs(ll_root, &ll_bit_length, &this->encoder->ll_lengths);

    if (dd_root != nullptr)
        __tree::dfs(dd_root, &dd_bit_length, &this->encoder->dd_lengths);
}

int CCompressor::records_size() {
    return this->records->size();
}

void CCompressor::print_new_codes() {
    this->encoder->print_new_codes();
}

void CCompressor::print_code_lengths() {
    this->encoder->print_code_lengths();
}

void CCompressor::print_next_codes() {
    this->encoder->print_next_codes();
}