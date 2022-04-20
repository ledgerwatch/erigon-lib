#include "decompressor.h"
#include "defs.h"
#include "timing.h"

CDecompressor *CNewDecompressor(const char *file_name) {
    return new CDecompressor(file_name);
}
void CCloseDecompressor(CDecompressor *dcmp) {
    delete dcmp;
}

int CNext(CDecompressor *dcmp, unsigned char *dst) {
    if (dcmp->has_next()) {
        auto r = dcmp->next();
        for (int i = 0; i < (int)r.size(); i++)
            dst[i] = r.at(i);
        return r.size();
    }
    return -1;
}

int CHasNext(CDecompressor *dcmp) {
    if (dcmp->has_next())
        return 1;
    return -1;
}

int CSkip(CDecompressor *dcmp) {
    if (dcmp->dst.size() > 0) {
        auto result = dcmp->dst.front();
        dcmp->dst.pop_front();
        dcmp->words_returned++;
        return result.size();
    } else {
        int r = dcmp->decode_words();
        if (r == 1)
            return CSkip(dcmp);
        return -1;
    }
}

int CMatch(CDecompressor *dcmp, unsigned char *word, int size) {
    if (dcmp->dst.size() > 0) {
        auto result = dcmp->dst.front();

        if ((int)result.size() != size)
            return -1;

        for (int i = 0; i < size; i++)
            if (result.at(i) != word[i])
                return -1;

        dcmp->dst.pop_front();
        dcmp->words_returned++;
        return 1;
    } else {
        int r = dcmp->decode_words();
        if (r == 1)
            CMatch(dcmp, word, size);
        return -1;
    }
}

int CMatchPrefix(CDecompressor *dcmp, unsigned char *prefix, int size) {
    if (dcmp->dst.size() > 0) {
        auto result = dcmp->dst.front();
        for (int i = 0; i < size; i++)
            if (result.at(i) != prefix[i])
                return -1;
        return 1;
    } else {
        int r = dcmp->decode_words();
        if (r == 1)
            CMatchPrefix(dcmp, prefix, size);
        return -1;
    }
}

size_t CSize(CDecompressor *dcmp) {
    return dcmp->f_size();
}

void CReset(CDecompressor *dcmp) {
    dcmp->reset_hard();
}

CDecompressor::CDecompressor(const char *file_name) {
    this->file_data = CMmapRead(file_name);

    if (this->file_data == NULL) {
        std::cout << "Failed to create mapping of the file"
                  << "\n";
        exit(1);
    }

    std::vector<uint8_t> header(this->file_data->buf, this->file_data->buf + 24);
    uint32_t a, b, c, d;

    a = header[0] << 24, b = header[1] << 16, c = header[2] << 8, d = header[3];
    this->total_words = a | b | c | d;
    // std::cout << "DECOMPRESSOR Total_words: " << total_words << "\n";
    a = header[4] << 24, b = header[5] << 16, c = header[6] << 8, d = header[7];
    this->total_blocks = a | b | c | d;

    this->three_blocks_count = this->total_blocks / 3;
    this->rest_blocks = this->total_blocks % 3;

    // std::cout << "DECOMPRESSOR Total_blocks: " << total_blocks << "\n";

    // std::cout << "\n";
    this->decoder = new Decoder(this->file_data->buf + 24, this->file_data->size - 24);

    this->block = {};
    this->blocks_decoded = 0;

    this->words_decoded = 0;
    this->words_returned = 0;
}

CDecompressor::~CDecompressor() {
    delete this->decoder;
    CMunMap(this->file_data);
}

size_t CDecompressor::f_size() {
    return this->file_data->size;
}

int CDecompressor::decode_words() {

    // auto start = timing::time_now();

    int is_decoded = -1;
    if (this->three_blocks_count > 0) {
        for (int i = 0; i < 3; i++) {
            int block_idx = this->decoder->decode_block(&this->block);
            std::copy(block.begin(), block.begin() + block_idx, std::back_inserter(prev_left));
            this->blocks_decoded++;
        }
        this->three_blocks_count--;
        is_decoded = 1;
    } else {
        if (this->rest_blocks > 0) {
            for (int i = 0; i < this->rest_blocks; i++) {
                int block_idx = this->decoder->decode_block(&this->block);
                std::copy(block.begin(), block.begin() + block_idx, std::back_inserter(prev_left));
                this->blocks_decoded++;
            }
            this->rest_blocks = 0;
            is_decoded = 1;
        }
        if (this->blocks_decoded != this->total_blocks) {
            std::cout << "BLOCKS DECODED: " << this->blocks_decoded << "\n";
            std::cout << "TOTAL BLOCKS: " << this->total_blocks << "\n";
        }
        assert(this->blocks_decoded == this->total_blocks);
    }
    if (is_decoded == 1) {
        int a, b, c, w_size;
        a = prev_left.at(0), b = prev_left.at(1), c = prev_left.at(2);
        w_size = (a << 16) | (b << 8) | c;

        int word_total = 3 + w_size;
        int prev_size = prev_left.size();

        if (word_total > prev_size)
            return this->decode_words();

        assert(this->dst.size() == 0);

        this->dst.push_back(std::vector<uint8_t>(prev_left.begin() + 3, prev_left.begin() + word_total));
        this->words_decoded++;

        int i = word_total;
        for (;;) {
            if (i >= prev_size || i + 1 >= prev_size || i + 2 >= prev_size) break;
            a = prev_left.at(i), b = prev_left.at(i + 1), c = prev_left.at(i + 2);
            w_size = (a << 16) | (b << 8) | c;
            word_total = 3 + w_size;
            if (i + word_total > prev_size) break;
            this->dst.push_back(std::vector<uint8_t>(prev_left.begin() + i + 3, prev_left.begin() + i + word_total));
            this->words_decoded++;
            i += word_total;
        }

        // std::copy(prev_left.begin(), prev_left.begin() + word_total, std::back_inserter(dst));
        prev_left = std::move(std::vector<uint8_t>(prev_left.begin() + i, prev_left.end()));

        // auto stop = timing::time_now();

        // timing::duration(start, stop, "DECODE WORDS");
        // std::cout << "================================ DECODE WORDS DONE"
        //           << "\n";

        return is_decoded;
    } else {
        // assert(prev_left.size() == 0);
        return is_decoded;
    }
}

// used for testing only
std::vector<uint8_t> CDecompressor::decode() {
    // auto start = timing::time_now();

    int total_b = this->total_blocks;
    int block_n = 0;
    while (total_b > 0) {
        // std::cout << "BLOCK_N: " << block_n << "\n";
        int block_idx = this->decoder->decode_block(&this->block);
        std::vector<uint8_t> v(this->block.begin(), this->block.begin() + block_idx);
        this->blocks.push_back(std::move(v));
        block_n++;
        total_b--;
    }

    auto stop = timing::time_now();

    // timing::duration(start, stop, "DECODE");
    // std::cout << "================================ DECODE DONE"
    //           << "\n";
    return std::vector<uint8_t>(0);
}

std::vector<uint8_t> CDecompressor::next() {
    if (this->dst.size() > 0) {
        auto result = this->dst.front();
        this->dst.pop_front();
        this->words_returned++;
        return result;
    } else {
        int r = this->decode_words();
        // std::cout << "R: " << r << "\n";
        if (r == 1)
            return this->next();
        return std::vector<uint8_t>(0);
    }
}

bool CDecompressor::has_next() {
    if (this->words_returned == this->total_words) {
        assert(this->words_returned == this->words_decoded);
        return false;
    }
    assert(this->words_returned < this->total_words);
    return true;
}

void CDecompressor::reset_hard() {

    this->three_blocks_count = this->total_blocks / 3;
    this->rest_blocks = this->total_blocks % 3;

    this->decoder->reset_hard();

    this->block = {};
    this->blocks_decoded = 0;

    this->words_decoded = 0;
    this->words_returned = 0;
}

// this->dst.clear();

// int a, b, c, w_size;
// int block_idx = this->decoder->decode_block(&this->block);
// std::copy(block.begin(), block.begin() + block_idx, std::back_inserter(prev_left));
// this->blocks_decoded++;

// a = prev_left.at(0), b = prev_left.at(1), c = prev_left.at(2);
// w_size = (a << 16) | (b << 8) | c;
// int word_total = 3 + w_size;

// if (word_total > prev_left.size()) {
//     int size_left = (word_total - prev_left.size());
//     int num_blocks = size_left / UINT16_MAX;
//     int rest = size_left % UINT16_MAX;
//     assert(num_blocks <= (this->total_blocks - this->blocks_decoded));
//     for (int i = 0; i < num_blocks; i++) {
//         int block_idx = this->decoder->decode_block(&this->block);
//         std::copy(block.begin(), block.begin() + block_idx, std::back_inserter(prev_left));
//         this->blocks_decoded++;
//     }
//     assert(this->total_blocks > this->blocks_decoded);

//     if (rest > 0) {
//         int block_idx = this->decoder->decode_block(&this->block);
//         std::copy(block.begin(), block.begin() + block_idx, std::back_inserter(prev_left));
//         this->blocks_decoded++;
//     }

//     std::copy(prev_left.begin(), prev_left.begin() + word_total, std::back_inserter(dst));
//     prev_left = std::move(std::vector<uint8_t>(prev_left.begin() + word_total, prev_left.end()));
// } else {
//     int prev_size = prev_left.size();
//     int i = word_total;
//     for (;;) {
//         if (i + 1 >= prev_size || i + 2 >= prev_size) break;
//         a = prev_left.at(i), b = prev_left.at(i + 1), c = prev_left.at(i + 2);
//         w_size = (a << 16) | (b << 8) | c;
//         word_total = 3 + w_size;
//         if (i + word_total > prev_size) break;
//         i += word_total;
//     }
//     std::copy(prev_left.begin(), prev_left.begin() + word_total, std::back_inserter(dst));
//     prev_left = std::move(std::vector<uint8_t>(prev_left.begin() + word_total, prev_left.end()));
//     // // starting point of the next word (+3 byte size of the first word)
//     // int sp = size + 3;

//     // // how many words we decoded?
//     // // since we know the size of a word, there has to be 1 word
//     // int words = 1;
//     // for (;;) {
//     //     // if we can't get another 3 bytes from dst, break the loop
//     //     if (sp + 1 > dst_idx || sp + 2 > dst_idx) break;

//     //     // combine 3 bytes to get the next word size
//     //     a = dst->at(sp), b = dst->at(sp + 1), c = dst->at(sp + 2);
//     //     size = (a << 16) | (b << 8) | c; // next word size

//     //     // if next word wasn't fully decoded (it is in the next block)
//     //     if (sp + size + 3 > dst_idx) break;

//     //     // increament starting point (word size + 3 byte for its size)
//     //     sp += (size + 3);
//     //     words++;
//     // }
// }
// int i = 0, j = 0;
// for (; i < block_idx;) {
//     a = block.at(i), b = block.at(i + 1), c = block.at(i + 2);
//     w_size = (a << 16) | (b << 8) | c;
//     j = i + 3;
//     if (j + w_size > UINT16_MAX) {

//     } else {

//     }
// }

// int _num_words, _size;
// std::tie(_size, _num_words) = this->decoder->decode(&this->dst, 0, 0);
// std::cout << "Dst_idx: " << size << "\n";
// if (_num_words == -1 && this->has_next()) {

//     int a, b, c, w_size;
//     int i = 0;
//     for (; i < size;) {
//         a = dst.at(i), b = dst.at(i + 1), c = dst.at(i + 2);
//         w_size = (a << 16) | (b << 8) | c;
//         // if (i + w_size + 3 > size) break;
//         i += (3 + w_size);
//         this->words_count += 1;
//         this->dst_words += 1;
//     }
//     this->size = _size;
//     assert(i == size);

// } else {
//     this->dst_words = _num_words;
//     this->size = _size;

//     this->words_count += this->dst_words;
// }

// std::cout << "DST WORDS: " << _num_words << "\n";