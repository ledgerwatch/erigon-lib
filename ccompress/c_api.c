#include "c_api.h"
#include "ccompress.h"

#include <assert.h> // assert

/*
    compressed file structure

HEADER
    - max_buff_size(4 byte) -> to allocate memory for possibly largest compressed data
    - num_words - 4 byte
    - shared_topology_size(2 byte) -> 0 means, there no shared topology
    --------------------------- 10 byte total
SHARED TOPOLOGY (if any)
    - random number of bytes -> size written in a header, could be 0
SIZES
    - each 4 ([0, 1, 2, 3]) byte in this data: ;
        - 0-byte: "PRE-BYTE" for earch word
            PRE-BYTE:
                NOT_COMPRESSED 0
                COMPRESSED 1
                SHARED 2 - is not written to final result
                SHARED_COMPRESSED 3
                ALL_SAME 4
                ONE_BYTE 8
        - 1..3 bytes: = n
            if (NOT_COMPRESSED)
                n = size of original data
            if (COMPRESSED)
                n =
                - 3 bytes -> size of original data
                - 2 bytes -> size of topology(t)
                - t-bytes -> topology it self
                - rest(n - 2 + 3 + t) -> compressed word
            if (SHARED_COMPRESSED) (means that there is a shared topology)
                n =
                - 3 bytes -> size of original data
                - rest(n - 3) -> compressed word
            if (ALL_SAME)
                n = size of original data
            if (ONE_BYTE)
                n = size of original data
    --------------------------- 4 * (num_words) bytes total
DATA
    if (PRE-BYTE == NOT_COMPRESSED)
        original data itself
    if (PRE-BYTE == COMPRESSED)
        - 3 bytes -> size of original data
        - 2 bytes -> size of topology(t)
        - t-bytes -> topology it self
        - rest(n - 2 + 3 + t) -> compressed word
    if (PRE-BYTE == SHARED_COMPRESSED)
        - 3 bytes -> size of original word
        - rest(n - 3) -> compressed word
    if (PRE-BYTE == ALL_SAME)
        single byte written
    if (PRE-BYTE == ONE_BYTE)
        single byte written
*/

int new_compressor(const char *out_file, const char *idt_file) {

    cmp = malloc(sizeof(compressor));

    cmp->idt = fopen(idt_file, "w+");
    cmp->fp = fopen(out_file, "w+");

    if (cmp->idt == NULL)
        return ERROR_FOPEN;
    if (cmp->fp == NULL)
        return ERROR_FOPEN;

    reset_encode_vars_shared();

    MAX_WORD = -1;
    MAX_WORD_DECODE = -1;
    NUM_WORDS = 0;
    return 0;
}

void close_compressor() {
    if (cmp != NULL) {
        if (cmp->fp != NULL) fclose(cmp->fp);
        if (cmp->idt != NULL) {
            // TODO remove file
            fclose(cmp->idt);
        }
        free(cmp);
        cmp = NULL;
    }
}

int all_same(byte *word, int size) {
    for (int i = 1; i < size; i++) {
        if (word[i] != word[0])
            return 0;
    }
    return 1;
}

void add_word(byte *word, int size) {

    int buf_size = size + 1024;
    byte *dst = malloc(sizeof(byte) * buf_size);

    if (buf_size > MAX_WORD_DECODE)
        MAX_WORD_DECODE = buf_size;

    dst[0] = NOT_COMPRESSED;     // flag - no need to compress at all by default
    dst[1] = (byte)(size >> 16); // original size
    dst[2] = (byte)(size >> 8);  // original size
    dst[3] = (byte)size;         // original size

    if (size == 1) {
        dst[0] = ONE_BYTE;
        dst[4] = word[0];
        fwrite(dst, sizeof(dst[0]), 5, cmp->idt);
        NUM_WORDS++;
        if (MAX_WORD < size) MAX_WORD = size;
        free(dst);
        return;
    }

    // if all bytes are the same, compressor can compress if there is at least 2 different bytes
    if (all_same(word, size)) {
        dst[0] = ALL_SAME; // all the same flag
        dst[4] = word[0];
        fwrite(dst, sizeof(dst[0]), 5, cmp->idt);
        NUM_WORDS++;
        if (MAX_WORD < size) MAX_WORD = size;
        free(dst);
        return;
    }

    // check if it makes sense to compress a word
    // if it does:
    //      compress it,
    //      set flag as compressed = 0000_0001
    // if it does not:
    //  a) if compressed_data + topo_size > uncompressed_size && compressed_data < uncompressed_size
    //      count_freq_shared();
    //      set flag as shared topology - 0000_0010 (this will be changed later in 2nd stage)
    //  b) if compressed_data >= uncompressed_size
    //      set flag as no need to compress at all - 0000_0000

    // [0, 1, 2, 3] - flag and size of tatal compressed data
    // [4, 5, 6] - size of original data
    // [7, 8] - size of topology
    // [9...compressed_size] - compressed word itself

    int seek_to = 0;

    int result = __compress(word, dst, size);
    int write_size = result != -1 ? result + 4 : size + 4; // +4 is flag byte + data_size

    fwrite(dst, sizeof(dst[0]), write_size, cmp->idt);
    NUM_WORDS++;
    if (MAX_WORD < write_size) MAX_WORD = write_size;
    free(dst);
}

void compress() {
    int seek_to = 0;       // changes every idt_file read cycle
    int compressed;        // size of all compressed small words
    int topo_size;         // size of shared tree topology, which is used to recreate encoded bits
    int total;             // compressed + topo_size
    topology *shared_topo; // shared tree topology between small words
    int header_size = 10;
    int sizes = 4 * NUM_WORDS;                   // how many bytes takes to write sizes
    int offset_data_start = header_size + sizes; // if there is no topology
    int skip_small = 1;                          // by default skip all small words

    byte *buf = malloc(sizeof(byte) * MAX_WORD);
    byte *dst = malloc(sizeof(byte) * MAX_WORD);
    byte *sizes_arr = malloc(sizeof(byte) * sizes);
    byte *header = malloc(sizeof(byte) * header_size);

    // byte *test_buf = malloc(sizeof(byte) * MAX_WORD);

    // could be 3 bytes!
    header[0] = (byte)(MAX_WORD_DECODE >> 24);
    header[1] = (byte)(MAX_WORD_DECODE >> 16);
    header[2] = (byte)(MAX_WORD_DECODE >> 8);
    header[3] = (byte)(MAX_WORD_DECODE);

    header[4] = (byte)(NUM_WORDS >> 24);
    header[5] = (byte)(NUM_WORDS >> 16);
    header[6] = (byte)(NUM_WORDS >> 8);
    header[7] = (byte)(NUM_WORDS);

    header[8] = 0, header[9] = 0;

    if (SHARED_SMALL_WORDS > 0) {                 // if there at least one word that shares toplogy
        shared_topo = create_new_codes_shared();  // create new encoding bits, get tree topology
        compressed = (SHARED_TOTAL_BITS / 8) + 1; // how many bytes takes to write all small words
        topo_size = shared_topo->data_idx + 1;    //
        total = compressed + topo_size;           // tree + compressed small words
        if (total < UNCOMPRESSED_BYTES_SIZE) {    // total bytes < combined_sizes of all small words
            offset_data_start += topo_size;       // add to offset additional bytes
            skip_small = 0;                       // do not skip
            header[8] = (byte)(topo_size >> 8);
            header[9] = (byte)topo_size;
        }
    }

    fwrite(header, sizeof(header[0]), header_size, cmp->fp);

    if (!skip_small)
        fwrite(shared_topo->data, sizeof(header[0]), topo_size, cmp->fp);

    fseek(cmp->idt, seek_to, SEEK_SET);

    fseek(cmp->fp, offset_data_start, SEEK_SET); // start writing data, befre writing sizes

    int sizes_idx = 0;
    int flag, write_size;
    while (fread(buf, sizeof(buf[0]), MAX_WORD, cmp->idt) != 0) {
        flag = buf[0];
        write_size = (int)(buf[1] << 16) | (int)(buf[2] << 8) | (int)buf[3];

        if (flag == COMPRESSED) {
            // [0, 1, 2, 3] - flag and size of tatal compressed data
            // [4, 5, 6] - size of original data
            // [7, 8] - size of topology
            // [9..topo_size-1] - topology itself
            // [topo_size...compressed_size-1] - compressed word itself

            fwrite(&buf[4], sizeof(buf[0]), write_size, cmp->fp);
        }

        if (flag == SHARED) {

            if (!skip_small) {
                // here write_size is original word size
                int result = __compress_shared(&buf[0], dst, write_size);

                fwrite(&dst[4], sizeof(buf[0]), result, cmp->fp);

                sizes_arr[sizes_idx] = dst[0];
                sizes_arr[sizes_idx + 1] = dst[1];
                sizes_arr[sizes_idx + 2] = dst[2];
                sizes_arr[sizes_idx + 3] = dst[3];
                sizes_idx += 4;
                seek_to += (write_size + 4);

                fseek(cmp->idt, seek_to, SEEK_SET);
                continue;
            } else {
                // all small words are not compressed
                buf[0] = NOT_COMPRESSED;
                fwrite(&buf[4], sizeof(buf[0]), write_size, cmp->fp);
            }
        }

        if (flag == NOT_COMPRESSED) {
            int a, b, c;
            a = buf[1], b = buf[2], c = buf[3];
            assert((a << 16 | b << 8 | c) == write_size);
            fwrite(&buf[4], sizeof(buf[0]), write_size, cmp->fp);
        }

        if (flag == ALL_SAME || flag == ONE_BYTE) {
            write_size = 1;
            fwrite(&buf[4], sizeof(buf[0]), write_size, cmp->fp);
        }

        sizes_arr[sizes_idx] = buf[0];
        sizes_arr[sizes_idx + 1] = buf[1];
        sizes_arr[sizes_idx + 2] = buf[2];
        sizes_arr[sizes_idx + 3] = buf[3];
        sizes_idx += 4;

        seek_to += (write_size + 4);

        fseek(cmp->idt, seek_to, SEEK_SET);
    }

    // int a, b, c, d;
    // for (int i = 0; i < sizes; i += 4) {
    //     a = sizes_arr[i];
    //     b = (int)sizes_arr[i + 1], c = (int)sizes_arr[i + 2], d = (int)sizes_arr[i + 3];
    //     printf("flag_after: %d, size_after: %d\n", a, (b << 16 | c << 8 | d));
    // }

    if (!skip_small)
        fseek(cmp->fp, header_size + topo_size, SEEK_SET);
    else
        fseek(cmp->fp, header_size, SEEK_SET);

    fwrite(sizes_arr, sizeof(buf[0]), sizes, cmp->fp);

    free(buf);
    free(dst);
    free(sizes_arr);
    free(header);
    if (SHARED_SMALL_WORDS > 0)
        free(shared_topo);
    // free(test_buf);

    close_compressor();
}

/* ------------------ decoding part -------------------- */

int new_decompressor(unsigned char *data, int size) {

    assert(dcmp == NULL);
    dcmp = malloc(sizeof(decompressor));

    dcmp->data = data;
    dcmp->size = size;
    dcmp->current = -1; // points to the current word, 0 - first word, -1 - points nowhere
    // TODO handle possible errors

    // could be 3 bytes!

    MAX_WORD_DECODE = (int)(data[0] << 24) | (int)(data[1] << 16) | (int)(data[2] << 8) | (int)data[3];
    NUM_WORDS = (int)(data[4] << 24) | (int)(data[5] << 16) | (int)(data[6] << 8) | (int)data[7];

    int topo_size = (uint16_t)(data[8] << 8) | data[9];

    huff_node *decode_root;
    if (topo_size != 0) { // means there is  shared topology
        reset_decode_vars_shared();
        decode_root = tree_from_topo_shared(&data[10], topo_size);
        int8_t bit_length = 0;
        uint16_t bits = 0;
        dfs_decode_shared(decode_root, &bit_length, bits); // this will create decoding table
    } else {                                               // we have no shared topology, so we dont need to create it
    }

    int offset_to_sizes = 10 + topo_size;
    dcmp->sizes = malloc(sizeof(int) * NUM_WORDS);
    int idx = offset_to_sizes;
    for (int i = 0; i < NUM_WORDS; i++) {
        int a, b, c, d;
        a = (int)data[idx], b = (int)data[idx + 1];
        c = (int)data[idx + 2], d = (int)data[idx + 3];
        // printf("pre_byte: %d, size: %d\n", a, (b << 16) | (c << 8) | d);
        dcmp->sizes[i] = (a << 24) | (b << 16) |
                         (c << 8) | (d);
        idx += 4;
    }

    dcmp->data_offset = idx;

    return MAX_WORD_DECODE;
}

int next(unsigned char *dst) {
    if (dcmp->current < NUM_WORDS) {
        dcmp->current++;
        int info = dcmp->sizes[dcmp->current];

        int size = 0x00FFFFFF & info;
        byte flag = info >> 24;

        if (dcmp->current > 0) {
            int prev_info = dcmp->sizes[dcmp->current - 1];
            byte prev_flag = prev_info >> 24;
            int prev_size = 0x00FFFFFF & prev_info;
            if (prev_flag == ALL_SAME || prev_flag == ONE_BYTE)
                dcmp->data_offset += 1;
            else
                dcmp->data_offset += prev_size;
        }

        int offset_to_current = dcmp->data_offset; // where data starts

        if (flag == NOT_COMPRESSED) {
            for (int i = 0; i < size; i++)
                dst[i] = dcmp->data[offset_to_current + i];
            return size;
        }

        if (flag == COMPRESSED) {
            int bytes_written = __decompress(&dcmp->data[offset_to_current], dst, size);
            return bytes_written;
        }

        if (flag == SHARED_COMPRESSED) {
            int bytes_written = __decompress_shared(&dcmp->data[offset_to_current], dst, size);
            return bytes_written;
        }

        if (flag == ALL_SAME) {
            for (int i = 0; i < size; i++)
                dst[i] = dcmp->data[offset_to_current];
            return size;
        }

        if (flag == ONE_BYTE) {
            assert(size == 1);
            dst[0] = dcmp->data[offset_to_current];
            return size;
        }

        assert(flag != SHARED);
    }
    return -1;
}

int has_next() {
    if (dcmp->current < NUM_WORDS - 1 && NUM_WORDS > 0)
        return 1;
    return -1;
}

void close_decompressor() {
    if (dcmp != NULL) {
        if (dcmp->sizes != NULL) free(dcmp->sizes);
        free(dcmp);
        dcmp = NULL;
    }
}