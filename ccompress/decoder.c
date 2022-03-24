#include "ccompress.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

// used to construct a tree from topology
huff_node *stack[256];
huff_node *stack_shared[256];

int8_t decode_bit_table[UINT16_MAX];
int16_t decode_table[UINT16_MAX];

int8_t decode_bit_table_shared[UINT16_MAX];
int16_t decode_table_shared[UINT16_MAX];

int min_bit_length = UINT16_MAX;
int min_bit_length_shared = UINT16_MAX;

int max_bit_length_shared = -1;

const uint16_t masks16[17] = {
    0b0000000000000000, // placeholder - 0
    0b1000000000000000, // 1
    0b1100000000000000, // 2
    0b1110000000000000, // 3
    0b1111000000000000, // 4
    0b1111100000000000, // 5
    0b1111110000000000, // 6
    0b1111111000000000, // 7
    0b1111111100000000, // 8
    0b1111111110000000, // 9
    0b1111111111000000, // 10
    0b1111111111100000, // 11
    0b1111111111110000, // 12
    0b1111111111111000, // 13
    0b1111111111111100, // 14
    0b1111111111111110, // 15
    0b1111111111111111, // 16
};

const uint8_t extra_bit_mask[9] = {
    0b00000000, // placeholder - 0
    0b00000001, // 1
    0b00000011, // 2
    0b00000111, // 3 // 0111_0000
    0b00001111, // 4 // 0b0111_1110_0000_0000
    0b00011111, // 5
    0b00111111, // 6
    0b01111111, // 7
    0b11111111, // 8
};

void reset_decode_vars() {
    min_bit_length = UINT16_MAX;
    for (int i = 0; i < 256; i++) {
        stack[i] = NULL;
    }

    for (int i = 0; i < UINT16_MAX; i++) {
        decode_table[i] = -1;
        decode_bit_table[i] = -1;
    }
}

void reset_decode_vars_shared() {
    min_bit_length_shared = UINT16_MAX;
    for (int i = 0; i < 256; i++) {
        stack_shared[i] = NULL;
    }

    for (int i = 0; i < UINT16_MAX; i++) {
        decode_table_shared[i] = -1;
        decode_bit_table_shared[i] = -1;
    }
}

void dfs_decode(huff_node *node, int8_t *bit_length, uint16_t bits) {
    if (node->left_child != NULL) {
        (*bit_length)++;

        int bit_idx = (~((*bit_length) - 16) + 1);
        bits &= ~(0 << bit_idx);

        dfs_decode(node->left_child, bit_length, bits);
    }

    if (node->right_child != NULL) {
        (*bit_length)++;
        // set bit to 1
        int bit_idx = (~((*bit_length) - 16) + 1);
        bits |= (1 << bit_idx);

        dfs_decode(node->right_child, bit_length, bits);
    }

    if (node->value >= 0) { // leaf node

        // assert((*bit_length) <= 16);
        // assert(decode_table[bits] == -1);

        // assert(bits <= UINT16_MAX);
        decode_table[bits] = (byte)node->value;
        decode_bit_table[bits] = *bit_length;

        if (*bit_length < min_bit_length)
            min_bit_length = *bit_length;
    }

    (*bit_length)--;
    free(node);
}

void dfs_decode_shared(huff_node *node, int8_t *bit_length, uint16_t bits) {
    if (node->left_child != NULL) {
        (*bit_length)++;

        int bit_idx = (~((*bit_length) - 16) + 1);
        bits &= ~(0 << bit_idx);

        dfs_decode_shared(node->left_child, bit_length, bits);
    }

    if (node->right_child != NULL) {
        (*bit_length)++;
        // set bit to 1
        int bit_idx = (~((*bit_length) - 16) + 1);
        bits |= (1 << bit_idx);

        dfs_decode_shared(node->right_child, bit_length, bits);
    }

    if (node->value >= 0) { // leaf node

        // assert((*bit_length) <= 16);
        // assert(decode_table_shared[bits] == -1);

        // assert(bits <= UINT16_MAX);
        decode_table_shared[bits] = (byte)node->value;
        decode_bit_table_shared[bits] = *bit_length;

        if (*bit_length < min_bit_length_shared)
            min_bit_length_shared = *bit_length;

        if (*bit_length > max_bit_length_shared)
            max_bit_length_shared = *bit_length;
    }

    (*bit_length)--;
    free(node);
}

// void print_byte(byte b) {
//     printf("0b");
//     int next = 0;
//     for (int i = 7; i >= 0; i--) {
//         next = 0;
//         if (i % 4 == 0 && i != 0) next = 1;
//         printf("%d", ((1 << i) & b) ? 1 : 0);
//         if (next) printf("_");
//     }
//     printf("\n");
// }

// void print_uint16(uint16_t n) {
//     printf("0b");
//     int next = 0;
//     for (int i = 15; i >= 0; i--) {
//         next = 0;
//         if (i % 4 == 0 && i != 0) next = 1;
//         printf("%d", ((1 << i) & n) ? 1 : 0);
//         if (next) printf("_");
//     }
//     printf("\n");
// }

void _decode_16(int min_bits, int *rest_bits, uint16_t *rest, byte *dst, int *d_idx) {
    int not_found = 1;
    while ((*rest_bits) >= min_bits) {
        for (int i = min_bits; i <= (*rest_bits); i++) {
            uint16_t p = masks16[i] & (*rest);
            if (decode_table[p] >= 0 && decode_bit_table[p] == i) {
                dst[(*d_idx)++] = decode_table[p];
                (*rest) = (*rest) << i;
                (*rest_bits) = (*rest_bits) - i;
                not_found = 0;
                break;
            }
            not_found = 1;
        }
        if (not_found) break;
    }
}

void _decode_16_shared(int min_bits, int *rest_bits, uint16_t *rest, byte *dst, int *d_idx) {
    int not_found = 1;
    while ((*rest_bits) >= min_bits) {
        for (int i = min_bits; i <= (*rest_bits); i++) {
            uint16_t p = masks16[i] & (*rest);
            if (decode_table_shared[p] >= 0 && decode_bit_table_shared[p] == i) {
                dst[(*d_idx)++] = decode_table_shared[p];
                (*rest) = (*rest) << i;
                (*rest_bits) = (*rest_bits) - i;
                not_found = 0;
                break;
            }
            not_found = 1;
        }
        if (not_found) break;
    }
}

int huff_decode2(byte *src, byte *dst, int size, int min_bits, int is_shared) {
    int d_idx = 0; // index in destination byte array

    uint16_t rest = 0; // whatever bits left from previous byte
    int rest_bits = 0; // how many bits occupied?

    byte extra = 0; // extra bits that doesnt fit into 16-bit uint
    int extra_bits = 0;

    for (int i = 0; d_idx <= size; i++) {

        //  16 15 14 13 12 11 10 9 8 7 6 5 4 3 2 1
        //  0  0  0  0  0  0  0  0 0 0 0 0 0 0 0 0
        //  1234 5678 9012 3456
        //  0000_0000_0000_0000

        if (rest_bits > 8 && rest_bits <= 16) { // 9 or more bits
            int to_shift = (rest_bits - 8);
            // assert(to_shift <= 8);
            rest |= (src[i] >> to_shift);
            rest_bits += (8 - to_shift);

            extra = (src[i] & extra_bit_mask[to_shift]) << (8 - to_shift);
            extra_bits = to_shift;
        } else if (rest_bits <= 8) {
            rest |= (((uint16_t)src[i]) << (8 - rest_bits));
            rest_bits += 8;
        } else {
            printf("ERROR: rest_bits > 16\n");
            exit(1);
        }

        if (is_shared)
            _decode_16_shared(min_bits, &rest_bits, &rest, dst, &d_idx);
        else
            _decode_16(min_bits, &rest_bits, &rest, dst, &d_idx);

        if (extra_bits != 0) {
            // assert(extra_bits <= 8);
            rest |= (extra << (8 - rest_bits));
            rest_bits += extra_bits;
            // assert(rest_bits <= 16);

            if (is_shared)
                _decode_16_shared(min_bits, &rest_bits, &rest, dst, &d_idx);
            else
                _decode_16(min_bits, &rest_bits, &rest, dst, &d_idx);
            extra = 0;
            extra_bits = 0;
        }
    }

    return size;
}

huff_node *tree_from_topo(byte *topo, int size) {

    int stack_idx = 0;
    int bit_idx = 7;

    huff_node *root;

    // for (int i = 0; i < size; i++)
    //     printf("%d ", topo[i]);
    // printf("\n");

    for (int d_idx = 0; d_idx < size;) {

        if (bit_idx < 0) {
            bit_idx = 7;
            d_idx++;
        }

        byte current = topo[d_idx];

        // printf("current: %d\n", current);

        for (; bit_idx > -1; bit_idx--) {
            // assert(bit_idx <= 7 && bit_idx > -1);

            if ((1 << bit_idx) & current) { // 1 found, next 8 bits are code we need
                // assert(d_idx + 1 < size);
                // e.g
                //             7654 3210           7654 3210 <- bit indexes
                // current = 0b0010_0111, next = 0b0010_0010
                // in this case 1 bit is at index = 5, so we need part of the bits
                // of the current byte starting from index 4 which is 0_0111
                // and 3 first bits from next byte which is 001

                // get required number of bits from current byte

                byte code = (current << (8 - bit_idx));
                byte next = topo[++d_idx];

                code |= (next >> bit_idx);

                huff_node *leaf = malloc(sizeof(huff_node));

                leaf->left_child = NULL;
                leaf->right_child = NULL;
                leaf->value = code;
                leaf->weight = 0;

                stack[stack_idx++] = leaf;

                // decrease bit index for the next cycle, before "break"
                // to point it to the next bit in the "current" byte of the next cycle
                bit_idx--;

                break;
            } else {
                // assert(((1 << bit_idx) & current) == 0);

                if (stack_idx > 1) {

                    huff_node *first = stack[--stack_idx];
                    stack[stack_idx] = NULL;
                    assert(first != NULL);
                    huff_node *second = stack[--stack_idx];
                    stack[stack_idx] = NULL;
                    assert(second != NULL);

                    huff_node *combined = malloc(sizeof(huff_node));

                    combined->left_child = second;
                    combined->right_child = first;
                    combined->value = -1;
                    combined->weight = 0;
                    assert(stack[stack_idx + 1] == NULL);
                    stack[stack_idx++] = combined;

                } else if (stack_idx == 1) {
                    root = stack[0];
                    stack[0] = NULL;
                    return root;
                }
            }
        }
    }

    root = stack[0];
    stack[0] = NULL;
    return root;
}

huff_node *tree_from_topo_shared(byte *topo, int size) {

    int stack_idx = 0;

    int bit_idx = 7;
    int node_idx = 0;
    huff_node *root;

    for (int d_idx = 0; d_idx < size;) {

        if (bit_idx < 0) {
            bit_idx = 7;
            d_idx++;
        }

        byte current = topo[d_idx];

        // printf("current: %d\n", current);

        for (; bit_idx > -1; bit_idx--) {
            // assert(bit_idx <= 7 && bit_idx > -1);

            if ((1 << bit_idx) & current) { // 1 found, next 8 bits are code we need

                // e.g
                //             7654 3210           7654 3210 <- bit indexes
                // current = 0b0010_0111, next = 0b0010_0010
                // in this case 1 bit is at index = 5, so we need part of the bits
                // of the current byte starting from index 4 which is 0_0111
                // and 3 first bits from next byte which is 001

                // get required number of bits from current byte
                byte code = (current << (8 - bit_idx));
                byte next = topo[++d_idx]; // increment data index to get next byte
                // printf("next: %d\n", next);
                code |= (next >> bit_idx);

                // printf("next: %d\n", next);
                huff_node *leaf = malloc(sizeof(huff_node));
                leaf->left_child = NULL;
                leaf->right_child = NULL;
                leaf->value = code;
                leaf->weight = 0;
                stack_shared[stack_idx++] = leaf;

                // decrease bit index for the next cycle, before "break"
                // to point it to the next bit in the "current" byte of the next cycle
                bit_idx--;

                // assert(code <= 127 && code >= 0);
                break;
            } else {
                // assert(((1 << bit_idx) & current) == 0);

                if (stack_idx > 1) {

                    huff_node *first = stack_shared[--stack_idx];
                    stack_shared[stack_idx] = NULL;
                    // assert(first != NULL);
                    huff_node *second = stack_shared[--stack_idx];
                    stack_shared[stack_idx] = NULL;
                    // assert(second != NULL);

                    huff_node *combined = malloc(sizeof(huff_node));

                    combined->left_child = second;
                    combined->right_child = first;
                    combined->value = -1;
                    combined->weight = 0;

                    stack_shared[stack_idx++] = combined;
                } else if (stack_idx == 1) {
                    root = stack_shared[0];
                    stack_shared[0] = NULL;
                    return root;
                }
            }
        }
    }

    root = stack_shared[0];
    stack_shared[0] = NULL;
    return root;
}

// void free_tree(huff_node *node) {
//     if (node == NULL)
//         return;

//     free_tree(node->left_child);
//     free_tree(node->right_child);
//     free(node);
// }

int __decompress(byte *src, byte *dst, int compressed_size) {

    reset_decode_vars();

    int o_size = (int)(src[0] << 16) | (int)(src[1] << 8) | src[2]; // original size of the word
    int topo_size = (int)(src[3] << 8) | (int)src[4];

    huff_node *decode_root = tree_from_topo(&src[5], topo_size);

    int8_t bit_length = 0;
    uint16_t bits = 0;

    dfs_decode(decode_root, &bit_length, bits);

    int data_start = 3 + 2 + topo_size;

    int d_idx = huff_decode2(&src[data_start], dst, o_size, min_bit_length, 0);
    return o_size;
}

int __decompress_shared(byte *src, byte *dst, int compressed_size) {

    int o_size = (int)(src[0] << 16) | (int)(src[1] << 8) | src[2]; // original size of the word

    int d_idx = huff_decode2(&src[3], dst, o_size, min_bit_length_shared, 1);
    return o_size;
}

// reset_decode_vars();

// // assert(data_start + size < size + 1024);

// huff_node *decode_root = tree_from_topo(&dst[9], j);
// int8_t bit_length = 0;
// uint16_t bits = 0;

// int t = 30;
// dfs_decode(decode_root, &bit_length, bits);
// byte *test = malloc(sizeof(byte) * size);
// int d_idx = huff_decode2(&dst[data_start], test, compressed_size, MIN_BIT_LEN);
// assert(d_idx <= size);
// // for (int i = 0, j = data_start; i < size; i++)
// //     printf("dst: %d\n", dst[j++]);

// for (int i = 0; i < t; i++)
//     assert(src[i] == test[i]);

// printf("Got here\n");
// // int t_size = __decompress(&dst[4], test);
// // assert(t_size == topo->data_idx);
// free(test);