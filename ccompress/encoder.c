#include "ccompress.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

// frequency of each byte
int WEIGHTS[256];
int SHARED_WEIGHTS[256];

huff_node *NODES[256];
huff_node *SHARED_NODES[256];

// how many bits takes a new code?
// the new code itself. e.g: byte = 55; encode_table[55] = <new_code>;
// encode_bit_table[55]=<bit_length>
int8_t encode_bit_table[256];
uint16_t encode_table[256];

int8_t shared_encode_bit_table[256];
uint16_t shared_encode_table[256];

int heap_nodes = 0;
int shared_heap_nodes = 0;

int topo_bits = 0;
int shared_topo_bits = 0;

int total_bytes = 0;
int shared_total_bytes = 0;

int max_bit_length = 0;

int shared_max_bit_length = 0;

int nodes_created = 0;
int nodes_freed = 0;

void reset_encode_vars() {
    TOTAL_BITS = 0;
    heap_nodes = 0;
    topo_bits = 0;
    MIN_BIT_LEN = UINT16_MAX;
    max_bit_length = 0;
    nodes_created = 0;
    nodes_freed = 0;
    for (int i = 0; i < 256; i++) {
        WEIGHTS[i] = 0;
        NODES[i] = NULL;
        encode_bit_table[i] = -1;
        encode_table[i] = 0;
    }
}

void reset_encode_vars_shared() {
    SHARED_TOTAL_BITS = 0;
    SHARED_SMALL_WORDS = 0;
    shared_heap_nodes = 0;
    shared_topo_bits = 0;
    SHARED_MIN_BIT_LEN = UINT16_MAX;
    shared_max_bit_length = 0;
    UNCOMPRESSED_BYTES_SIZE = 0;
    for (int i = 0; i < 256; i++) {
        SHARED_WEIGHTS[i] = 0;
        SHARED_NODES[i] = NULL;
        shared_encode_bit_table[i] = -1;
        shared_encode_table[i] = 0;
    }
}

void count_freq(byte *src, int size) {
    for (int i = 0; i < size; i++)
        WEIGHTS[(int)src[i]]++;
}

void count_freq_shared(byte *src, int size) {
    for (int i = 0; i < size; i++)
        SHARED_WEIGHTS[(int)src[i]]++;
}

topology *init_topo() {
    topology *topo = malloc(sizeof(topology));
    for (int i = 0; i < 256 * 4; i++)
        topo->data[i] = 0;

    topo->data_idx = 0;
    topo->bit_idx = 7;
    return topo;
}

huff_node *make_node(int weight, int16_t value) {
    huff_node *node = malloc(sizeof(huff_node));
    node->weight = weight;
    node->value = value;
    node->left_child = NULL;
    node->right_child = NULL;
    nodes_created++;
    return node;
}

void add_node(huff_node *node) {
    int inserted = 0;
    if (node->value >= 0) { // it's a leaf node

        assert(NODES[node->value] == NULL);
        assert(node->value <= 255);
        NODES[node->value] = node; // insert it back to its place
        heap_nodes++;
        inserted = 1;
    } else { // non leaf node
        for (int i = 0; i < 256; i++) {
            if (NODES[i] == NULL) { // find empty place in array
                NODES[i] = node;    // insert it back to its place
                heap_nodes++;
                inserted = 1;
                break;
            }
        }
    }

    assert(inserted == 1);
}

void add_node_shared(huff_node *node) {
    int inserted = 0;
    if (node->value >= 0) { // it's a leaf node

        assert(SHARED_NODES[node->value] == NULL);
        assert(node->value <= 255);
        SHARED_NODES[node->value] = node; // insert it back to its place
        shared_heap_nodes++;
        inserted = 1;
    } else { // non leaf node
        for (int i = 0; i < 256; i++) {
            if (SHARED_NODES[i] == NULL) { // find empty place in array
                SHARED_NODES[i] = node;    // insert it back to its place
                shared_heap_nodes++;
                inserted = 1;
                break;
            }
        }
    }

    assert(inserted == 1);
}

huff_node *get_min_node() {

    int min_idx = 0;
    huff_node *node = NODES[min_idx]; // temp node with min weight

    for (int i = 1; i < 256; i++) {
        if (node == NULL && NODES[i] != NULL) {
            node = NODES[i];
            min_idx = i;
            continue;
        }

        if (NODES[i] != NULL && NODES[i]->weight < node->weight) {
            node = NODES[i];
            min_idx = i;
        }
    }
    NODES[min_idx] = NULL;
    heap_nodes--;
    return node;
}

huff_node *get_min_node_shared() {

    int min_idx = 0;
    huff_node *node = SHARED_NODES[min_idx]; // temp node with min weight

    for (int i = 1; i < 256; i++) {
        if (node == NULL && SHARED_NODES[i] != NULL) {
            node = SHARED_NODES[i];
            min_idx = i;
            continue;
        }

        if (SHARED_NODES[i] != NULL && SHARED_NODES[i]->weight < node->weight) {
            node = SHARED_NODES[i];
            min_idx = i;
        }
    }
    SHARED_NODES[min_idx] = NULL;
    shared_heap_nodes--;
    return node;
}

huff_node *make_tree() {
    huff_node *root;
    while (heap_nodes > 0) {

        if (heap_nodes == 1) {
            root = get_min_node();
            break;
        }

        huff_node *first = get_min_node();
        huff_node *second = get_min_node();
        // printf("first->value: %d, first->weight: %d\n", first->value, first->weight);
        // printf("second->value: %d, second->weight: %d\n", second->value, second->weight);
        huff_node *combined = malloc(sizeof(huff_node));
        nodes_created++;
        combined->weight = first->weight + second->weight;
        combined->left_child = first;
        combined->right_child = second;
        combined->value = -1;
        // printf("combined->value: %d, combined->weight: %d\n", combined->value, combined->weight);
        add_node(combined);
    }

    return root;
}

huff_node *make_tree_shared() {
    huff_node *root;
    while (shared_heap_nodes > 0) {

        if (shared_heap_nodes == 1) {
            root = get_min_node_shared();
            break;
        }

        huff_node *first = get_min_node_shared();
        huff_node *second = get_min_node_shared();
        // printf("first->value: %d, first->weight: %d\n", first->value, first->weight);
        // printf("second->value: %d, second->weight: %d\n", second->value, second->weight);
        huff_node *combined = malloc(sizeof(huff_node));
        combined->weight = first->weight + second->weight;
        combined->left_child = first;
        combined->right_child = second;
        combined->value = -1;
        // printf("combined->value: %d, combined->weight: %d\n", combined->value, combined->weight);
        add_node_shared(combined);
    }

    return root;
}

void write_topology(topology *topo, int16_t value) {
    // byte current = topo->data[topo->bit_idx];

    if (value >= 0) {            // leaf node
        byte code = (byte)value; // we need last 8 bits only (from left to right)
        // set bit to 1 at bit_idx in the currently "working" byte
        // printf("code: %d\n", code);

        topo->data[topo->data_idx] |= (1 << topo->bit_idx);
        // last index in the byte we just wrote to
        // update bit_idx
        if (topo->bit_idx == 0) {
            topo->data[++topo->data_idx] = code;
            topo->bit_idx = 7;
            topo->data_idx++; // point to the next byte after the code
        } else {
            // decrement bit_idx, since we set 1 there
            // so we can set code bits
            topo->bit_idx--;

            // 7654 3210 7654 3210 <- bit indexes
            // 0001_0000 0000_0000

            byte first_bits = code >> (7 - topo->bit_idx);
            topo->data[topo->data_idx++] |= first_bits;

            int to_shift = 8 - (7 - topo->bit_idx);
            byte last_bits = (code & (0xFF >> to_shift)) << to_shift;

            byte _test = (first_bits << (7 - topo->bit_idx)) | (last_bits >> to_shift);
            assert(_test == code);
            topo->data[topo->data_idx] |= last_bits;
        }
    } else { // non-leaf/intermediate node

        // set bit to 0 at bit_idx

        if (topo->bit_idx == 0) {
            topo->bit_idx = 7;
            topo->data_idx++;
        } else {
            topo->bit_idx--;
        }
    }
}

void dfs_encode(huff_node *node, int8_t *bit_length, topology *topo, uint16_t bits) {
    if (node->left_child != NULL) {
        (*bit_length)++;
        // set bit to 0
        int bit_idx = (~((*bit_length) - 16) + 1);
        bits &= ~(0 << bit_idx);
        // printf("GOING LEFT Bits in node: ");
        // for (int i = 15; i > 0; i--)
        //     printf("%u", bits & (uint16_t)(0 | (1 << i)) ? 1 : 0);
        // printf("\n");
        // printf("SETTING: 0 --- bit_length: %d, bit_idx: %d\n", *bit_length, bit_idx);
        topo_bits++;
        dfs_encode(node->left_child, bit_length, topo, bits);
    }

    if (node->right_child != NULL) {
        (*bit_length)++;
        // set bit to 1
        int bit_idx = (~((*bit_length) - 16) + 1);

        bits |= (1 << bit_idx);
        // printf("GOING RIGHT Bits in node: ");
        // for (int i = 15; i > 0; i--)
        //     printf("%u", bits & (uint16_t)(0 | (1 << i)) ? 1 : 0);
        // printf("\n");
        // printf("SETTING: 1 --- bit_length: %d, bit_idx: %d\n", *bit_length, bit_idx);
        topo_bits++;
        dfs_encode(node->right_child, bit_length, topo, bits);
    }

    if (node->value >= 0) { // leaf node
        // printf("bit_length: %d\n", *bit_length);
        // printf("my weight: %d\n", node->weight);
        TOTAL_BITS += (uint64_t)(node->weight * (int)*bit_length);
        assert((*bit_length) <= 16);
        // printf("SETTING: VALUE --- bit_length: %d, new code: %d\n", *bit_length, bits);
        // table[val as usize] = (bits, *counter);
        // write

        // cell->bit_length = *bit_length;
        // cell->bits = bits;
        // huff_table[node->value] = cell;
        assert(encode_bit_table[node->value] == -1);
        assert(encode_table[node->value] == 0);
        encode_bit_table[node->value] = *bit_length;
        encode_table[node->value] = bits;

        // printf("my value: %d, code: %d\n", node->value, bits);
        assert(*bit_length > 0);
        if (*bit_length > max_bit_length)
            max_bit_length = *bit_length;
        if (*bit_length < MIN_BIT_LEN)
            MIN_BIT_LEN = *bit_length;
    }

    (*bit_length)--;

    write_topology(topo, node->value);
    free(node);
    nodes_freed++;
}

void dfs_encode_shared(huff_node *node, int8_t *bit_length, topology *topo, uint16_t bits) {
    if (node->left_child != NULL) {
        (*bit_length)++;
        // set bit to 0
        int bit_idx = (~((*bit_length) - 16) + 1);
        bits &= ~(0 << bit_idx);

        shared_topo_bits++;
        dfs_encode_shared(node->left_child, bit_length, topo, bits);
    }

    if (node->right_child != NULL) {
        (*bit_length)++;
        // set bit to 1
        int bit_idx = (~((*bit_length) - 16) + 1);
        bits |= (1 << bit_idx);

        shared_topo_bits++;
        dfs_encode_shared(node->right_child, bit_length, topo, bits);
    }

    if (node->value >= 0) { // leaf node

        SHARED_TOTAL_BITS += (uint64_t)(node->weight * (int)*bit_length);
        assert((*bit_length) <= 16);

        assert(shared_encode_bit_table[node->value] == -1);
        assert(shared_encode_table[node->value] == 0);
        shared_encode_bit_table[node->value] = *bit_length;
        shared_encode_table[node->value] = bits;

        assert(*bit_length > 0);
        if (*bit_length > shared_max_bit_length) shared_max_bit_length = *bit_length;
        if (*bit_length < SHARED_MIN_BIT_LEN) SHARED_MIN_BIT_LEN = *bit_length;
    }

    (*bit_length)--;

    write_topology(topo, node->value);
    free(node);
}

int huff_encode(byte *src, byte *dst, int size) {

    int dst_idx = 0;
    int free_bits = 8;

    byte in_process = 0;

    for (int i = 0; i < size; i++) {
        // printf("bits left: %d\n", free_bits);
        uint16_t new_code = encode_table[src[i]];
        int8_t bit_length = encode_bit_table[src[i]];

        if (free_bits == 8) { //
            if (bit_length <= 8) {
                in_process = new_code >> 8;
                free_bits -= bit_length;
            } else {
                // new code takes 9 or more bits
                in_process = new_code >> 8;       // take first 8 bits
                dst[dst_idx++] = in_process;      // add it to destination
                in_process = (byte)new_code;      // take remaming bits
                free_bits = 8 - (bit_length - 8); // how many bits left until byte is full?
            }

        } else { // we have some bits already set after previous encoding cycle

            if (free_bits >= bit_length) { // if new code fits into remaining unoccupied bits
                in_process |= (new_code >> 8) >> (8 - free_bits);
                free_bits -= bit_length; // how many bits left until byte is full?
            } else {                     // if new code does not fit into remaining bits
                in_process |= ((new_code >> 8) >> (8 - free_bits));
                dst[dst_idx++] = in_process; // add it to destination

                //  1234 5678 9012 3456
                //  0000_0000_0000_0000

                new_code <<= free_bits;
                bit_length -= free_bits;

                in_process = (byte)(new_code >> 8);
                if (bit_length >= 8) {
                    dst[dst_idx++] = in_process; // add it to destination
                    new_code <<= 8;
                    bit_length -= 8;
                }
                assert(bit_length <= 8);
                in_process = (byte)(new_code >> 8);
                free_bits = 8 - bit_length;
            }
        }
        assert(free_bits >= 0);
        if (free_bits == 0) {
            dst[dst_idx++] = in_process;
            in_process = 0;
            free_bits = 8;
        }
    }

    dst[dst_idx++] = in_process; // add it to destination

    return dst_idx;
}

int huff_encode_shared(byte *src, byte *dst, int size) {

    int dst_idx = 0;
    int free_bits = 8;

    byte in_process = 0;

    for (int i = 0; i < size; i++) {
        // printf("bits left: %d\n", free_bits);
        uint16_t new_code = shared_encode_table[src[i]];
        int8_t bit_length = shared_encode_bit_table[src[i]];

        if (free_bits == 8) {      // all bits are free
            if (bit_length <= 8) { // if new code fits in 8-bit code
                in_process = new_code >> 8;
                free_bits -= bit_length;
            } else {
                // new code takes 9 or more bits
                in_process = (byte)(new_code >> 8); // take first 8 bits
                dst[dst_idx++] = in_process;        // add it to destination
                in_process = (byte)new_code;        // take remaming bits
                free_bits = 8 - (bit_length - 8);   // how many bits left until byte is full?
            }

        } else { // we have some bits already set after previous encoding cycle

            if (free_bits >= bit_length) { // if new code fits into remaining unoccupied bits
                in_process |= ((new_code >> 8) >> (8 - free_bits));
                free_bits -= bit_length; // how many bits left until byte is full?
            } else {                     // if new code does not fit into remaining bits
                in_process |= ((new_code >> 8) >> (8 - free_bits));
                dst[dst_idx++] = in_process; // add it to destination

                //  1234 5678 9012 3456
                //  0000_0000_0000_0000

                new_code <<= free_bits;
                bit_length -= free_bits;

                if (bit_length >= 8) {
                    in_process = (byte)(new_code >> 8);
                    dst[dst_idx++] = in_process; // add it to destination
                    new_code <<= 8;
                    bit_length -= 8;
                }
                assert(bit_length <= 8);
                in_process = (byte)(new_code >> 8);
                free_bits = 8 - bit_length;
            }
        }

        assert(free_bits >= 0);

        if (free_bits == 0) {
            dst[dst_idx++] = in_process;
            in_process = 0;
            free_bits = 8;
        }
    }

    dst[dst_idx++] = in_process; // add it to destination

    return dst_idx;
}

// is not actual priority queue
void make_priority_queue() {
    for (int i = 0; i < 256; i++) {
        if (WEIGHTS[i] > 0) {
            add_node(make_node(WEIGHTS[i], (int16_t)i));
            total_bytes += WEIGHTS[i];
        }
    }
}

void make_priority_queue_shared() {
    for (int i = 0; i < 256; i++) {
        if (SHARED_WEIGHTS[i] > 0) {
            add_node_shared(make_node(SHARED_WEIGHTS[i], (int16_t)i));
            shared_total_bytes += SHARED_WEIGHTS[i];
        }
    }
}

// void free_tree_(huff_node *node) {
//     if (node == NULL)
//         return;

//     free_tree_(node->left_child);
//     free_tree_(node->right_child);
//     free(node);
//     // nodes_freed++;
// }

topology *create_new_codes() {
    make_priority_queue();
    huff_node *encode_root = make_tree();

    assert(heap_nodes == 0);

    int8_t bit_length = 0;
    topology *topo = init_topo();
    uint16_t bits = 0;
    dfs_encode(encode_root, &bit_length, topo, bits);

    assert(nodes_created == nodes_freed);

    for (int i = 0; i < 256; i++)
        assert(NODES[i] == NULL);

    return topo;
}

topology *create_new_codes_shared() {

    // int sum = 0;

    // for (int i = 0; i < 256; i++)
    //     sum += SHARED_WEIGHTS[i];
    // printf("sum: %d\n", sum);

    make_priority_queue_shared();
    huff_node *encode_root = make_tree_shared();

    assert(shared_heap_nodes == 0);

    int8_t bit_length = 0;
    topology *topo = init_topo();
    uint16_t bits = 0;
    dfs_encode_shared(encode_root, &bit_length, topo, bits);

    // for (int i = 0; i < 256; i++) {
    //     if (shared_encode_bit_table[i] != -1) {
    //         printf("%d: %d, ", i, shared_encode_bit_table[i]);
    //     }
    // }

    // printf("\n");

    return topo;
}

int __compress(byte *src, byte *dst, int size) {
    reset_encode_vars();

    int result = -1; // no point to compress by default

    count_freq(src, size);

    topology *topo = create_new_codes();

    int compressed = (TOTAL_BITS / 8) + 1;
    int topo_size = topo->data_idx + 1;
    int total = compressed + topo_size + 3 + 2; // 3-original word size, 2-topo-size
    if (total < size) {                         // if it makes sense to compress it
        dst[0] = COMPRESSED;                    // set flag as compressed

        dst[4] = dst[1]; // original word size
        dst[5] = dst[2]; // original word size
        dst[6] = dst[3]; // original word size

        dst[7] = (byte)topo_size >> 8;
        dst[8] = (byte)topo_size;

        // [0, 1, 2, 3] - flag and size of tatal compressed data
        // [4, 5, 6] - size of original data
        // [7, 8] - size of topology
        // [9..topo_size-1] - topology itself
        // [topo_size...compressed_size-1] - compressed word itself

        int i = 9, j = 0;
        while (j <= topo->data_idx)
            dst[i++] = topo->data[j++];

        assert(j == topo_size);
        int data_start = i;
        assert(data_start == 9 + topo_size);
        int compressed_size = huff_encode(src, &dst[data_start], size);
        // if (compressed != compressed_size) {
        //     printf("compressed: %d, compressed_size: %d\n", compressed, compressed_size);
        // }
        assert(compressed_size == compressed);
        total = compressed_size + topo_size + 3 + 2;
        dst[1] = (byte)(total >> 16); // total encoded size
        dst[2] = (byte)(total >> 8);  // total encoded size
        dst[3] = (byte)total;         // total encoded size

        result = total;
        // result = compressed_size + topo_size + 3 + 2; // +3 additional 3 bytes for original word size

        // for (int i = 4; i < 9 + topo_size; i++)
        //     printf("%d ", dst[i]);
        // printf("\n");

        // reset_decode_vars();
        // // assert(data_start + size < size + 1024);
        // huff_node *decode_root = tree_from_topo(&dst[9], j);
        // int8_t bit_length = 0;
        // uint16_t bits = 0;

        // int t = size;
        // dfs_decode(decode_root, &bit_length, bits);
        // byte *test = malloc(sizeof(byte) * size);
        // int d_idx = huff_decode2(&dst[data_start], test, t, MIN_BIT_LEN);
        // assert(d_idx <= size);
        // // for (int i = 0, j = data_start; i < size; i++)
        // //     printf("dst: %d\n", dst[j++]);

        // for (int i = 0; i < t; i++)
        //     assert(src[i] == test[i]);

        // printf("Got here\n");
        // // int t_size = __decompress(&dst[4], test);
        // // assert(t_size == topo->data_idx);
        // free(test);

    } else {
        if (compressed < size && total > size && size < LARGE_INPUT_SIZE) {
            // set flag as small input, may show better result when combined with other
            // small inputs
            SHARED_SMALL_WORDS++;
            dst[0] = SHARED;
            count_freq_shared(src, size);
            UNCOMPRESSED_BYTES_SIZE += size;
        }

        for (int i = 0; i < size; i++)
            dst[i + 4] = src[i];
    }

    free(topo);

    return result;
}

int __compress_shared(byte *src, byte *dst, int size) {

    dst[0] = SHARED_COMPRESSED;
    dst[4] = src[1]; // original word size
    dst[5] = src[2]; // original word size
    dst[6] = src[3]; // original word size

    int a = ((int)src[1] << 16 | (int)src[2] << 8 | (int)src[3]);

    int result = huff_encode_shared(&src[4], &dst[7], size) + 3;

    dst[1] = (byte)(result >> 16);
    dst[2] = (byte)(result >> 8);
    dst[3] = (byte)(result);

    return result;
}