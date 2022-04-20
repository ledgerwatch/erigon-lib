#ifndef CCOMPRESS_DEFS_
#define CCOMPRESS_DEFS_

// #include <limits>
#include <iostream>

#define SHIFT_BITS 14
#define TABLE_SIZE (1 << SHIFT_BITS)
#define HASH_BITS (32 - SHIFT_BITS)
#define HASH_FUNC(A, x) (A * x) >> HASH_BITS

#define LITERAL_ALPHABET 256 // literal chars 0..255
#define LENGTH_ALPHABET 30   // length of repetition 257..285
#define LL_ALPHABET (LITERAL_ALPHABET + LENGTH_ALPHABET)
#define DISTANCE_ALPHABET 30 // distances alphabet
#define MAX_DISTANCE 32768   // 1 << 15, distance between first chars in repeated sequence

#define MANY_WORDS 0b00
#define WORD_START 0b01
#define WORD_CONTINUE 0b10
#define WORD_END 0b11

#define NOT_COMPRESSED 0
#define COMPRESSED 3

#define ALPHABET_BITS 5

#define COPY_X_3_6 21
#define COPY_0_3_10 22
#define COPY_0_11_138 23

#define BITS_X_3_6 2
#define BITS_0_3_10 3
#define BITS_0_11_138 7

#define MIN_X_3_6 3
#define MIN_0_3_10 3
#define MIN_0_11_138 11

#define MAX_WORD_SIZE ((1 << 24) - 1)

#endif