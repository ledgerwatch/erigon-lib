#ifndef _RANDOMS_H
#define _RANDOMS_H

#include <cassert>
#include <iostream>
#include <limits>
#include <random>
#include <vector>

//     std::vector<uint8_t> rand_bytes(int size) {
//         std::random_device rd;
//         std::mt19937 gen(rd());
//         std::uniform_int_distribution<> distrib(0, 255);

//         std::vector<uint8_t> v(size);
//         for (int i = 0; i < size; i++) {
//             v[i] = distrib(gen);
//         }
//         return v;
//     }

class Rand {
private:
    std::mt19937 _rd;

public:
    Rand() : _rd(std::random_device()()) {}
    ~Rand() {}

    uint8_t rand_byte(int min, int max) {
        return std::uniform_int_distribution<uint8_t>(min, max)(_rd);
    }

    // this will likely create byte array that requires no compresseion
    // (compressed size >= uncompressed size)
    std::vector<uint8_t> rand_bytes_255(int min_size, int max_size) {
        int size = rand_int_range(min_size, max_size);
        std::vector<uint8_t> result(size);

        for (int i = 0; i < size; i++)
            result[i] = rand_byte(0, 255);

        return result;
    }

    std::vector<uint8_t> rand_bytes_repeated(int min_size, int max_size) {
        int size = rand_int_range(min_size, max_size);
        int size_copy = size;
        std::vector<uint8_t> result(size);

        int parts = size / rand_int_range(4, 20);

        int j = 0, part_size;
        int part = (int)(size / parts);
        uint8_t b;
        int min_byte, max_byte;
        for (int p = 0; p < parts; p++) {
            min_byte = rand_int_range(0, 126);
            max_byte = rand_int_range(127, 255);
            if (size_copy - part > 0) {
                // b = rand_byte(0, 127);
                b = rand_byte(min_byte, max_byte);
                part_size = rand_int_range(2, part);
                for (int q = 0; q < part_size; q++)
                    result[j++] = b;
                size_copy -= part;
                part = part_size;
            } else {
                break;
            }
        }

        while (j < size) {
            min_byte = rand_int_range(0, 126);
            max_byte = rand_int_range(127, 255);
            result[j++] = rand_byte(min_byte, max_byte);
        }

        assert(j == size);
        return result;
    }

    std::vector<uint8_t> rand_bytes(int min_size, int max_size) {
        int size = rand_int_range(min_size, max_size);
        std::vector<uint8_t> result(size);

        int min_byte, max_byte;
        for (int i = 0; i < size; i++) {
            min_byte = rand_int_range(0, 126);
            max_byte = rand_int_range(127, 255);
            result[i] = rand_byte(min_byte, max_byte);
            // result[i] = rand_byte(0, 127);
        }

        return result;
    }

    int rand_int_range(int min, int max) {
        return std::uniform_int_distribution<int>(min, max)(_rd);
    }

    uint32_t rand_odd_32() {
        auto d = std::uniform_int_distribution<uint32_t>(0x01000001, 0x0FFFFFFF)(_rd);
        if ((d & 1) == 0)
            d ^= 1;
        return d;
    }
};

#endif