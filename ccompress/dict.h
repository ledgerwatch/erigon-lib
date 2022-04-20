#ifndef CCOMPRESS_DICT_
#define CCOMPRESS_DICT_

#include "defs.h"
#include "records.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <iostream>
#include <list>
#include <vector>

// Two hash tables (caches). "curr_table" keeps track of indexes of the block that is being
// currently encoded. "prev_table" contains all indexes of previously encoded block.
class Dict {
private:
    std::array<std::list<int>, TABLE_SIZE> curr_table;
    std::array<std::list<int>, TABLE_SIZE> prev_table;

public:
    Dict() {}

    ~Dict() {}

    void reset() {
        for (int i = 0; i < TABLE_SIZE; i++) {
            this->prev_table[i] = std::move(this->curr_table[i]);
            this->curr_table[i].clear();
        }
    }

    void insert(int at, int i) { this->curr_table[at].push_front(i); }

    record *match_longest(std::vector<uint8_t> *src, std::vector<uint8_t> *prev_block, int at, int i, int n) {

        int a, b, c, d;
        int j, k, temp;
        int longest = 0;
        int start, where;

        int src_size = src->size();
        int prev_size;
        if (prev_block != NULL && i < MAX_DISTANCE) {
            prev_size = prev_block->size();

            int range_start = prev_size + (i - MAX_DISTANCE);
            assert(range_start > 0);

            for (auto it = this->prev_table[at].begin(); it != this->prev_table[at].end(); ++it) {
                int idx = *it;

                if (idx >= range_start && idx + 2 < prev_size) {

                    a = prev_block->at(idx), b = prev_block->at(idx + 1), c = prev_block->at(idx + 2);
                    d = (a << 16) | (b << 8) | c;

                    if (d == n) {

                        j = i + 3, k = idx + 3, temp = 3;
                        while (k < prev_size && j < src_size && temp < 258 && src->at(j) == prev_block->at(k))
                            j++, k++, temp++;

                        int m = idx, n = i;
                        int l = 0;
                        for (; l < temp; l++)
                            assert(prev_block->at(m++) == src->at(n++));

                        int t = temp;

                        if (k == prev_size) {
                            k = 0;
                            while (j < src_size && temp < 258 && src->at(j) == src->at(k)) {
                                assert(j < src_size && k < src_size);
                                j++, k++, temp++;
                            }

                            for (; l < t; l++)
                                assert(src->at(m++) == src->at(n++));
                        }

                        if (temp >= longest) {
                            longest = temp;
                            start = idx; // here idx is the index in prev block
                            where = i;
                            // std::cout << "\nfound: "
                            //           << "\n";
                            // std::cout << "longest: " << temp << "\n";
                            // std::cout << "start: " << idx << "\n";
                            // std::cout << "where: " << i << "\n";
                        }
                    }
                }
            }
        }

        for (auto it = this->curr_table[at].begin(); it != this->curr_table[at].end(); ++it) {
            int idx = *it;
            assert(idx < i);
            if (i - idx > MAX_DISTANCE) {
                // this->table[at].erase(it, this->table[at].end());
                // std::cout << "erasing" << "\n";
                return longest == 0 ? NULL : new record(start, where, longest);
            }
            assert(idx + 2 < src_size);
            a = src->at(idx), b = src->at(idx + 1), c = src->at(idx + 2);
            d = (a << 16) | (b << 8) | c;

            if (d == n) {
                j = i + 3, k = idx + 3, temp = 3;
                while (j < src_size && temp < 258 && src->at(j) == src->at(k)) {
                    assert(j < src_size && k < src_size);
                    j++, k++, temp++;
                }

                if (temp >= longest) {
                    longest = temp;
                    start = idx;
                    where = i;
                }
            }
        }

        return longest == 0 ? NULL : new record(start, where, longest);
    }
};

#endif