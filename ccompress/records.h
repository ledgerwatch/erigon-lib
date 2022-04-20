#ifndef CCOMPRESS_RECORDS_
#define CCOMPRESS_RECORDS_

#include <cstddef>
#include <vector>

typedef struct record {
    int start;  // back reference to the start of the repeated sequence
    int where;  // a position where repeated sequence occur
    int length; // length of repetition

    record(int s, int w, int l) : start(s), where(w), length(l){};
} record;

class Records {
private:
    std::vector<record *> records;

public:
    Records(/* args */) {
        this->records.reserve(1024);
    }
    ~Records() {
        this->reset();
    }

    void reset() {
        for (auto ptr : records)
            delete ptr;
        this->records.clear();
    }

    void push_back(record *r) {
        this->records.push_back(r);
    }

    int size() {
        return this->records.size();
    }

    record *at(int idx) {
        if (idx < this->size())
            return this->records[idx];

        return NULL;
    }
};

#endif
