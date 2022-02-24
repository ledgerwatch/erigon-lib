#ifndef _DECOMPRESS_H
#define _DECOMPRESS_H 1

typedef struct decompress {
    int t;
} decompress;

extern decompress *test_d();

extern void free_ptrs(void *p0);

#endif /* _DECOMPRESS_H*/