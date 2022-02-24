#include "decompress.h"

#include <stdlib.h>

decompress *test_d() {
    decompress *ptr = malloc(sizeof(decompress) * 1);
    return ptr;
}

void free_ptrs(void *p0) {
    free(p0);
}