#include "decompress.h"

#include <stdio.h>
#include <stdlib.h>

decompress *init_decompressor(const char *file_name) {
    decompress *d = malloc(sizeof(decompress));
    d->t = 123;
    printf("Created dummy struct\n");
    printf("%s\n", file_name);
    return d;
}

int my_func(decompress *dcmp) {
    printf("my value: %d\n", dcmp->t);
    return dcmp->t;
}

void close_decompressor(decompress *d) {
    printf("Freed dummy struct\n");

    // if (d->data != NULL)
    printf("We got some data: %d\n", d->data != NULL);
    free(d);
}