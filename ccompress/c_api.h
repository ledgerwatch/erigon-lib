#ifndef _C_API
#define _C_API 1

#include <stdio.h>
#include <stdlib.h>

#define ERROR_FOPEN -1

typedef struct compressor {
    FILE *idt; // intermediate file
    FILE *fp;  // final results file
    char *idt_file_name;
} compressor;

typedef struct decompressor {
    unsigned char *data;
    int size;
    int current;
    int *sizes;
    int data_offset;
} decompressor;

compressor *cmp;
decompressor *dcmp;


extern int new_compressor(const char *out_file, const char *idt_file);
extern void add_word(unsigned char *word, int size);
extern void compress();
extern void close_compressor();

extern int new_decompressor(unsigned char *data, int size);
extern int next(unsigned char *dst);
extern int has_next();
extern void close_decompressor();

#endif // _C_API