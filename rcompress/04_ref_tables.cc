#include "04_encoding_assets.h"

// consider making

//       Extra           Extra                 Extra

//  Code Bits IDXs   Code Bits  IDXs      Code Bits  IDXs

//  ---- ---- ----   ---- ----  ------     ---- ---- --------

//    0   1   0,1      10   6   124-187      20   11  4092-6139

//    1   1   2,3      11   6   188-251      21   11  6140-8187

//    2   2   4-7      12   7   252-379      22   12  8188-12283

//    3   2   8-11     13   7   380-507      23   12  12284-16379

//    4   3   12-19    14   8   508-763      24   13  16380-24571

//    5   3   20-27    15   8   764-1019     25   13  24572-32763

//    6   4   28-43    16   9   1020-1531    26   14  32764-49147

//    7   4   44-59    17   9   1532-2043    27   15  49148-81915

//    8   5   60-91    18  10   2044-3067    28   16  81916-147451

//    9   5   92-123   19  10   3068-4091    29   17  147452-278523

//                                           30   18  278524-540667

//                                           31   19  540668-1064955

// max prefixes = 1064956

std::array<uint8_t, R_MAX_PREFIXES> prefix_id_codes;

void init_prefix_id_codes() {
    prefix_id_codes[0] = 0;
    prefix_id_codes[1] = 0;
    prefix_id_codes[2] = 1;
    prefix_id_codes[3] = 1;

    int i = 4;
    for (; i < 8; i++)
        prefix_id_codes[i] = 2;

    for (; i < 12; i++)
        prefix_id_codes[i] = 3;

    for (; i < 20; i++)
        prefix_id_codes[i] = 4;

    for (; i < 28; i++)
        prefix_id_codes[i] = 5;

    for (; i < 44; i++)
        prefix_id_codes[i] = 6;

    for (; i < 60; i++)
        prefix_id_codes[i] = 7;

    for (; i < 92; i++)
        prefix_id_codes[i] = 8;

    for (; i < 124; i++)
        prefix_id_codes[i] = 9;

    for (; i < 188; i++)
        prefix_id_codes[i] = 10;

    for (; i < 252; i++)
        prefix_id_codes[i] = 11;

    for (; i < 380; i++)
        prefix_id_codes[i] = 12;

    for (; i < 508; i++)
        prefix_id_codes[i] = 13;

    for (; i < 764; i++)
        prefix_id_codes[i] = 14;

    for (; i < 1020; i++)
        prefix_id_codes[i] = 15;

    for (; i < 1532; i++)
        prefix_id_codes[i] = 16;

    for (; i < 2044; i++)
        prefix_id_codes[i] = 17;

    for (; i < 3068; i++)
        prefix_id_codes[i] = 18;

    for (; i < 4092; i++)
        prefix_id_codes[i] = 19;

    for (; i < 6140; i++)
        prefix_id_codes[i] = 20;

    for (; i < 8188; i++)
        prefix_id_codes[i] = 21;

    for (; i < 12284; i++)
        prefix_id_codes[i] = 22;

    for (; i < 16380; i++)
        prefix_id_codes[i] = 23;

    for (; i < 24572; i++)
        prefix_id_codes[i] = 24;

    for (; i < 32764; i++)
        prefix_id_codes[i] = 25;

    for (; i < 49148; i++)
        prefix_id_codes[i] = 26;

    for (; i < 81916; i++)
        prefix_id_codes[i] = 27;

    for (; i < 147452; i++)
        prefix_id_codes[i] = 28;

    for (; i < 278524; i++)
        prefix_id_codes[i] = 29;

    for (; i < 540668; i++)
        prefix_id_codes[i] = 30;

    for (; i < R_MAX_PREFIXES; i++)
        prefix_id_codes[i] = 31;
}

int get_prefix_id_code(int rp_idx) {
    assert(rp_idx >= 0);
    assert(rp_idx < R_MAX_PREFIXES);
    return prefix_id_codes[rp_idx];
}

//       Extra           Extra               Extra

//  Code Bits Dist  Code Bits   Dist     Code Bits Distance

//  ---- ---- ----  ---- ----  ------    ---- ---- --------

//    0   0    1     10   4     33-48    20    9   1025-1536

//    1   0    2     11   4     49-64    21    9   1537-2048

//    2   0    3     12   5     65-96    22   10   2049-3072

//    3   0    4     13   5     97-128   23   10   3073-4096

//    4   1   5,6    14   6    129-192   24   11   4097-6144

//    5   1   7,8    15   6    193-256   25   11   6145-8192

//    6   2   9-12   16   7    257-384   26   12  8193-12288

//    7   2  13-16   17   7    385-512   27   12 12289-16384

//    8   3  17-24   18   8    513-768   28   13 16385-24576

//    9   3  25-32   19   8   769-1024   29   13 24577-32768

std::array<uint8_t, 32769> dict_distancecs;

void init_dict_dist_codes() {
    dict_distancecs[1] = 0;
    dict_distancecs[2] = 1;
    dict_distancecs[3] = 2;
    dict_distancecs[4] = 3;

    int i = 5;
    for (; i < 7; i++)
        dict_distancecs[i] = 4;

    for (; i < 9; i++)
        dict_distancecs[i] = 5;

    for (; i < 13; i++)
        dict_distancecs[i] = 6;

    for (; i < 17; i++)
        dict_distancecs[i] = 7;

    for (; i < 25; i++)
        dict_distancecs[i] = 8;

    for (; i < 33; i++)
        dict_distancecs[i] = 9;

    for (; i < 49; i++)
        dict_distancecs[i] = 10;

    for (; i < 65; i++)
        dict_distancecs[i] = 11;

    for (; i < 97; i++)
        dict_distancecs[i] = 12;

    for (; i < 129; i++)
        dict_distancecs[i] = 13;

    for (; i < 193; i++)
        dict_distancecs[i] = 14;

    for (; i < 257; i++)
        dict_distancecs[i] = 15;

    for (; i < 385; i++)
        dict_distancecs[i] = 16;

    for (; i < 513; i++)
        dict_distancecs[i] = 17;

    for (; i < 769; i++)
        dict_distancecs[i] = 18;

    for (; i < 1025; i++)
        dict_distancecs[i] = 19;

    for (; i < 1537; i++)
        dict_distancecs[i] = 20;

    for (; i < 2049; i++)
        dict_distancecs[i] = 21;

    for (; i < 3073; i++)
        dict_distancecs[i] = 22;

    for (; i < 4097; i++)
        dict_distancecs[i] = 23;

    for (; i < 6145; i++)
        dict_distancecs[i] = 24;

    for (; i < 8193; i++)
        dict_distancecs[i] = 25;

    for (; i < 12289; i++)
        dict_distancecs[i] = 26;

    for (; i < 16385; i++)
        dict_distancecs[i] = 27;

    for (; i < 24577; i++)
        dict_distancecs[i] = 28;

    for (; i < 32769; i++)
        dict_distancecs[i] = 29;
}

int get_dict_dist_code(int d) {
    assert(d <= 32768);

    return dict_distancecs[d];
}