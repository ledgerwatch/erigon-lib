#include "rutils.h"

void LcpKasai(const unsigned char *SRC, int *SA, int *LCP, int *AUX, int src_size) {

    int i = 0, j = 0, k = 0;

    for (i = 0; i < src_size; i++)
        AUX[SA[i]] = i;

    for (i = 0; i < src_size; i++, k ? k-- : 0) {
        if (AUX[i] == src_size - 1) {
            k = 0;
            continue;
        }

        j = SA[AUX[i] + 1];
        while (i + k < src_size && j + k < src_size && SRC[i + k] == SRC[j + k])
            k++;

        LCP[AUX[i]] = k;
    }
}
