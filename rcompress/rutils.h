#ifndef R_UTILS
#define R_UTILS

#ifdef __cplusplus

#else

#endif // __cplusplus

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

#if defined(__STDC__) || defined(__cplusplus)
extern void LcpKasai(const unsigned char *SRC, int *SA, int *LCP, int *AUX, int src_size);
#else
extern void LcpKasai(const unsigned char *SRC, int *SA, int *LCP, int *AUX, int sa_size);
#endif

#ifdef __cplusplus
}
#endif // __cplusplus

#endif