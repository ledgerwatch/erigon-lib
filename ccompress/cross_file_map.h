#ifndef CROSS_FILE_MAP_
#define CROSS_FILE_MAP_

typedef unsigned char m_byte;

#ifdef _WIN32
#include <windows.h>
#else
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#ifdef _WIN32
typedef HANDLE m_int;
#else
typedef int m_int;
#endif

#ifdef __cplusplus
struct m_file {
    m_byte *buf;
    m_int fh; // windows: file HANDLE, unix: file descriptor
    m_int mh; // windows: map HANDLE, uinx: 0
    size_t size;

    m_file(m_byte *b, m_int f, m_int m, int s) : buf(b), fh(f), mh(m), size(s){};
};

#else
typedef struct m_file m_file;
#endif

#ifdef __cplusplus
extern "C" {
#endif

#if defined(__STDC__) || defined(__cplusplus)
m_file *CMmapRead(const char *file_name);
int CMunMap(m_file *f);
#else
m_file *CMmapRead(const char *file_name);
int CMunMap(m_file *f);
#endif

#ifdef __cplusplus
}
#endif

#endif // CROSS_FILE_MAP_