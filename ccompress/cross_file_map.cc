#include "cross_file_map.h"

#include <iostream>

m_file *CMmapRead(const char *file_name) {

    m_byte *buf;
#ifdef _WIN32

    HANDLE hFile, hMapFile;

    hFile = CreateFileA(file_name, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);

    if (hFile == INVALID_HANDLE_VALUE) {
        std::cout << "Failed to open file: "
                  << "INVALID_HANDLE_VALUE"
                  << "\n";
        if (GetLastError() == ERROR_FILE_NOT_FOUND) {
            std::cout << "GetLastError() == ERROR_FILE_NOT_FOUND"
                      << "\n";
        }

        return NULL;
    }

    auto size = GetFileSize(hFile, NULL);

    hMapFile = CreateFileMappingA(hFile, NULL, PAGE_READONLY, 0, 0, NULL);

    if (hMapFile == NULL) {
        std::cout << "Could not create file mapping"
                  << "\n";
        std::cout << "GetLastError(): " << GetLastError() << "\n";
        return NULL;
    }

    buf = (m_byte *)MapViewOfFile(hMapFile, FILE_MAP_READ, 0, 0, size);
    if (buf == NULL) {
        std::cout << "Could not create view of file"
                  << "\n";
        std::cout << "GetLastError(): " << GetLastError() << "\n";
        CloseHandle(hMapFile);
        CloseHandle(hFile);
        return NULL;
    }
    CloseHandle(f->mh); // TODO double check this, we may dont need to keep HANDLES
    CloseHandle(f->fh);
    return new m_file(buf, hFile, hMapFile, size);

#else

    int fd;
    size_t size;
    struct stat _stat;

    fd = open(file_name, O_RDONLY);
    if (fd == -1) {
        std::cout << "Failed to open file: " << file_name << "\n";
        return NULL;
    }

    int r = fstat(fd, &_stat);
    if (r == -1) {
        std::cout << "Failed to get file size"
                  << "\n";
        return NULL;
    }

    size = _stat.st_size;

    buf = (m_byte *)mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
    if ((*buf) == -1) {
        std::cout << "Could not create file mapping"
                  << "\n";
        return NULL;
    }

    close(fd);

    return new m_file(buf, 0, 0, size);

#endif

    return NULL;
}

int CMunMap(m_file *f) {
#ifdef _WIN32
    UnmapViewOfFile(f->buf);
    delete f;
    return 0;
#else
    int r = munmap(f->buf, f->size);
    if (r == -1) {
        std::cout << "Failed to 'munmap'"
                  << "\n";
        return -1;
    }

    delete f;
    return 0;
#endif

    return 0;
}