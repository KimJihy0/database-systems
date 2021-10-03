#ifndef DB_FILE_H_
#define DB_FILE_H_

#include "page.h"
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

#define PAGE_SIZE 0x1000 								// 4KiB
#define INITIAL_FILESIZE (10 * 0x100000) 				// 10MiB
#define INITIAL_PAGENUM (INITIAL_FILESIZE / PAGE_SIZE) 	// 2560
#define SLOT_SIZE 12
#define FREE_SPACE 3968
#define OFFSET(X) ((X) - 128)

int file_open_database_file(const char* pathname);
pagenum_t file_alloc_page(int fd);
void file_free_page(int fd, pagenum_t pagenum);
void file_read_page(int fd, pagenum_t pagenum, page_t* dest);
void file_write_page(int fd, pagenum_t pagenum, const page_t* src);
void file_close_database_file();

#endif	// DB_FILE_H_
