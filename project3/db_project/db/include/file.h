#ifndef DB_FILE_H_
#define DB_FILE_H_

#include "page.h"
#include "hash.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#define PAGE_SIZE (4 * 1024)							// 4KiB
#define INITIAL_FILESIZE (10 * 1024 * 1024)				// 10MiB
#define INITIAL_PAGENUM (INITIAL_FILESIZE / PAGE_SIZE) 	// 2560

extern table_t tables[];

int64_t file_open_table_file(const char* pathname);
pagenum_t file_alloc_page(int64_t table_id);
pagenum_t file_double_page(int64_t table_id, uint64_t num_pages);
void file_free_page(int64_t table_id, pagenum_t pagenum);
void file_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest);
void file_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src);
void file_close_table_file();

#endif	// DB_FILE_H_
