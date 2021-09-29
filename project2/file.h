#pragma once

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#define page_size 0x1000 								// 4KiB
#define default_filesize (10 * 0x100000) 				// 10MiB
#define default_pagenum (default_filesize / page_size) 	// 2560

dbfile* fds;

typedef uint64_t pagenum_t;

struct page_t {
	uint64_t free_num;
	uint64_t page_num;
	uint64_t next_frpg;
	char data[4072];
};

struct head_page {
	uint64_t free_num;
	uint64_t page_num;
	char data[4080];
};

struct free_page {
	uint64_t next_frpg;
	char data[4088];
};

struct aloc_page {
	char data[4096];
};

struct dbfile {
	int fd;
	dbfile* next;
};

// Open existing database file or create one if not existed.
int file_open_database_file(const char* pathname);

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int fd);

// Free an on-disk page to the free page list
void file_free_page(int fd, pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int fd, pagenum_t pagenum, page_t* dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(int fd, pagenum_t pagenum, const page_t* src);

// Close the database file
void file_close_database_file();