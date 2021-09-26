#pragma once

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>

#define page_size 4096

typedef uint64_t pagenum_t;

int fd;
page_t* header;

struct page_t {
	// in-memory page structure
	uint64_t this_num;
	// header
	uint64_t free_num;
	uint64_t page_num;
	// free page
	uint64_t next_page;
	// alocated page
	char Reserved[4064];
};

struct head_page {
	uint64_t free_num;	// 0-7
	uint64_t page_num;	// 8-15
	char Reserved[4088];
};

struct free_page {
	uint64_t next_page;	// 0-7
	char Reserved[4092];
};

struct aloc_page {
	//implement alocated page
};

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page();

// Free an on-disk page to the free page list
void file_free_page(pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(pagenum_t pagenum, page_t* dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(pagenum_t pagenum, const page_t* src);
