#include "file.h"

// Open existing database file or create one if not existed
int64_t file_open_database_file(char* path) {
	fd = open(path, O_RDWR|O_SYNC|O_CREAT|O_EXCL);
	if ((fd < 0) && (errno == EEXIST)) {
		fd = open(path, O_RDWR|O_SYNC);
		return fd;
	}
	else if (fd < 0) {
		perror("File open failed.");
		exit(1);
	}

	const pagenum_t page_num = 10 * 0x100000 / page_size;

	free_page* tmp;
	tmp = (free_page*)malloc(sizeof(free_page) * page_num);
	int i;
	for (i = 1; i < page_num; i++) {
		tmp[i].next_page = i - 1;
	}
	
	lseek(fd, 0, SEEK_SET);
	if (write(fd, tmp, page_size * page_num) < page_size * page_num) {
		perror("Write failed.");
		exit(1);
	}

	head_page header;
	header.free_num = page_num - 1;
	header.page_num = page_num;

	lseek(fd, 0, SEEK_SET);
	if (write(fd, &header, page_size) < page_size) {
		perror("Write failed.");
		exit(1);
	}
	
	return fd;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page() {
	page_t* header;
	file_read_page(0, header);
	
	pagenum_t num;

	if (header->free_num == 0) {
		num = header->page_num;
		if (num == UINT64_MAX) {
			perror("Allocation failed.");
			exit(1);
		}

		free_page tmppage;
		pagenum_t tmpnum;

		tmppage.next_page = 0;
		tmpnum = num;
		lseek(fd, tmpnum * page_size, SEEK_SET);
		if (write(fd, &tmppage, page_size) < page_size) {
			perror("Write failed.");
			exit(1);
		}
		for(tmpnum = num + 1; tmpnum < 2 * num && tmpnum < UINT64_MAX; tmpnum++) {
			tmppage.next_page = tmpnum - 1;
			if (write(fd, &tmppage, page_size) < page_size) {
				perror("Write failed.");
				exit(1);
			}
		}

		header->free_num = tmpnum - 1;
		header->page_num = tmpnum;
		file_write_page(0, header);
		return tmpnum - 1;
	}
	else {
		num = header->free_num;
		free_page tmp;

		lseek(fd, num * page_size, SEEK_SET);
		if (read(fd, &tmp, page_size) < page_size) {
			perror("Read failed.");
			exit(1);
		}
		
		header->free_num = tmp.next_page;

		file_write_page(0, header);
		return num;
	}
}

// Free an on-disk page to the free page list
void file_free_page(pagenum_t pagenum) {
	page_t* header;
	file_read_page(0, header);

	free_page tmp;
	tmp.next_page = header->free_num;
	header->free_num = pagenum;
	
	lseek(fd, pagenum * page_size, SEEK_SET);
	if (write(fd, &tmp, page_size) < page_size) {
		perror("Write failed.");
		exit(1);
	}

	file_write_page(0, header);
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(pagenum_t pagenum, page_t* dest) {
	lseek(fd, pagenum * page_size, SEEK_SET);
	if (pagenum == 0) {
		head_page tmp;

		lseek(fd, 0, SEEK_SET);
		if (read(fd, &tmp, page_size) < page_size) {
			perror("Read failed.");
			exit(1);
		}

		dest->free_num = tmp.free_num;
		dest->page_num = tmp.page_num;
	}
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(pagenum_t pagenum, const page_t* src) {
	if (pagenum == 0) {
		head_page tmp;
		
		tmp.free_num = src->free_num;
		tmp.page_num = src->page_num;

		lseek(fd, pagenum * page_size, SEEK_SET);
		if (write(fd, &tmp, page_size) < page_size) {
			perror("Write failed.");
			exit(1);
		}
	}
}

// Stop referencing the database file
void file_close_database_file() {
	if (close(fd) < 0) {
		perror("Close failed.");
		exit(1);
	}
}
