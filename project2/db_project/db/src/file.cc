#include "../include/file.h"

file_t* files;

// Open existing table file or create one if not existed
int64_t file_open_table_file(const char* pathname) {
	int fd;
	fd = open(pathname, O_RDWR|O_CREAT|O_EXCL);
	// file exists
	if (fd < 0 && errno == EEXIST) {
		fd = open(pathname, O_RDWR);
		if (fd < 0) {
			perror("Failure to open table file");
			exit(1);
		}
	}
	else if (fd < 0) {
		perror("Failure to open table file");
		exit(1);
	}
	// file not exists
	else {
		header_t header;
		header.free_num = INITIAL_PAGENUM - 1;
		header.num_pages = INITIAL_PAGENUM;
		header.root_num = 0;
		lseek(fd, 0, SEEK_SET);
		if (write(fd, &header, PAGE_SIZE) < PAGE_SIZE) {
			perror("Failure to write");
			exit(1);
		}
		sync();

		freepg_t freepg;
		int i;
		for (i = 1; i < INITIAL_PAGENUM; i++) {
			freepg.next_frpg = i - 1;
			if (write(fd, &freepg, PAGE_SIZE) < PAGE_SIZE) {
				perror("Failure to write");
				exit(1);
			}
			sync();
		}
	}

	// insert file descriptor into linked-list
	file_t* new_file;
	new_file = (file_t*)malloc(sizeof(file_t));
	if (new_file == NULL) {
		perror("New file node creation");
		exit(1);
	}
	new_file->fd = fd;
	new_file->next = files;
	files = new_file;

	return fd;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int fd) {
	header_t header;
	lseek(fd, 0, SEEK_SET);
	if (read(fd, &header, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to read");
		exit(1);
	}

	pagenum_t num;
	// free page not exists
	if (header.free_num == 0) {
		num = header.num_pages;
		if (num == UINT64_MAX) {
			perror("Failure to allocate page(too many allocated pages)");
			exit(1);
		}

		freepg_t tmp_page;
		tmp_page.next_frpg = 0;
		lseek(fd, num * PAGE_SIZE, SEEK_SET);
		if (write(fd, &tmp_page, PAGE_SIZE) < PAGE_SIZE) {
			perror("Failure to write");
			exit(1);
		}
		sync();

		pagenum_t tmp_num;
		for(tmp_num = num + 1; tmp_num < 2 * num && tmp_num < UINT64_MAX; tmp_num++) {
			tmp_page.next_frpg = tmp_num - 1;
			if (write(fd, &tmp_page, PAGE_SIZE) < PAGE_SIZE) {
				perror("Failure to write");
				exit(1);
			}
			sync();
		}

		header.free_num = tmp_num - 1;
		header.num_pages = tmp_num;
		lseek(fd, 0, SEEK_SET);
		if (write(fd, &header, PAGE_SIZE) < PAGE_SIZE) {
			perror("Failure to write");
			exit(1);
		}
		sync();
	}
	// allocate page
	num = header.free_num;

	freepg_t freepg;
	lseek(fd, num * PAGE_SIZE, SEEK_SET);
	if (read(fd, &freepg, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to read");
		exit(1);
	}
	
	header.free_num = freepg.next_frpg;
	lseek(fd, 0, SEEK_SET);
	if (write(fd, &header, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to write");
		exit(1);
	}
	sync();

	memset(&freepg, 0x00, PAGE_SIZE);
	lseek(fd, num * PAGE_SIZE, SEEK_SET);
	if (write(fd, &freepg, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to write");
		exit(1);
	}
	sync();

	return num;
}

// Free an on-disk page to the free page list
void file_free_page(int fd, pagenum_t pagenum) {
	header_t header;
	lseek(fd, 0, SEEK_SET);
	if (read(fd, &header, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to read");
		exit(1);
	}

	freepg_t tmp;
	tmp.next_frpg = header.free_num;
	lseek(fd, pagenum * PAGE_SIZE, SEEK_SET);
	if (write(fd, &tmp, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to write");
		exit(1);
	}
	sync();

	header.free_num = pagenum;
	lseek(fd, 0, SEEK_SET);
	if (write(fd, &header, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to write");
		exit(1);
	}
	sync();
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int fd, pagenum_t pagenum, page_t* dest) {
	lseek(fd, pagenum * PAGE_SIZE, SEEK_SET);
	if (read(fd, dest, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to read");
		exit(1);
	}
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int fd, pagenum_t pagenum, const page_t* src) {
	lseek(fd, pagenum * PAGE_SIZE, SEEK_SET);
	if (write(fd, src, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to write");
		exit(1);
	}
	sync();
}

// Close all table files
void file_close_table_file() {
	file_t* tfile;
	for (tfile = files; tfile; tfile = tfile->next) {
		close(tfile->fd);
	}
}
