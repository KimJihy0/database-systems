#include "file.h"

// Open existing database file or create one if not existed
int file_open_database_file(const char* path) {
	int fd;
	fd = open(path, O_RDWR|O_SYNC|O_CREAT|O_EXCL, 0644);
	if (fd < 0) {
		if (errno == EEXIST) {
			fd = open(path, O_RDWR|O_SYNC, 0644);
		}
		if (fd < 0) {
			perror("open error");
			exit(1);
		}
	}
	else {
		head_page header;
		header.free_num = default_pagenum - 1;
		header.page_num = default_pagenum;
		lseek(fd, 0, SEEK_SET);
		if (write(fd, &header, page_size) < page_size) {
			perror("write error");
			exit(1);
		}

		free_page tmp;
		int i;
		for (i = 1; i < default_pagenum; i++) {
			tmp.next_frpg = i - 1;
			if (write(fd, &tmp, page_size) < page_size) {
				perror("write error");
				exit(1);
			}
		}
	}

	dbfile* new_file;
	new_file = (dbfile*)malloc(sizeof(dbfile));
	new_file->fd = fd;
	new_file->next = fds;
	fds = new_file;

	return fd;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int fd) {
	page_t* header;
	header = (page_t*)malloc(sizeof(page_t));
	lseek(fd, 0, SEEK_SET);
	if (read(fd, header, page_size) < page_size) {
		perror("read error");
		exit(1);
	}

	if (header->free_num == 0) {
		pagenum_t num;
		num = header->page_num;
		if (num == UINT64_MAX) {
			perror("alloc error");
			exit(1);
		}

		free_page tmppage;
		tmppage.next_frpg = 0;
		lseek(fd, num * page_size, SEEK_SET);
		if (write(fd, &tmppage, page_size) < page_size) {
			perror("write error");
			exit(1);
		}

		pagenum_t tmpnum;
		for(tmpnum = num + 1; tmpnum < 2 * num && tmpnum < UINT64_MAX; tmpnum++) {
			tmppage.next_frpg = tmpnum - 1;
			if (write(fd, &tmppage, page_size) < page_size) {
				perror("write error");
				exit(1);
			}
		}

		header->free_num = tmpnum - 1;
		header->page_num = tmpnum;
		lseek(fd, 0, SEEK_SET);
		if (write(fd, header, page_size) < page_size) {
			perror("write error");
			exit(1);
		}

		return tmpnum - 1;
	}
	else {
		pagenum_t num;
		num = header->free_num;

		free_page tmp;
		lseek(fd, num * page_size, SEEK_SET);
		if (read(fd, &tmp, page_size) < page_size) {
			perror("read error");
			exit(1);
		}
		
		header->free_num = tmp.next_frpg;
		lseek(fd, 0, SEEK_SET);
		if (write(fd, header, page_size) < page_size) {
			perror("write error");
			exit(1);
		}

		return num;
	}
}

// Free an on-disk page to the free page list
void file_free_page(int fd, pagenum_t pagenum) {
	page_t* header;
	header = (page_t*)malloc(sizeof(page_t));
	lseek(fd, 0, SEEK_SET);
	if (read(fd, header, page_size) < page_size) {
		perror("read error");
		exit(1);
	}

	free_page tmp;
	tmp.next_frpg = header->free_num;
	lseek(fd, pagenum * page_size, SEEK_SET);
	if (write(fd, &tmp, page_size) < page_size) {
		perror("write error");
		exit(1);
	}

	header->free_num = pagenum;
	lseek(fd, 0, SEEK_SET);
	if (write(fd, header, page_size) < page_size) {
		perror("write error");
		exit(1);
	}
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int fd, pagenum_t pagenum, page_t* dest) {
	lseek(fd, pagenum * page_size, SEEK_SET);
	if (pagenum == 0) {
		head_page tmp;
		lseek(fd, pagenum * page_size, SEEK_SET);
		if (read(fd, &tmp, page_size) < page_size) {
			perror("read error");
			exit(1);
		}

		dest->page_num = tmp.page_num;
		dest->free_num = tmp.free_num;
	}
	else {
		free_page tmp;
		lseek(fd, pagenum * page_size, SEEK_SET);
		if (read(fd, &tmp, page_size) < page_size) {
			perror("read error");
			exit(1);
		}

		dest->next_frpg = tmp.next_frpg;
	}
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int fd, pagenum_t pagenum, const page_t* src) {
	if (pagenum == 0) {
		head_page tmp;
		tmp.free_num = src->free_num;
		tmp.page_num = src->page_num;

		lseek(fd, pagenum * page_size, SEEK_SET);
		if (write(fd, &tmp, page_size) < page_size) {
			perror("write error");
			exit(1);
		}
	}
	else {
		free_page tmp;
		tmp.next_frpg = src->next_frpg;

		lseek(fd, pagenum * page_size, SEEK_SET);
		if (write(fd, &tmp, page_size) < page_size) {
			perror("write error");
			exit(1);
		}
	}
}

// Stop referencing the database file
void file_close_database_file() {
	dbfile* tfile;
	for (tfile = fds; tfile; tfile = tfile->next) {
		close(tfile->fd);
	}
}