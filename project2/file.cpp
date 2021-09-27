#include "file.h"

// Open existing database file or create one if not existed
int file_open_database_file(const char* path) {
	fd = open(path, O_RDWR|O_SYNC|O_CREAT|O_EXCL, 0644);
	if ((fd < 0) && (errno == EEXIST)) {
		fd = open(path, O_RDWR|O_SYNC, 0644);
		return fd;
	}
	else if (fd < 0) {
		perror("File open failed.");
		exit(1);
	}

	//const pagenum_t page_num = 10 * 0x100000 / page_size;
	const pagenum_t page_num = 10;

	head_page header;
	header.free_num = page_num - 1;
	header.page_num = page_num;

	lseek(fd, 0, SEEK_SET);
	if (write(fd, &header, page_size) < page_size) {
		perror("Write failed.");
		exit(1);
	}

	free_page tmp;
	for (int idx = 1; idx < page_num; idx++) {
		tmp.next_page = idx - 1;
		if (write(fd, &tmp, page_size) < page_size) {
			perror("Write failed.");
			exit(1);
		}
	}

	return fd;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int fd) {
	page_t* header;
	header = (page_t*)malloc(sizeof(page_t));

	lseek(fd, 0, SEEK_SET);
	if (read(fd, header, page_size) < page_size) {
		perror("Read failed.");
		exit(1);
	}
	
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

		lseek(fd, 0, SEEK_SET);
		if (write(fd, header, page_size) < page_size) {
			perror("Write failed.");
			exit(1);
		}
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

		lseek(fd, 0, SEEK_SET);
		if (write(fd, header, page_size) < page_size) {
			perror("Write failed.");
			exit(1);
		}
		return num;
	}
}

// Free an on-disk page to the free page list
void file_free_page(int fd, pagenum_t pagenum) {
	page_t* header;
	header = (page_t*)malloc(sizeof(page_t));
	file_read_page(fd, 0, header);

	free_page tmp;
	tmp.next_page = header->free_num;
	header->free_num = pagenum;
	
	lseek(fd, pagenum * page_size, SEEK_SET);
	if (write(fd, &tmp, page_size) < page_size) {
		perror("Write failed.");
		exit(1);
	}

	file_write_page(fd, 0, header);
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int fd, pagenum_t pagenum, page_t* dest) {
	lseek(fd, pagenum * page_size, SEEK_SET);
	if (pagenum == 0) {
		head_page tmp;

		lseek(fd, 0, SEEK_SET);
		if (read(fd, &tmp, page_size) < page_size) {
			perror("Read failed.");
			exit(1);
		}

		dest->page_num = tmp.page_num;
		dest->free_num = tmp.free_num;
	}
	else {
		free_page tmp;

		lseek(fd, pagenum * page_size, SEEK_SET);
		if (read(fd, &tmp, page_size) < page_size) {
			perror("Read failed.");
			exit(1);
		}

		dest->next_page = tmp.next_page;
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

int main() {
	int mainfd;
	mainfd = file_open_database_file("db");
	page_t* header;
	header = (page_t*)malloc(sizeof(page_t));
	file_read_page(mainfd, 0, header);

	page_t* footer;
	footer = (page_t*)malloc(sizeof(page_t));
	file_read_page(mainfd, 19, footer);

	printf("nextfreepg: %ld\n", footer->next_page);


	// file_alloc_page(fd);
	//file_free_page(fd, 19);


	file_read_page(mainfd, 0, header);
	printf("pagenum: %ld\n", header->page_num);
	printf("freenum: %ld\n", header->free_num);

	file_close_database_file();

	return 0;
}

// int main() {
// 	int fd;
// 	fd = file_open_database_file("db4");
// 	printf("fd: %d\n", fd);
// 	page_t* newpage;
// 	newpage = (page_t*)malloc(sizeof(page_t));
// 	file_read_page(fd, 0, newpage);
// 	printf("%ld, %ld\n", newpage->free_num, newpage->page_num);

// 	file_alloc_page(fd);
// 	file_close_database_file();
// 	file_open_database_file("db4");
// 	file_alloc_page(fd);
// 	file_close_database_file();
// 	file_open_database_file("db4");

// 	printf("%ld, %ld\n", newpage->free_num, newpage->page_num);
// 	file_close_database_file();
// }