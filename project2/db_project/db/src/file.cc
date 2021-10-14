#include "file.h"

table_t tables[NUM_TABLES];

// Open existing table file or create one if not existed
int64_t file_open_table_file(const char* pathname) {
	int fd;
	fd = open(pathname, O_RDWR|O_CREAT|O_EXCL|O_SYNC, 0777);
	// file exists
	if (fd < 0 && errno == EEXIST) {
		fd = open(pathname, O_RDWR|O_SYNC, 0777);
		if (fd < 0) {
			perror("Failure to open table file(open error)");
			exit(EXIT_FAILURE);
		}
	}
	else if (fd < 0) {
		perror("Failure to open table file(creat error)");
		exit(EXIT_FAILURE);
	}
	// file not exists
	else {
		header_t header;
		header.free_num = INITIAL_PAGENUM - 1;
		header.num_pages = INITIAL_PAGENUM;
		header.root_num = 0;
		lseek(fd, 0, SEEK_SET);
		if (write(fd, &header, PAGE_SIZE) < PAGE_SIZE) {
			perror("Failure to open table file(write error)");
			exit(EXIT_FAILURE);
		}
		fsync(fd);

		page_t freepg;
		int i;
		for (i = 1; i < INITIAL_PAGENUM; i++) {
			freepg.next_frpg = i - 1;
			if (write(fd, &freepg, PAGE_SIZE) < PAGE_SIZE) {
				perror("Failure to open table file(write error)");
				exit(EXIT_FAILURE);
			}
			fsync(fd);
		}
	}
	// add file into hash table
	table_t new_table;
	strcpy(new_table.pathname, pathname);
	new_table.fd = fd;
	return add_table(new_table, tables);
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int64_t table_id) {
	int fd;
	fd = tables[table_id % NUM_TABLES].fd;

	header_t header;
	lseek(fd, 0, SEEK_SET);
	if (read(fd, &header, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to alloc page(read error)");
		exit(EXIT_FAILURE);
	}

	pagenum_t num;
	// free page not exists
	if (header.free_num == 0) {
		num = header.num_pages;

		page_t tmp_page;
		tmp_page.next_frpg = 0;
		lseek(fd, num * PAGE_SIZE, SEEK_SET);
		if (write(fd, &tmp_page, PAGE_SIZE) < PAGE_SIZE) {
			perror("Failure to alloc page(write error)");
			exit(EXIT_FAILURE);
		}
		fsync(fd);

		pagenum_t tmp_num;
		for(tmp_num = num + 1; tmp_num < 2 * num; tmp_num++) {
			tmp_page.next_frpg = tmp_num - 1;
			if (write(fd, &tmp_page, PAGE_SIZE) < PAGE_SIZE) {
				perror("Failure to alloc page(write error)");
				exit(EXIT_FAILURE);
			}
			fsync(fd);
		}

		header.free_num = tmp_num - 1;
		header.num_pages = tmp_num;
	}
	// allocate page
	num = header.free_num;

	page_t freepg;
	lseek(fd, num * PAGE_SIZE, SEEK_SET);
	if (read(fd, &freepg, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to alloc page(read error)");
		exit(EXIT_FAILURE);
	}
	
	header.free_num = freepg.next_frpg;
	lseek(fd, 0, SEEK_SET);
	if (write(fd, &header, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to alloc page(write error)");
		exit(EXIT_FAILURE);
	}
	fsync(fd);

	return num;
}

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t pagenum) {
	int fd;
	fd = tables[table_id % NUM_TABLES].fd;

	header_t header;
	lseek(fd, 0, SEEK_SET);
	if (read(fd, &header, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to free page(read error)");
		exit(EXIT_FAILURE);
	}

	page_t tmp;
	tmp.next_frpg = header.free_num;
	lseek(fd, pagenum * PAGE_SIZE, SEEK_SET);
	if (write(fd, &tmp, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to free page(write error)");
		exit(EXIT_FAILURE);
	}
	fsync(fd);

	header.free_num = pagenum;
	lseek(fd, 0, SEEK_SET);
	if (write(fd, &header, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to free page(write error)");
		exit(EXIT_FAILURE);
	}
	fsync(fd);
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest) {
	int fd;
	fd = tables[table_id % NUM_TABLES].fd;

	lseek(fd, pagenum * PAGE_SIZE, SEEK_SET);
	if (read(fd, dest, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to read page(read error)");
		exit(EXIT_FAILURE);
	}
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src) {
	int fd;
	fd = tables[table_id % NUM_TABLES].fd;

	lseek(fd, pagenum * PAGE_SIZE, SEEK_SET);
	if (write(fd, src, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to write page(write error)");
		exit(EXIT_FAILURE);
	}
	fsync(fd);
}

// Close all table files
void file_close_table_file() {
	int i;
	for (i = 0; i < NUM_TABLES; i++) {
		if (!EMPTY(tables[i]))
			close(tables[i].fd);
	}
}

pagenum_t get_root_num(int64_t table_id) {
	int fd;
	fd = tables[table_id % NUM_TABLES].fd;

	header_t header;
	lseek(fd, 0, SEEK_SET);
	if (read(fd, &header, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to get root num(read error)");
		exit(EXIT_FAILURE);
	}

	return header.root_num;
}

void set_root_num(int64_t table_id, pagenum_t root_num) {
	int fd;
	fd = tables[table_id % NUM_TABLES].fd;

	header_t header;
	lseek(fd, 0, SEEK_SET);
	if (read(fd, &header, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to set root num(read error)");
		exit(EXIT_FAILURE);
	}

	header.root_num = root_num;

	lseek(fd, 0, SEEK_SET);
	if (write(fd, &header, PAGE_SIZE) < PAGE_SIZE) {
		perror("Failure to set root num(write error)");
		exit(EXIT_FAILURE);
	}
	fsync(fd);
}
