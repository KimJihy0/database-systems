#include "../include/file.h"

table_t tables[NUM_TABLES];

int64_t file_open_table_file(const char * pathname) {
	int fd;
	fd = open(pathname, O_RDWR|O_CREAT|O_EXCL|O_SYNC, 0644);
	if (fd < 0 && errno == EEXIST) {
		fd = open(pathname, O_RDWR|O_SYNC, 0644);
		if (fd < 0) {
			perror("Failure to open table file(open error)");
			exit(EXIT_FAILURE);
		}
	}
	else if (fd < 0) {
		perror("Failure to open table file(creat error)");
		exit(EXIT_FAILURE);
	}
	else {
		page_t header;
		header.free_num = INITIAL_PAGENUM - 1;
		header.num_pages = INITIAL_PAGENUM;
		header.root_num = 0;
		lseek(fd, 0, SEEK_SET);
		if (write(fd, &header, PAGE_SIZE) != PAGE_SIZE) {
			perror("Failure to open table file(write error)");
			exit(EXIT_FAILURE);
		}
		fsync(fd);

		page_t freepg;
		int i;
		for (i = 1; i < INITIAL_PAGENUM; i++) {
			freepg.next_frpg = i - 1;
			if (write(fd, &freepg, PAGE_SIZE) != PAGE_SIZE) {
				perror("Failure to open table file(write error)");
				exit(EXIT_FAILURE);
			}
			fsync(fd);
		}
	}
	table_t new_table;
	strcpy(new_table.pathname, pathname);
	new_table.fd = fd;
	int64_t table_id = 0;
	int i;
	for (i = 0; i < strlen(pathname); i++)
		table_id += pathname[i];
	i = table_id % NUM_TABLES;
	while (strlen(tables[i].pathname) != 0) {
        if (!strcmp(tables[i].pathname, pathname)) {
			close(fd);
			return -1;
		}
		i = (++table_id) % NUM_TABLES;
	}
	tables[i] = new_table;
	return table_id;
}

pagenum_t file_alloc_page(int64_t table_id) {
	int fd;
	fd = tables[table_id % NUM_TABLES].fd;

	page_t header;
	lseek(fd, 0, SEEK_SET);
	if (read(fd, &header, PAGE_SIZE) != PAGE_SIZE) {
		perror("Failure to alloc page(read error)");
		exit(EXIT_FAILURE);
	}

	pagenum_t pagenum;
	if (header.free_num == 0) {
		pagenum = header.num_pages;

		page_t tmp_page;
		tmp_page.next_frpg = 0;
		lseek(fd, pagenum * PAGE_SIZE, SEEK_SET);
		if (write(fd, &tmp_page, PAGE_SIZE) != PAGE_SIZE) {
			perror("Failure to alloc page(write error)");
			exit(EXIT_FAILURE);
		}
		fsync(fd);

		pagenum_t tmp_num;
		for(tmp_num = pagenum + 1; tmp_num < 2 * pagenum; tmp_num++) {
			tmp_page.next_frpg = tmp_num - 1;
			if (write(fd, &tmp_page, PAGE_SIZE) != PAGE_SIZE) {
				perror("Failure to alloc page(write error)");
				exit(EXIT_FAILURE);
			}
			fsync(fd);
		}

		header.free_num = tmp_num - 1;
		header.num_pages = tmp_num;
	}
	pagenum = header.free_num;

	page_t freepg;
	lseek(fd, pagenum * PAGE_SIZE, SEEK_SET);
	if (read(fd, &freepg, PAGE_SIZE) != PAGE_SIZE) {
		perror("Failure to alloc page(read error)");
		exit(EXIT_FAILURE);
	}
	
	header.free_num = freepg.next_frpg;
	lseek(fd, 0, SEEK_SET);
	if (write(fd, &header, PAGE_SIZE) != PAGE_SIZE) {
		perror("Failure to alloc page(write error)");
		exit(EXIT_FAILURE);
	}
	fsync(fd);

	return pagenum;
}

void file_free_page(int64_t table_id, pagenum_t pagenum) {
	int fd;
	fd = tables[table_id % NUM_TABLES].fd;

	page_t header;
	lseek(fd, 0, SEEK_SET);
	if (read(fd, &header, PAGE_SIZE) != PAGE_SIZE) {
		perror("Failure to free page(read error)");
		exit(EXIT_FAILURE);
	}

	page_t tmp;
	tmp.next_frpg = header.free_num;
	lseek(fd, pagenum * PAGE_SIZE, SEEK_SET);
	if (write(fd, &tmp, PAGE_SIZE) != PAGE_SIZE) {
		perror("Failure to free page(write error)");
		exit(EXIT_FAILURE);
	}
	fsync(fd);

	header.free_num = pagenum;
	lseek(fd, 0, SEEK_SET);
	if (write(fd, &header, PAGE_SIZE) != PAGE_SIZE) {
		perror("Failure to free page(write error)");
		exit(EXIT_FAILURE);
	}
	fsync(fd);
}

void file_read_page(int64_t table_id, pagenum_t pagenum, page_t * dest) {
	int fd;
	fd = tables[table_id % NUM_TABLES].fd;

	lseek(fd, pagenum * PAGE_SIZE, SEEK_SET);
	if (read(fd, dest, PAGE_SIZE) != PAGE_SIZE) {
		perror("Failure to read page(read error)");
		exit(EXIT_FAILURE);
	}
}

void file_write_page(int64_t table_id, pagenum_t pagenum, const page_t * src) {
	int fd;
	fd = tables[table_id % NUM_TABLES].fd;

	lseek(fd, pagenum * PAGE_SIZE, SEEK_SET);
	if (write(fd, src, PAGE_SIZE) != PAGE_SIZE) {
		perror("Failure to write page(write error)");
		exit(EXIT_FAILURE);
	}
	fsync(fd);
}

void file_close_table_file() {
	int i;
	for (i = 0; i < NUM_TABLES; i++) {
        if (strlen(tables[i].pathname) != 0)
			close(tables[i].fd);
	}
}
