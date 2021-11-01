#include "file.h"

table_t tables[NUM_BUCKETS];

int64_t file_open_table_file(const char * pathname) {
	int fd;
	fd = open(pathname, O_RDWR|O_CREAT|O_EXCL, 0644);
	if (fd < 0 && errno == EEXIST) {
		fd = open(pathname, O_RDWR);
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
		header.next_frpg = INITIAL_PAGENUM - 1;
		header.num_pages = INITIAL_PAGENUM;
		header.root_num = 0;
		lseek(fd, 0, SEEK_SET);
		if (write(fd, &header, PAGE_SIZE) != PAGE_SIZE) {
			perror("Failure to open table file(write error)");
			exit(EXIT_FAILURE);
		}
		fsync(fd);

		page_t p;
		int i;
		for (i = 1; i < INITIAL_PAGENUM; i++) {
			p.next_frpg = i - 1;
			if (write(fd, &p, PAGE_SIZE) != PAGE_SIZE) {
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
	i = table_id % NUM_BUCKETS;
	while (strlen(tables[i].pathname) != 0) {
        if (!strcmp(tables[i].pathname, pathname)) {
			close(fd);
			return -1;
		}
		i = (++table_id) % NUM_BUCKETS;
	}
	tables[i] = new_table;
	return table_id;
}

pagenum_t file_alloc_page(int64_t table_id) {
	int fd;
	fd = tables[table_id % NUM_BUCKETS].fd;

	page_t header;
	lseek(fd, 0, SEEK_SET);
	if (read(fd, &header, PAGE_SIZE) != PAGE_SIZE) {
		perror("Failure to alloc page(read error)");
		exit(EXIT_FAILURE);
	}

	pagenum_t page_num;
	if (header.next_frpg == 0) {
		page_num = header.num_pages;

		page_t p;
		p.next_frpg = 0;
		lseek(fd, page_num * PAGE_SIZE, SEEK_SET);
		if (write(fd, &p, PAGE_SIZE) != PAGE_SIZE) {
			perror("Failure to alloc page(write error)");
			exit(EXIT_FAILURE);
		}
		fsync(fd);

		pagenum_t i;
		for(i = page_num + 1; i < 2 * page_num; i++) {
			p.next_frpg = i - 1;
			if (write(fd, &p, PAGE_SIZE) != PAGE_SIZE) {
				perror("Failure to alloc page(write error)");
				exit(EXIT_FAILURE);
			}
			fsync(fd);
		}

		header.next_frpg = i - 1;
		header.num_pages = i;
	}
	page_num = header.next_frpg;

	page_t alloc;
	lseek(fd, page_num * PAGE_SIZE, SEEK_SET);
	if (read(fd, &alloc, PAGE_SIZE) != PAGE_SIZE) {
		perror("Failure to alloc page(read error)");
		exit(EXIT_FAILURE);
	}
	
	header.next_frpg = alloc.next_frpg;
	lseek(fd, 0, SEEK_SET);
	if (write(fd, &header, PAGE_SIZE) != PAGE_SIZE) {
		perror("Failure to alloc page(write error)");
		exit(EXIT_FAILURE);
	}
	fsync(fd);

	return page_num;
}

void file_free_page(int64_t table_id, pagenum_t page_num) {
	int fd;
	fd = tables[table_id % NUM_BUCKETS].fd;

	page_t header;
	lseek(fd, 0, SEEK_SET);
	if (read(fd, &header, PAGE_SIZE) != PAGE_SIZE) {
		perror("Failure to free page(read error)");
		exit(EXIT_FAILURE);
	}

	page_t free;
	free.next_frpg = header.next_frpg;
	lseek(fd, page_num * PAGE_SIZE, SEEK_SET);
	if (write(fd, &free, PAGE_SIZE) != PAGE_SIZE) {
		perror("Failure to free page(write error)");
		exit(EXIT_FAILURE);
	}
	fsync(fd);

	header.next_frpg = page_num;
	lseek(fd, 0, SEEK_SET);
	if (write(fd, &header, PAGE_SIZE) != PAGE_SIZE) {
		perror("Failure to free page(write error)");
		exit(EXIT_FAILURE);
	}
	fsync(fd);
}

void file_read_page(int64_t table_id, pagenum_t page_num, page_t * dest) {
	int fd;
	fd = tables[table_id % NUM_BUCKETS].fd;

	lseek(fd, page_num * PAGE_SIZE, SEEK_SET);
	if (read(fd, dest, PAGE_SIZE) != PAGE_SIZE) {
		perror("Failure to read page(read error)");
		exit(EXIT_FAILURE);
	}
}

void file_write_page(int64_t table_id, pagenum_t page_num, const page_t * src) {
	int fd;
	fd = tables[table_id % NUM_BUCKETS].fd;

	lseek(fd, page_num * PAGE_SIZE, SEEK_SET);
	if (write(fd, src, PAGE_SIZE) != PAGE_SIZE) {
		perror("Failure to write page(write error)");
		exit(EXIT_FAILURE);
	}
	fsync(fd);
}

void file_close_table_file() {
	int i;
	for (i = 0; i < NUM_BUCKETS; i++) {
        if (strlen(tables[i].pathname) != 0)
			close(tables[i].fd);
	}
}
