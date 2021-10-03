#include "file.h"
#include <gtest/gtest.h>
#include <string>
#include <iostream>

/*******************************************************************************
 * The test structures stated here were written to give you and idea of what a
 * test should contain and look like. Feel free to change the code and add new
 * tests of your own. The more concrete your tests are, the easier it'd be to
 * detect bugs in the future projects.
 ******************************************************************************/

/*
 * Tests file open/close APIs.
 * 1. Open a file and check the descriptor
 * 2. Check if the file's initial size is 10 MiB
 */
TEST(FileInitTest, HandlesInitialization) {
    int fd;                                 // file descriptor
    std::string pathname = "init_test.db";  // customize it to your test file

    // Open a database file
    fd = file_open_database_file(pathname.c_str());

    // Check if the file is opened
    ASSERT_TRUE(fd >= 0);  // change the condition to your design's behavior

    // Check the size of the initial file
    int file_size;
    file_size = lseek(fd, 0, SEEK_END);
    EXPECT_EQ(file_size, INITIAL_FILESIZE);

    // Close all database files
    file_close_database_file();

    // Remove the db file
    int is_removed = remove(pathname.c_str());

    ASSERT_EQ(is_removed, 0);
}

/*
 * TestFixture for page allocation/deallocation tests
 */
class FileTest : public ::testing::Test {
    protected:
    /*
    * NOTE: You can also use constructor/destructor instead of SetUp() and
    * TearDown(). The official document says that the former is actually
    * perferred due to some reasons. Checkout the document for the difference
    */
    FileTest() { fd = file_open_database_file(pathname.c_str()); }

    ~FileTest() {
        if (fd >= 0) {
            file_close_database_file();
			remove(pathname.c_str());
        }
    }

    int fd;                                     // file descriptor
    std::string pathname = "test_dbfile.db";    // path for the file
};

/*
 * Tests page allocation and free
 * 1. Allocate 2 pages and free one of them, traverse the free page list
 *    and check the existence/absence of the freed/allocated page
 */
TEST_F(FileTest, HandlesPageAllocation) {
    pagenum_t allocated_page, freed_page;

    // Allocate the pages
    allocated_page = file_alloc_page(fd);
    freed_page = file_alloc_page(fd);

    // Free one page
    file_free_page(fd, freed_page);

    // Traverse the free page list and check the existence of the freed/allocated
    // pages. You might need to open a few APIs soley for testing.
    page_t header;
    lseek(fd, 0, SEEK_SET);
    read(fd, &header, PAGE_SIZE);
    EXPECT_EQ(header.free_num, 2558);

    for (int i = 0; i < 2559; i++) {
        file_alloc_page(fd);
    }

    lseek(fd, 0, SEEK_SET);
    read(fd, &header, PAGE_SIZE);
    EXPECT_EQ(header.free_num, 5118);
}

/*
 * Tests page read/write operations
 * 1. Write/Read a page with some random content and check if the data matches
 */
TEST_F(FileTest, CheckReadWriteOperation) {
   	pagenum_t pagenum;
    page_t src, dest;

    char a[4096];
    std::fill_n(a, 4096, 'a');
    strcpy(src.data, a);

	pagenum = file_alloc_page(fd);

    file_write_page(fd, pagenum, &src);

    file_read_page(fd, pagenum, &dest);

    for (int i = 0; i < 4096; i++) {
        EXPECT_EQ(a[i], dest.data[i]);
    }
}
