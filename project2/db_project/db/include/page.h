#ifndef DB_PAGE_H_
#define DB_PAGE_H_

#include <stdint.h>

typedef uint64_t pagenum_t;

#pragma pack (push, 1)
struct slot_t {
    uint64_t key;
    uint16_t size;
    uint16_t offset;
};
#pragma pack (pop)

struct entry_t {
    uint64_t key;
    uint64_t page_num;
};

struct page_t {
    union { // 000~008
        uint64_t free_num;
        uint64_t parent;
        uint64_t next_frpg;
    };
    union { // 008~016
        uint64_t num_pages;
        struct {
            uint32_t is_leaf;
            uint32_t num_keys;
        };
    };
    union { // 016~024
        uint64_t root_num;
    };
    char reserved[88];
    union { // 112~120
        uint64_t free_space;
    };
    union { // 120~128
        uint64_t sibling;
        uint64_t this_num;
    };
    union {
        union {
            slot_t slots[64];
            char values[3968];
        };
        entry_t entries[248];
    };
};

struct file {
	int fd;
	file* next;
};

#endif