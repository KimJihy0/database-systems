#include "bpt.h"

#include <random>
#include <algorithm>
#include <vector>
#include <assert.h>
#include <unistd.h>
#include <time.h>
#include <iostream>
#include <time.h>

#define NUM_KEYS    10000
#define NUM_BUFS    400
#define size(n)     ((n) % 63 + 46)                 

// using namespace std;

static int count = 0;

int search(int64_t table_id, int trx_id, int64_t key);
int update(int64_t table_id, int trx_id, int64_t key);
void* thread1(void* arg);
void* thread2(void* arg);
void* thread3(void* arg);
int create_db(const char* pathname);
void print_page(pagenum_t page_num, page_t page);
void print_pgnum(int64_t table_id, pagenum_t page_num); 

#if 1
int main() {
    srand(time(__null));

    init_db(NUM_BUFS);
    int64_t table_id = create_db("table0");
    printf("file creation complete(%ld).\n", table_id);

    print_pgnum(table_id, 2559);

    pthread_t tx1, tx2, tx3;
    pthread_create(&tx1, 0, thread1, &table_id);
    
    for(int i = 0; i < 100000000; ++i);
    pthread_create(&tx2, 0, thread2, &table_id);

    pthread_join(tx1, NULL);
    pthread_join(tx2, NULL);

    print_pgnum(table_id, 2559);

    shutdown_db();
    printf("file saved complete(%ld).\n", table_id);
    return 0;
}
#endif

int search(int64_t table_id, int trx_id, int64_t key) {
    char ret_val[120];
    uint16_t old_size;
    int result;

    std::cout << "thread " << trx_id << " : key " <<  key << " 검색 시도!\n";
    if(db_find(table_id, key, ret_val, &old_size, trx_id) != 0) {
        std::cout << "thread "<< trx_id << " : ABORT 발생\n";
        return 1;
    }
    std::cout << "thread " << trx_id << " 검색 성공!\n";
    return 0;
}

int update(int64_t table_id, int trx_id, int64_t key) {
    char* val = (char*)"__";
    uint16_t old_size;
    int result;

    std::cout << "thread " << trx_id << " : key " <<  key << " 업데이트 시도!\n";
    if(db_update(table_id, key, val, 10, &old_size, trx_id) != 0) {
        std::cout << "thread "<< trx_id << " : ABORT 발생\n";
        return 1;
    }
    std::cout << "thread " << trx_id << " 업데이트 성공!\n";
    return 0;
}


void* thread1(void* arg)
{
    int randTemp, a, b;
    int* tid = (int*)arg;
    int txid = trx_begin();
    std::cout << "Thread1 TXID = " << txid << std::endl;
    
    while(1) {
        a = rand()%4;
        if(a==0) {
            if(search(*tid, txid, rand()%10)) return nullptr;
        } else if(a==1) {
            if(update(*tid, txid, rand()%10)) return nullptr;
        } else if(a==2) {
            std::cout << 2 << " sleep!" << '\n';
            sleep(3);
        } else {
            trx_commit(txid);
            return nullptr;
        }
    }

    return nullptr;
}

void* thread2(void* arg)
{
    int randTemp, a, b;
    int* tid = (int*)arg;
    int txid = trx_begin();
    std::cout << "Thread2 TXID = " << txid << std::endl;
    
    while(1) {
        a = rand()%10;
        if(a<4) {
            if(search(*tid, txid, rand()%10)) return nullptr;
        } else if(a<8) {
            if(update(*tid, txid, rand()%10)) return nullptr;
        } else if(a<9) {
            std::cout << 2 << " sleep!" << '\n';
            sleep(3);
        } else {
            trx_commit(txid);
            return nullptr;
        }
    }

    return nullptr;
}

int create_db(const char* pathname) {
    std::random_device rd;
	std::mt19937 gen(rd());
	std::default_random_engine rng(rd());
	char value[112];
	uint16_t val_size;
	std::vector<int> keys;
	for (int i = 0; i < NUM_KEYS; i++) {
		keys.push_back(i);
	}
    shuffle(keys.begin(), keys.end(), rng);
	int64_t table_id = open_table((char*)pathname);
    for (const auto& i : keys) {
        sprintf(value, "%02d", i % 100);
        if (db_insert(table_id, i, value, size(i)) != 0) return table_id;
    }
    return table_id;
}

void print_page(pagenum_t page_num, page_t page) {
	if (page_num == 0) {
		printf("----header information----\n");
        printf("number: %ld\n", page_num);
		printf("next_frpg: %ld\n", page.next_frpg);
		printf("num_pages: %ld\n", page.num_pages);
		printf("root_num: %ld\n", page.root_num);
		printf("\n");
		return;
	}
	if (page.is_leaf) {
        if (page.parent)
		    printf("-----leaf information-----\n");
        else
            printf("-----root information-----\n");
        printf("number: %ld\n", page_num);
		printf("parent: %ld\n", page.parent);
		printf("is_leaf: %d\n", page.is_leaf);
		printf("num_keys: %d\n", page.num_keys);
		printf("free_space: %ld\n", page.free_space);
		printf("sibling: %ld\n", page.sibling);

		for (int i = 0; i < page.num_keys; i++) {
			printf("key: %3ld, size: %3d, offset: %4d, value: %s\n", page.slots[i].key, page.slots[i].size, page.slots[i].offset, page.values + page.slots[i].offset - HEADER_SIZE);
		}
	}
	else {
        if (page.parent)
		    printf("-----page information-----\n");
        else
            printf("-----root information-----\n");
        printf("number: %ld\n", page_num);
		printf("parent: %ld\n", page.parent);
		printf("is_leaf: %d\n", page.is_leaf);
		printf("num_keys: %d\n", page.num_keys);

		printf("left_child: %ld\n", page.left_child);
		for (int i = 0; i < page.num_keys; i++) {
			printf("key: %3ld, child: %ld\n", page.entries[i].key, page.entries[i].child);
		}
	}
    printf("\n");
}

void print_pgnum(int64_t table_id, pagenum_t page_num) {
	page_t* page;
	int idx = buffer_read_page(table_id, page_num, &page);
	print_page(page_num, *page);
    pthread_mutex_unlock(&(buffers[idx]->page_latch));
}

#if 0
int main() {
    std::random_device rd;
	std::mt19937 gen(rd());
	std::default_random_engine rng(rd());
	char value[112];
	uint16_t val_size;
	std::vector<int> keys;
	for (int i = 0; i < NUM_KEYS; i++) {
		keys.push_back(i);
	}
    shuffle(keys.begin(), keys.end(), rng);
	
    printf("[TEST START]\n\n");
    printf("[NUM_KEYS : %7d] \n", NUM_KEYS);
    printf("[NUM_BUFS : %7d] \n", NUM_BUFS);
    printf("\n");

    init_db(NUM_BUFS);
	int64_t table_id = open_table((char*)"table0");
    for (int j = 0; j < 3; j++) {
        shuffle(keys.begin(), keys.end(), rng);

        printf("[REP%2d] ", j);

        printf("[INSERT START]\n");
        for (const auto& i : keys) {
            // printf("insert %4d\n", i);
            sprintf(value, "%02d", i % 100);
            if (db_insert(table_id, i, value, size(i)) != 0) goto func_exit;
        }
        printf("\t[INSERT END]\n");

        printf("\t[FIND START]\n");
        for (const auto& i : keys) {
            memset(value, 0x00, 112);
            val_size = 0;
            // printf("find %4d\n", i);
            if(db_find(table_id, i, value, &val_size, 0) != 0) goto func_exit;
            // else if (size(i) != val_size ||
            // 		 val(i) != std::string(value, val_size)) {
            // 	printf("value dismatched\n");
            // 	goto func_exit;
            // }
        }
        printf("\t[FIND END]\n");

        // print_tree(table_id);
        // printf("\n");

        printf("\t[DELETE START]\n");
        for (const auto& i : keys) {
            // printf("delete %4d\n", i);
            if (db_delete(table_id, i) != 0) goto func_exit;
        }
        printf("\t[DELETE END]\n");

        printf("\t[FIND START AGAIN]\n");
        for (const auto& i : keys) {
            memset(value, 0x00, 112);
            val_size = 0;
            if (db_find(table_id, i, value, &val_size, 0) == 0) goto func_exit;
        }
        printf("\t[FIND END AGAIN]\n");

        // print_tree(table_id);
        // printf("\n");
    }

    printf("\n[TEST END]\n\n");    

	func_exit:
	printf("[SHUTDOWN START]\n");
    // print_freepg_list(table_id);
    // printf("\n");
	// print_tree(table_id);
    // printf("\n");
	if (shutdown_db() != 0) {
		return 0;
	}
	printf("[SHUTDOWN END]\n");
}
#endif