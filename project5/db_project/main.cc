#include "bpt.h"

#include <random>
#include <algorithm>
#include <vector>
#include <assert.h>
#include <unistd.h>
#include <time.h>
#include <iostream>
#include <time.h>

#define NUM_KEYS    100
#define NUM_BUFS    5
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
void print_all(int64_t table_id);

#if 1
int main() {
    srand(time(__null));

    init_db(NUM_BUFS);
    int64_t table_id = create_db("table0");
    printf("file creation complete(%ld).\n", table_id);

    // print_pgnum(table_id, 2559);
    print_all(table_id);

    pthread_t tx1, tx2, tx3;
    // pthread_create(&tx1, 0, thread1, &table_id);
    
    // for(int i = 0; i < 100000000; ++i);
    pthread_create(&tx2, 0, thread2, &table_id);

    // pthread_join(tx1, NULL);
    pthread_join(tx2, NULL);

    print_all(table_id);

    shutdown_db();
    printf("file saved complete(%ld).\n", table_id);
    return 0;
}
#endif

int search(int64_t table_id, int trx_id, int64_t key) {
    char ret_val[120];
    uint16_t old_size;
    int result;

    // std::cout << "thread " << trx_id << " : key " <<  key << " 검색 시도!\n";
    if(db_find(table_id, key, ret_val, &old_size, trx_id) != 0) {
        std::cout << "thread " << trx_id << " : key " <<  key << " ABORT!\n\n";
    }
    // else std::cout << "thread " << trx_id << " : key " <<  key << " 검색 성공!\n\n";
    return 0;
}

int update(int64_t table_id, int trx_id, int64_t key) {
    char* val = (char*)"**";
    uint16_t old_size;
    int result;

    // std::cout << "thread " << trx_id << " : key " <<  key << " 업데이트 시도!\n";
    if(db_update(table_id, key, val, 10, &old_size, trx_id) != 0) {
        std::cout << "thread " << trx_id << " : key " <<  key << " ABORT!\n\n";
    }
    // else std::cout << "thread " << trx_id << " : key " <<  key << " 업데이트 성공!\n\n";
    return 0;
}


void* thread1(void* arg)
{
    int a;
    int* table_id = (int*)arg;
    
    int trx_id = trx_begin();
    while (1) {
        a = rand()%4;
        if(a==0) search(*table_id, trx_id, rand() % NUM_KEYS);
        else if(a==1) update(*table_id, trx_id, rand() % NUM_KEYS);
        else if(a==2) sleep(1);
        else break;
    }
    trx_commit(trx_id);

    return nullptr;
}

void* thread2(void* arg)
{
    int a;
    int* table_id = (int*)arg;

    int trx_id = trx_begin();
    int T = 200;
    while(T--) {
        a = rand()%10;
        if(a<4) search(*table_id, trx_id, rand() % NUM_KEYS);
        else if(a<8) update(*table_id, trx_id, rand() % NUM_KEYS);
        else if(a<9) sleep(1);
        // else break;
        else continue;
    }
    trx_commit(trx_id);

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

void print_all(int64_t table_id) {
    pagenum_t root_num, temp_num;
    page_t* root, * page, * header;

    int header_idx = buffer_read_page(table_id, 0, &header);
    root_num = header->root_num;
    pthread_mutex_unlock(&(buffers[header_idx]->page_latch));

	printf("-----header information-----\n");
	printf("root_num: %ld\n", root_num);
	printf("\n");
	if (!root_num) return;

    int root_idx = buffer_read_page(table_id, root_num, &root);
    print_page(root_num, *root);
    if (root->is_leaf) {
        pthread_mutex_unlock(&(buffers[root_idx]->page_latch));
        return;
    }

    temp_num = root->left_child;
    int temp_idx = buffer_read_page(table_id, temp_num, &page);
    print_page(temp_num, *page); 
    for (int i = 0; i < root->num_keys; i++) {
        temp_num = root->entries[i].child;
        pthread_mutex_unlock(&(buffers[temp_idx]->page_latch));
        temp_idx = buffer_read_page(table_id, temp_num, &page);
        print_page(temp_num, *page);
    }
    pthread_mutex_unlock(&(buffers[root_idx]->page_latch));
    pthread_mutex_unlock(&(buffers[temp_idx]->page_latch));
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