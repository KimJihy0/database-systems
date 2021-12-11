#include "bpt.h"

#include <string.h>
#include <random>
#include <algorithm>
#include <vector>
#include <time.h>

#define NUM_KEYS    (50)
#define NUM_BUFS    (1000)
#define SIZE(n)     ((n) % 63 + 46)
#define NEW_VAL     ((char*)"$$")

#define UPDATE_THREADS_NUMBER   (5)
#define SEARCH_THREADS_NUMBER   (1)

#define UPDATE_COUNT            (50)
#define SEARCH_COUNT            (50)

std::string gen_rand_val(int size);
int create_db(const char* pathname);
void print_page(pagenum_t page_num, page_t page);
void print_pgnum(int64_t table_id, pagenum_t page_num);
void print_all(int64_t table_id);

void* update_thread_func(void* arg) {
    int64_t keys[UPDATE_COUNT];
    uint16_t old_size;
    std::string value = gen_rand_val(2);

    for (int i = 0; i < UPDATE_COUNT; i++)
        keys[i] = rand() % NUM_KEYS;
        // keys[i] = i;

    int trx_id = trx_begin();
    for (int i = 0; i < UPDATE_COUNT; i++)
        db_update(1, keys[i], (char*)value.c_str(), 2, &old_size, trx_id);
    if (trx_commit(trx_id) == trx_id)
        printf("Update thread is done(commit).(%s)\n", (char*)value.c_str());
    else
        printf("Update thread is done(abort).(%s)\n", (char*)value.c_str());

    return nullptr;
}

void* search_thread_func(void* arg) {
    int64_t keys[SEARCH_COUNT];
    char ret_val[108];
    uint16_t old_size;

    for (int i = 0; i < SEARCH_COUNT; i++)
        keys[i] = rand() % NUM_KEYS;
        // keys[i] = i;

    int trx_id = trx_begin();
    for (int i = 0; i < SEARCH_COUNT; i++)
        db_find(1, keys[i], ret_val, &old_size, trx_id);
    if (trx_commit(trx_id) == trx_id)
        printf("Search thread is done(commit).\n");
    else
        printf("Search thread is done(abort).\n");

    return nullptr;
}

#if 1
int main() {
    pthread_t update_threads[UPDATE_THREADS_NUMBER];
    pthread_t search_threads[SEARCH_THREADS_NUMBER];

    srand(time(__null));

    init_db(NUM_BUFS, 0, 0, (char*)"logfile.data", (char*)"logmsg.txt");
    int64_t table_id = create_db("DATA1");
    printf("file creation complete(%ld).\n", table_id);

    for (int i = 0; i < UPDATE_THREADS_NUMBER; i++)
        pthread_create(&update_threads[i], 0, update_thread_func, &i);
    for (int i = 0; i < SEARCH_THREADS_NUMBER; i++)
        pthread_create(&search_threads[i], 0, search_thread_func, &i);
        
    for (int i = 0; i < UPDATE_THREADS_NUMBER; i++)
        pthread_join(update_threads[i], NULL);
    for (int i = 0; i < SEARCH_THREADS_NUMBER; i++)
        pthread_join(search_threads[i], NULL);

    print_pgnum(table_id, 2559);
    print_pgnum(table_id, 2558);

    shutdown_db();
    printf("file saved complete(%ld).\n", table_id);
    return 0;
}
#endif

std::string gen_rand_val(int size) {
    static const std::string CHARACTERS {
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"};
    char ret_value[2];
	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<int> char_dis(1, CHARACTERS.size());
	auto helper_function = [] (auto& gen, auto& cd, auto size) -> std::string {
		std::string ret_str;
		int index;
		for (int i = 0; i < size; ++i) {
			index = cd(gen) - 1;
			ret_str += CHARACTERS[index];
		}
		return ret_str;
	};
    return helper_function(gen, char_dis, size);
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
        if (db_insert(table_id, i, value, SIZE(i)) != 0) return table_id;
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
		// printf("parent: %ld\n", page.parent);
		// printf("is_leaf: %d\n", page.is_leaf);
		// printf("num_keys: %d\n", page.num_keys);
		// printf("free_space: %ld\n", page.free_space);
		// printf("sibling: %ld\n", page.sibling);

		for (int i = 0; i < page.num_keys; i++) {
            char value[page.slots[i].size + 1];
            memcpy(value, page.values + page.slots[i].offset - HEADER_SIZE, page.slots[i].size);
            value[page.slots[i].size] = 0;
			printf("key: %3ld, size: %3d, offset: %4d, trx_id: %d, value: %s\n", page.slots[i].key, page.slots[i].size, page.slots[i].offset, page.slots[i].trx_id, value);
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
	buffer_read_page(table_id, page_num, &page);
	print_page(page_num, *page);
    buffer_unpin_page(table_id, page_num);
}

// void print_all(int64_t table_id) {
//     pagenum_t root_num, temp_num;
//     page_t* root, * page, * header;

//     buffer_read_page(table_id, 0, &header);
//     root_num = header->root_num;
//     buffer_unpin_page(table_id, 0);

// 	printf("-----header information-----\n");
// 	printf("root_num: %ld\n", root_num);
// 	printf("\n");
// 	if (!root_num) return;

//     buffer_read_page(table_id, root_num, &root);
//     print_page(root_num, *root);
//     if (root->is_leaf) {
//         buffer_unpin_page(table_id, root_num);
//         return;
//     }

//     temp_num = root->left_child;
//     int temp_idx = buffer_read_page(table_id, temp_num, &page);
//     print_page(temp_num, *page); 
//     for (int i = 0; i < root->num_keys; i++) {
//         temp_num = root->entries[i].child;
//         pthread_mutex_unlock(&(buffers[temp_idx]->page_latch));
//         temp_idx = buffer_read_page(table_id, temp_num, &page);
//         print_page(temp_num, *page);
//     }
//     pthread_mutex_unlock(&(buffers[root_idx]->page_latch));
//     pthread_mutex_unlock(&(buffers[temp_idx]->page_latch));
// }

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

    init_db(NUM_BUFS, 0, 0, (char*)"logfile.data", (char*)"logmsg.txt");
	int64_t table_id = open_table((char*)"DATA1");
    int trx_id = trx_begin();
    for (int j = 0; j < 3; j++) {
        shuffle(keys.begin(), keys.end(), rng);

        printf("[REP%2d] ", j);

        printf("[INSERT START]\n");
        for (const auto& i : keys) {
            // printf("insert %4d\n", i);
            sprintf(value, "%02d", i % 100);
            if (db_insert(table_id, i, value, SIZE(i)) != 0) goto func_exit;
        }
        printf("\t[INSERT END]\n");

        printf("\t[FIND START]\n");
        for (const auto& i : keys) {
            memset(value, 0x00, 112);
            val_size = 0;
            // printf("find %4d\n", i);
            if(db_find(table_id, i, value, &val_size, trx_id) != 0) goto func_exit;
            // else if (SIZE(i) != val_size ||
            // 		 val(i) != std::string(value, val_size)) {
            // 	printf("value dismatched\n");
            // 	goto func_exit;
            // }
        }
        printf("\t[FIND END]\n");

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
            if (db_find(table_id, i, value, &val_size, trx_id) == 0) goto func_exit;
        }
        printf("\t[FIND END AGAIN]\n");
    }
    trx_commit(trx_id);

    printf("\n[TEST END]\n\n");    

	func_exit:
	printf("[SHUTDOWN START]\n");
	if (shutdown_db() != 0) {
		return 0;
	}
	printf("[SHUTDOWN END]\n");
}
#endif