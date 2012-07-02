#ifndef BTREE_H
#define BTREE_H

#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>

#define KEY_MAX_LENGTH (20)
#define CACHE_SLOTS	(23)

struct slice {
	char *data;
	int len;
};

struct btree_item {
	char key[KEY_MAX_LENGTH];
	uint64_t offset;
	uint32_t child;
} __attribute__((packed));

#define TABLE_SIZE	((4096 - 1) / sizeof(struct btree_item))

struct btree_table {
	struct btree_item items[TABLE_SIZE];
	uint8_t size;
} __attribute__((packed));

struct btree_cache {
	uint64_t offset;
	struct btree_table *table;
};

struct btree_super {
	uint32_t top;
	uint32_t free_top;
} __attribute__((packed));

struct btree {
	int fd;
	int db_fd;

	uint32_t top;
	uint32_t free_top;
	uint32_t alloc;
	uint32_t db_alloc;
	struct btree_cache cache[CACHE_SLOTS];
};

struct btree *btree_new(const char *file);
void btree_close(struct btree *btree);
void btree_insert(struct btree *btree, struct slice *sk, struct slice *sv);
struct slice *btree_get(struct btree *btree, struct slice *sk);
int btree_delete(struct btree *btree, struct slice *sk);

#endif
