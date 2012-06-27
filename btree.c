#include "btree.h"
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#ifdef _WIN32
#include <winsock.h>
#else
/* Unix */
#include <arpa/inet.h> /* htonl/ntohl */
#define O_BINARY 0
#endif

#define DBEXT 	".db"
#define IDXEXT	".idx"

#define FREE_QUEUE_LEN	64

struct chunk {
	uint32_t offset;
	uint32_t len;
};

static struct chunk free_queue[FREE_QUEUE_LEN];
static size_t free_queue_len = 0;

static int _file_exists(const char *path)
{
	int fd = open(path, O_RDWR);
	if (fd > -1){
		close(fd);
		return 1;
	}

	return 0;
}

static int _cmp_key(const char *a, const char *b)
{
	return strcmp(a, b);
}

static struct btree_table *_alloc_table(struct btree *btree)
{
	struct btree_table *table = malloc(sizeof *table);
	memset(table, 0, sizeof *table);

	return table;
}

static struct btree_table *_get_table(struct btree *btree, uint32_t offset)
{
	assert(offset != 0);

	/* take from cache */
	struct btree_cache *slot = &btree->cache[offset % CACHE_SLOTS];
	if (slot->offset == offset) {
		slot->offset = 0;

		return slot->table;
	}

	struct btree_table *table = malloc(sizeof *table);

	lseek(btree->fd, offset, SEEK_SET);
	if (read(btree->fd, table, sizeof *table) != (ssize_t) sizeof *table) {
		fprintf(stderr, "btree: I/O error\n");
		abort();
	}

	return table;
}

static void _put_table(struct btree *btree, struct btree_table *table, uint32_t offset)
{
	assert(offset != 0);

	struct btree_cache *slot = &btree->cache[offset % CACHE_SLOTS];

	if (slot->offset != 0) {
		free(slot->table);
	}

	slot->offset = offset;
	slot->table = table;
}

static void _flush_table(struct btree *btree, struct btree_table *table, uint32_t offset)
{
	assert(offset != 0);

	lseek(btree->fd, offset, SEEK_SET);

	if (write(btree->fd, table, sizeof *table) != (ssize_t) sizeof *table) {
		fprintf(stderr, "btree: I/O error offset:%d\n", offset);
		abort();
	}

	_put_table(btree, table, offset);
}

static int _btree_open(struct btree *btree, const char *idxname,const char *dbname)
{
	memset(btree, 0, sizeof *btree);
	btree->fd = open(idxname, O_RDWR | O_BINARY);
	btree->db_fd = open(dbname, O_RDWR | O_BINARY);

	if (btree->fd < 0)
		return -1;

	struct btree_super super;
	if (read(btree->fd, &super, sizeof super) != (ssize_t) sizeof super)
		return -1;

	btree->top = super.top;
	btree->free_top = super.free_top;

	btree->alloc = lseek(btree->fd, 0, SEEK_END);
	btree->db_alloc = lseek(btree->db_fd, 0, SEEK_END);

	return 0;
}

static void _flush_super(struct btree *btree)
{
	size_t i;

	for (i = 0; i < free_queue_len; ++i) {
		struct chunk *chunk = &free_queue[i];
		//free_chunk(btree, chunk->offset, chunk->len);
	}

	free_queue_len = 0;

	struct btree_super super;

	memset(&super, 0, sizeof super);
	super.top = btree->top;
	super.free_top = btree->free_top;

	lseek(btree->fd, 0, SEEK_SET);
	if (write(btree->fd, &super, sizeof super) != sizeof super) {
		fprintf(stderr, "btree: I/O error\n");
		abort();
	}
}

static int _btree_creat(struct btree *btree, const char *idxname,const char* dbname)
{
	int magic = 2012;

	memset(btree, 0, sizeof *btree);
	btree->fd = open(idxname, O_RDWR | O_TRUNC | O_CREAT | O_BINARY, 0644);
	btree->db_fd = open(dbname, O_RDWR | O_TRUNC | O_CREAT | O_BINARY, 0644);
	if (btree->fd < 0)
		return -1;

	_flush_super(btree);

	btree->alloc = sizeof(struct btree_super);
	btree->db_alloc = sizeof(int);
	write(btree->db_fd, &magic, sizeof(int));
	
	lseek(btree->fd, 0, SEEK_END);

	return 0;
}

struct btree *btree_new(const char* fname)
{
	char dbname[1024], idxname[1024];
	struct btree *b = malloc(sizeof(*b));

	memset(b, 0, sizeof(*b));
	sprintf(idxname, "%s%s", fname,IDXEXT);
	sprintf(dbname, "%s%s", fname,DBEXT);

	if(_file_exists(idxname))
		_btree_open(b, idxname, dbname);
	else
		_btree_creat(b, idxname, dbname);

	return b;
}

void btree_close(struct btree *btree)
{
	size_t i;

	close(btree->fd);

	for (i = 0; i < CACHE_SLOTS; ++i) {
		if (btree->cache[i].offset)
			free(btree->cache[i].table);
	}
}

static size_t _round_power2(size_t val)
{
	size_t i = 1;
	while (i < val)
		i <<= 1;

	return i;
}

static uint32_t _alloc_chunk(struct btree *btree, size_t len)
{
	assert(len > 0);

	len = _round_power2(len);

	uint32_t offset= btree->alloc;
	if (offset & (len - 1)) {
		offset += len - (offset & (len - 1));
	}

	btree->alloc = offset + len;

	return offset;
}

static uint32_t _alloc_dbchunk(struct btree *btree, size_t len)
{
	assert(len > 0);

	uint32_t offset= btree->db_alloc;
	btree->db_alloc = offset + len;

	return offset;
}

static uint32_t _insert_data(struct btree *btree, struct slice *sv)
{
	uint32_t offset = _alloc_dbchunk(btree, sizeof(int) + sv->len);

	lseek64(btree->db_fd, offset, SEEK_SET);
	if (write(btree->db_fd, &sv->len, sizeof(int)) != sizeof(int)) {
		fprintf(stderr, "btree: I/O error\n");
		abort();
	}

	if (write(btree->db_fd, sv->data, sv->len) != (ssize_t) sv->len) {
		fprintf(stderr, "btree: I/O error\n");
		abort();
	}

	return offset;
}

static uint32_t _split_table(struct btree *btree, struct btree_table *table, char *key, uint32_t *offset)
{
	memcpy(key, table->items[TABLE_SIZE / 2].key, KEY_MAX_LENGTH);
	*offset = table->items[TABLE_SIZE / 2].offset;

	struct btree_table *new_table = _alloc_table(btree);
	new_table->size = table->size - TABLE_SIZE / 2 - 1;

	table->size = TABLE_SIZE / 2;

	memcpy(new_table->items, &table->items[TABLE_SIZE / 2 + 1],
		(new_table->size + 1) * sizeof(struct btree_item));

	uint32_t new_table_offset = _alloc_chunk(btree, sizeof *new_table);
	_flush_table(btree, new_table, new_table_offset);

	return new_table_offset;
}

static uint32_t _insert_table(struct btree *btree, uint32_t table_offset, struct slice *sk, struct slice *sv)
{
	struct btree_table *table = _get_table(btree, table_offset);
	assert(table->size < TABLE_SIZE-1);

	size_t left = 0, right = table->size;
	while (left < right) {
		size_t i = (right - left) / 2 + left;
		int cmp = _cmp_key(sk->data, table->items[i].key);
		if (cmp == 0) {
			uint32_t ret = table->items[i].offset;
			_put_table(btree, table, table_offset);

			return ret;
		}

		if (cmp < 0)
			right = i;
		else
			left = i + 1;
	}
	size_t i = left;

	uint32_t offset = 0;
	uint32_t left_child = table->items[i].child;
	uint32_t right_child = 0; 
	uint32_t ret = 0;

	if (left_child != 0) {
		ret = _insert_table(btree, left_child, sk, sv);

		struct btree_table *child = _get_table(btree, left_child);
		if (child->size < TABLE_SIZE-1) {
			_put_table(btree, table, table_offset);
			_put_table(btree, child, left_child);

			return ret;
		}

		right_child = _split_table(btree, child, sk->data, &offset);
		_flush_table(btree, child, left_child);
	} else {
		ret = offset = _insert_data(btree, sv);
	}

	table->size++;
	memmove(&table->items[i + 1], &table->items[i],
		(table->size - i) * sizeof(struct btree_item));
	memcpy(table->items[i].key, sk->data, KEY_MAX_LENGTH);
	table->items[i].offset = offset;
	table->items[i].child = left_child;
	table->items[i + 1].child = right_child;

	_flush_table(btree, table, table_offset);

	return ret;
}

uint32_t _insert_toplevel(struct btree *btree, uint32_t *table_offset, struct slice *sk, struct slice *sv)
{
	uint32_t offset = 0;
	uint32_t ret = 0;
	uint32_t right_child = 0;

	if (*table_offset != 0) {
		ret = _insert_table(btree, *table_offset, sk, sv);

		struct btree_table *table = _get_table(btree, *table_offset);
		if (table->size < TABLE_SIZE-1) {
			_put_table(btree, table, *table_offset);

			return ret;
		}

		right_child = _split_table(btree, table, sk->data, &offset);
		_flush_table(btree, table, *table_offset);
	} else {
		ret = offset = _insert_data(btree, sv);
	}

	/* create new top level table */
	struct btree_table *new_table = _alloc_table(btree);
	new_table->size = 1;
	memcpy(new_table->items[0].key, sk->data, sk->len);
	new_table->items[0].offset = offset;
	new_table->items[0].child = *table_offset;
	new_table->items[1].child = right_child;

	uint32_t new_table_offset = _alloc_chunk(btree, sizeof *new_table);
	_flush_table(btree, new_table, new_table_offset);

	*table_offset = new_table_offset;

	return ret;
}

void btree_insert(struct btree *btree, struct slice *sk, struct slice *sv)
{
	_insert_toplevel(btree, &btree->top, sk, sv);
	_flush_super(btree);
}

static uint32_t _lookup(struct btree *btree, uint32_t table_offset, const char *key)
{
	while (table_offset) {
		struct btree_table *table = _get_table(btree, table_offset);
		size_t left = 0, right = table->size, i;

		while (left < right) {
			i = (right - left) / 2 + left;
			int cmp = _cmp_key(key, table->items[i].key);
			if (cmp == 0) {
				uint32_t ret = table->items[i].offset;
				_put_table(btree, table, table_offset);

				return ret;
			}
			if (cmp < 0)
				right = i;
			else
				left = i + 1;
		}
		uint32_t  child = table->items[left].child;

		_put_table(btree, table, table_offset);
		table_offset = child;
	}

	return 0;
}

struct slice *btree_get(struct btree *btree, struct slice *sk)
{
	uint32_t offset;
	struct blob_info info;
	char *data;
	struct slice *sv;
	
	sv = calloc(1, sizeof(*sv));

	offset = _lookup(btree, btree->top, sk->data);
	if (offset == 0)
		return NULL;

	lseek(btree->db_fd, offset, SEEK_SET);

	if (read(btree->db_fd, &info, sizeof info) != (ssize_t) sizeof info)
		return NULL;

	sv->len = info.len;
	data = malloc(sv->len + 1);
	memset(data, 0, sv->len + 1);

	if (read(btree->db_fd, data, sv->len) != sv->len) {
		free(data);
		free(sv);

		return NULL;
	}
	sv->data = data;

	return sv;
}
