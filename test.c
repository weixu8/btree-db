/*
 * B-tree storage engine
 * Copyright (c) 2011-2012, BohuTANG <overred.shuttler at gmail dot com>
 * All rights reserved.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include "btree.h"


#define KSIZE (16)
#define VSIZE (80)
#define V "1.8"
#define LINE "+-----------------------------+----------------+------------------------------+-------------------+\n"
#define LINE1 "---------------------------------------------------------------------------------------------------\n"

long long get_ustime_sec(void)
{
	struct timeval tv;
	long long ust;

	gettimeofday(&tv, NULL);
	ust = ((long long)tv.tv_sec)*1000000;
	ust += tv.tv_usec;
	return ust / 1000000;
}

void _random_key(char *key,int length) 
{
	int i;
	char salt[36]= "abcdefghijklmnopqrstuvwxyz0123456789";

	for (i = 0; i < length; i++)
		key[i] = salt[rand() % 36];
}

void _print_header(int count)
{
	double index_size = (double)((double)(KSIZE + 8 + 1) * count) / 1048576.0;
	double data_size = (double)((double)(VSIZE + 4) * count) / 1048576.0;

	printf("Keys:		%d bytes each\n", KSIZE);
	printf("Values:		%d bytes each\n", VSIZE);
	printf("Entries:	%d\n", count);
	printf("IndexSize:	%.1f MB (estimated)\n", index_size);
	printf("DataSize:	%.1f MB (estimated)\n", data_size);
	printf(LINE1);
}

void _print_environment()
{
	printf("B-Tree:		version %s(for nessDB storage engine)\n", V);

	int num_cpus = 0;
	char cpu_type[256] = {0};
	char cache_size[256] = {0};

	FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
	if (cpuinfo) {
		char line[1024] = {0};
		while (fgets(line, sizeof(line), cpuinfo) != NULL) {
			const char* sep = strchr(line, ':');
			if (sep == NULL || strlen(sep) < 10)
				continue;

			char key[1024] = {0};
			char val[1024] = {0};
			strncpy(key, line, sep-1-line);
			strncpy(val, sep+1, strlen(sep)-1);
			if (strcmp("model name", key) == 0) {
				num_cpus++;
				strcpy(cpu_type, val);
			}
			else if (strcmp("cache size", key) == 0)
				strncpy(cache_size, val + 1, strlen(val) - 1);	
		}

		fclose(cpuinfo);
		printf("CPU:		%d * %s", num_cpus, cpu_type);
		printf("CPUCache:	%s\n", cache_size);
	}
}

void _write_test(long int count)
{
	int i;
	double cost;
	long long start,end;
	struct slice sk, sv;
	struct btree *btree;

	char key[KSIZE + 1];
	char val[VSIZE + 1];


	btree = btree_new("test");
	start = get_ustime_sec();
	for (i = 0; i < count; i++) {
		memset(key, 0, KSIZE + 1);
		memset(val, 0, VSIZE + 1);

		_random_key(key, KSIZE);
		snprintf(val, VSIZE, "val:%d", i);

		sk.len = KSIZE;
		sk.data = key;
		sv.len = VSIZE;
		sv.data = val;

		btree_insert(btree, &sk, &sv);
		if ((i % 10000) == 0) {
			fprintf(stderr,"random write finished %d ops%30s\r", i, "");
			fflush(stderr);
		}
	}

	btree_close(btree);

	end = get_ustime_sec();
	cost = end -start;

	printf(LINE);
	printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,count, (double)(cost / count)
		,(double)(count / cost)
		,cost);	
}

void _readone_test(char *key)
{
	struct slice sk;
	struct slice *sv;
	struct btree *btree;

	char k[KSIZE + 1];
	int len = strlen(key);

	memset(k, 0, KSIZE + 1);
	memcpy(k, key, len);

	btree = btree_new("test");
	sk.len = (KSIZE + 1);
	sk.data = k;

	sv = btree_get(btree, &sk);
	if (sv){ 
		printf("Get Key:value is :<%s>\n", sv->data);
		free(sv->data);
		free(sv);
	} else
		printf("Get Key:<%s>,but value is NULL\n", key);

	btree_close(btree);
}

int main(int argc,char** argv)
{
	long int count;

	srand(time(NULL));
	if (argc != 3) {
		fprintf(stderr,"Usage: db-bench <op: write | readone> <count>\n");
		exit(1);
	}
	
	if (strcmp(argv[1], "write") == 0) {
		count = atoi(argv[2]);
		_print_header(count);
		_print_environment();
		_write_test(count);
	} else if (strcmp(argv[1], "readone") == 0) {
		_readone_test(argv[2]);
	} else {
		fprintf(stderr,"Usage: db-bench <op: write | readone> <count>\n");
		exit(1);
	}

	return 1;
}
