#include "btree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#define OP_ADD 1
#define OP_GET 2
#define OP_WALK 3

#define R_NUM 		10000
#define NUM		2000000

#define DBNAME		"db_btree"
#define V 		"1.1"
#define KEYSIZE 	16
#define VALSIZE 	100
#define LINE 		"+-----------------------+---------------------------+----------------------------------+---------------------+\n"
#define LINE1		"--------------------------------------------------------------------------------------------------------------\n"

#define warning(fmt...)	fprintf(stderr, "WARNING: " fmt)


static struct timespec start;
static struct btree btree;
static void start_timer(void)
{
        clock_gettime(CLOCK_MONOTONIC, &start);
}

static double get_timer(void)
{
        struct timespec end;
        clock_gettime(CLOCK_MONOTONIC, &end);
        long seconds  = end.tv_sec  - start.tv_sec;
        long nseconds = end.tv_nsec - start.tv_nsec;
        return seconds + (double) nseconds / 1.0e9;
}

static char val[VALSIZE+1]={0};
double _file_size=((double)(KEYSIZE+8*6)*NUM)/1048576.0+((double)(VALSIZE+8*3)*NUM)/1048576.0;
double _query_size=(double)((double)(KEYSIZE+VALSIZE+8*4)*R_NUM)/1048576.0;

void random_value()
{
	char salt[10]={'1','2','3','4','5','6','7','8','a','b'};
	int i;
	for(i=0;i<VALSIZE;i++)
	{
		val[i]=salt[rand()%10];
	}
}

void print_header()
{
	printf("Keys:		%d bytes each\n",KEYSIZE);
	printf("Values:		%d bytes each\n",VALSIZE);
	printf("Entries:	%d\n",NUM);
	printf("IndexSize:	%.1f MB (estimated)\n",(double)((double)(KEYSIZE+8*6)*NUM)/1048576.0);
	printf("DBSize:		%.1f MB (estimated)\n",(double)((double)(VALSIZE+8*3)*NUM)/1048576.0);
	printf(LINE1);
}

void print_environment()
{
	printf("BTreeDB:	version %s\n",V);		
	time_t now=time(NULL);
	printf("Date:		%s",(char*)ctime(&now));

	int num_cpus=0;
	char cpu_type[256]={0};
	char cache_size[256]={0};

	FILE* cpuinfo=fopen("/proc/cpuinfo","r");
	if(cpuinfo)
	{
		char line[1024]={0};
		while(fgets(line,sizeof(line),cpuinfo)!=NULL)
		{
			const char* sep=strchr(line,':');
			if(sep==NULL||strlen(sep)<10)
				continue;

			char key[1024]={0};
			char val[1024]={0};
			strncpy(key,line,sep-1-line);
			strncpy(val,sep+1,strlen(sep)-1);
			if(strcmp("model name",key)==0)
			{
				num_cpus++;
				strcpy(cpu_type,val);
			}
			else if(strcmp("cache size",key)==0)
			{
				strncpy(cache_size,val+1,strlen(val)-1);	
			}
		}

		fclose(cpuinfo);
		printf("CPU:		%d * %s",num_cpus,cpu_type);
		printf("CPUCache:	%s\n",cache_size);
	}
}

void db_write_test()
{
	uint8_t key[KEYSIZE];
	int v_len=strlen(val);
	start_timer();
	int i;
	for (i = 0; i < NUM; ++i)
	{
		memset(key,0,sizeof(key));
		sprintf((char *) key, "%dkey", i);
		btree_insert(&btree, key, val, v_len);
		if((i%10000)==0)
		{
			fprintf(stderr,"finished %d ops%30s\r",i,"");
			fflush(stderr);
		}
	}
	printf(LINE);
	double cost=get_timer();
	printf("|write		(succ:%ld): %.6f sec/op; %.1f writes/sec(estimated); %.1f MB/sec; cost:%.6f(sec)\n"
		,NUM
		,(double)(cost/NUM)
		,(double)(NUM/cost)
		,(_file_size/cost)
		,(double)cost);		
}

void db_read_seq_test()
{
	uint8_t key[KEYSIZE];
	int all=0,i;
	int start=NUM/2;
	int end=start+R_NUM;
	start_timer();
	for (i = start; i <end ; ++i) 
	{
		memset(key,0,sizeof(key));
		sprintf(key, "%dkey", i);

		size_t len;
		void *data = btree_get(&btree,key, &len);
		if(data!=NULL)
			all++;
		else
			printf("not found:%s\n",key);

		free(data);

		 if((i%10000)==0)
		{
			fprintf(stderr,"finished %d ops%30s\r",i,"");
			fflush(stderr);
		}     
	}
	printf(LINE);
	double cost=get_timer();
	printf("|readseq	(found:%ld): %.6f sec/op; %.1f reads /sec(estimated); %.1f MB/sec; cost:%.6f(sec)\n"
	,R_NUM
	,(double)(cost/R_NUM)
	,(double)(R_NUM/cost)
	,(_query_size/cost)
	,cost);
}

void db_read_random_test()
{
	uint8_t key[KEYSIZE];
	int all=0,i;
	int start=NUM/2;
	int end=start+R_NUM;
	start_timer();
	for (i = start; i <end ; ++i) 
	{
		memset(key,0,sizeof(key));
		sprintf(key, "%dkey", rand()%(i+1));

		size_t len;
		void *data = btree_get(&btree,key, &len);
		if(data!=NULL)
			all++;
		else
			printf("not found:%s\n",key);

		free(data);

		 if((i%10000)==0)
		{
			fprintf(stderr,"finished %d ops%30s\r",i,"");
			fflush(stderr);
		}     
	}
	printf(LINE);
	double cost=get_timer();
	printf("|readrandom	(found:%ld): %.6f sec/op; %.1f reads /sec(estimated); %.1f MB/sec; cost:%.6f(sec)\n"
	,R_NUM
	,(double)(cost/R_NUM)
	,(double)(R_NUM/cost)
	,(_query_size/cost)
	,cost);
}

void db_init_test()
{
	random_value();
	btree_init(&btree,DBNAME);
}

void db_tests()
{
	db_write_test();
	db_read_seq_test();
	db_read_random_test();
	printf(LINE);
}


int main(int argc, char **argv)
{

	long i,count,op;
	if(argc!=2)
	{
		fprintf(stderr,"Usage: nessdb_benchmark <op>\n");
        	exit(1);
	}

	if(strcmp(argv[1],"add")==0)
		op=OP_ADD;
	else if(strcmp(argv[1],"get")==0)
		op=OP_GET;
	else if(strcmp(argv[1],"walk")==0)
		op=OP_WALK;
	else
	{
		printf("not supported op %s\n", argv[1]);
        	exit(1);
    	}


	srand(time(NULL));
	print_header();
	print_environment();

	db_init_test();
	if(op==OP_ADD)
		db_tests();
	else if(op==OP_WALK)
		db_read_seq_test();
	else if(op==OP_GET)
		db_read_random_test();
	return 1;
	btree_close(&btree);

	return 0;
}
