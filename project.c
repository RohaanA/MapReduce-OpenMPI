/* 
================================================================================================
 * =-=-=-=   PDC Project - Implementation of Map-Shuffle-Reduce Algorithm using OpenMPI  =-=-=-=
 * =-=-=-=                                       by                                      =-=-=-=
 * =-=-=-=                             Rohaan Atique (20I-0410)                          =-=-=-= 
 * =-=-=-=                              Zubair Fawad (20I-1755)                          =-=-=-=
 * =-=-=-=                               Ahmed Moiz (20I-2603)                           =-=-=-=
================================================================================================

README:

This program is a simple implementation of the Map-Shuffle-Reduce algorithm using OpenMPI.
Currently, it is designed to multiply two square matrices.

### Tasks Dictionary: 
    Task ID 1: Map
    Task ID 2: Shuffle
    Task ID 3: Reduce
    Task ID 4: Exit Call
### Matrix Files:
    matrixFile_1: Matrix A
    matrixFile_2: Matrix B
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <time.h>
#include <math.h>
#include <stdbool.h>
#include <unistd.h>
// Custom Files
#include "utils.h"
#include "hashmap.h"
#include "hashmap.c"

#define MASTER_RANK 0
#define debugMode true

//TASK DICTIONARY
#define MAP_TASK 1 
#define SHUFFLE_TASK 2
#define REDUCE_TASK 3
#define EXIT_CALL 4 //Ask slaves to exit.

// MATRICES
#define matrixFile_1 "m1.txt"
#define matrixFile_2 "m2.txt"
#define matrixSize 2 

// #define matrixFile_1 "random_matrix_1.txt"
// #define matrixFile_2 "random_matrix_2.txt"
// #define matrixSize 4096

// Structs used for passing data between processes
struct mapLoad {
    int lineStart;
    int lineEnd;
};
struct entry {
    char* key;
    char* value;
};

// Hashmap Functions
int entry_compare(const void *a, const void *b, void *udata) {
    const struct entry *ea = a;
    const struct entry *eb = b;
    return strcmp(ea->key, eb->key);
}

void *hashmap_set_concat(struct hashmap *map, const char *key, const char *value) {
    char delim = '|';

    if (!key) {
        panic("key is null");
    }
    map->oom = false;
    if (map->count == map->growat) {
        if (!resize(map, map->nbuckets*2)) {
            map->oom = true;
            return NULL;
        }
    }

    // Create a temporary entry with the given key and value
    struct entry temp_entry = { .key = (char *)key, .value = (char *)value };
    
    struct bucket *entry = map->edata;
    entry->hash = get_hash(map, &temp_entry);
    entry->dib = 1;
    memcpy(bucket_item(entry), &temp_entry, map->elsize);
    
    size_t i = entry->hash & map->mask;
    for (;;) {
        struct bucket *bucket = bucket_at(map, i);
        if (bucket->dib == 0) {
            memcpy(bucket, entry, map->bucketsz);
            map->count++;
            return NULL;
        }
        if (entry->hash == bucket->hash && 
            map->compare(bucket_item(entry), bucket_item(bucket), 
                         map->udata) == 0)
        {
            // Concatenate the new value with the existing value
            size_t old_value_len = strlen(((struct entry *)bucket_item(bucket))->value);
            size_t new_value_len = strlen(value);
            size_t delimiter_len = 1;
            char *new_value = malloc(old_value_len + delimiter_len + new_value_len + 1);
            if (!new_value) {
                map->oom = true;
                return NULL;
            }
            memcpy(new_value, ((struct entry *)bucket_item(bucket))->value, old_value_len);
            new_value[old_value_len] = delim; // Add a delimiter between old and new values
            memcpy(new_value + old_value_len + delimiter_len, value, new_value_len);
            new_value[old_value_len + delimiter_len + new_value_len] = '\0';
            ((struct entry *)bucket_item(bucket))->value = new_value;
            return ((struct entry *)bucket_item(bucket))->value;
        }
        if (bucket->dib < entry->dib) {
            memcpy(map->spare, bucket, map->bucketsz);
            memcpy(bucket, entry, map->bucketsz);
            memcpy(entry, map->spare, map->bucketsz);
        }
        i = (i + 1) & map->mask;
        entry->dib += 1;
    }
}



bool entry_iter(const void *item, void *udata) {
    const struct entry *entry = item;
    printf("%s=%s\n", entry->key, entry->value);
    return true;
}

uint64_t entry_hash(const void *item, uint64_t seed0, uint64_t seed1) {
    const struct entry *entry = item;
    return hashmap_sip(entry->key, strlen(entry->key), seed0, seed1);
}

int** readMatrixLines(char* filename, int startingLine, int N) {
    FILE* fp = fopen(filename, "r");
    if (fp == NULL) {
        printf("Unable to open file.\n");
        return NULL;
    }

    int** matrix = (int**) malloc(N * sizeof(int*));
    for (int i = 0; i < N; i++) {
        matrix[i] = (int*) malloc(4096 * sizeof(int));
    }

    char buff[4096*10];  // buffer to store each line
    int line = 0;
    while (fgets(buff, sizeof(buff), fp)) {
        if (line >= startingLine && line < startingLine + N) {
            char* token = strtok(buff, " ");
            int col = 0;
            while (token != NULL) {
                matrix[line-startingLine][col] = atoi(token);
                token = strtok(NULL, " ");
                col++;
            }
        }
        line++;
    }
    fclose(fp);
    return matrix;
}

/*
 * MAIN FUNCTION
 */
int main(int argc, char *argv[]) {
    int rank, size;
    char proc_name[100];
    int proc_name_len;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Get_processor_name(proc_name, &proc_name_len);
    //Initialize the hashmap to be used between mapper and reducer
    struct hashmap *map = hashmap_new(sizeof(struct entry), 0, 0, 0, entry_hash, entry_compare, NULL, NULL);
    //Defining the MPI Datatype for the Map load
    //////////////////////////////////
    /////// MPI Datatype: mapLoad_mpi_type
    /////// Description: Used to store map data for each process
    /////// Elements: lineStart, lineEnd
    //////////////////////////////////
        int block_lengths[2] = {1, 1};
        MPI_Datatype types[2] = {MPI_INT, MPI_INT};
        MPI_Aint offsets[2] = {offsetof(struct mapLoad, lineStart), offsetof(struct mapLoad, lineEnd)};
        MPI_Datatype mapLoad_mpi_type;
        MPI_Type_create_struct(2, block_lengths, offsets, types, &mapLoad_mpi_type);
        MPI_Type_commit(&mapLoad_mpi_type);
    //////////////////////////////////
    /////// MPI Datatype: hashmap_mpi_type
    /////// Description: Used to store hashmap data for each process
    /////// Elements: 


    if (rank == MASTER_RANK) {
        char buff [50]; //Used for storing pre-formatted strings

        sprintf(buff, "Master with process id %d, running on %s", rank, proc_name);
        logger(rank, buff);
        bool isDataSmall = false;
        //Based on size, utilize all processes to map
        int totalLoad = (matrixSize*2); //MatrixSize * 2 as we have 2 matrices
        int loadPerMapper =  totalLoad / (size-1); //Size-1 as master does not map 
        if (loadPerMapper < 1) {
            raiseError(rank, "Matrix size is too small for the number of mappers. ");
            raiseWarning(rank, "Due to size mismatch. Some mappers will be idle.");
            isDataSmall = true;
            loadPerMapper = 1;
        }
        //Assign mappers to map
        logger(rank, "=========================================================================");
        logger(rank, "                           Starting Map Phase                            ");
        logger(rank, "=========================================================================");
        sprintf(buff, "Total Load: %d", totalLoad);
        debug_logger(debugMode, rank, buff);
        sprintf(buff, "Load Per Mapper: %d", loadPerMapper);
        debug_logger(debugMode, rank, buff);
    
        int startLine = 0;
        //Sending Map Tasks
        for(int mapperRank=0; mapperRank<size; mapperRank++) {
            if (mapperRank == MASTER_RANK) continue;
            int task = MAP_TASK;
            announceTask(mapperRank, "assigned", proc_name, "map");            
            
            if (totalLoad < loadPerMapper)
                loadPerMapper = 0;
            else totalLoad -= loadPerMapper;

            struct mapLoad assignedLoad = {startLine, startLine+loadPerMapper};
            startLine += loadPerMapper;

            MPI_Send(&task, 1, MPI_INT, mapperRank, 0, MPI_COMM_WORLD);
            // MPI_Send(&loadPerMapper, 1, MPI_INT, mapperRank, 0, MPI_COMM_WORLD);
            MPI_Send(&assignedLoad, 1, mapLoad_mpi_type, mapperRank, 0, MPI_COMM_WORLD);
            // In the Map phase, each mapper is assigned a block of rows from matrix A and a block of columns from matrix B. 
            // The mapper multiplies the two blocks and produces a block of the resulting matrix C.

        }
        for(int mapperRank=0; mapperRank<size; mapperRank++) {
            if (mapperRank == MASTER_RANK) continue;

            int task;
            MPI_Recv(&task, 1, MPI_INT, mapperRank, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (task == MAP_TASK) {
                announceCompletion(mapperRank+1, "map");
            }
            else raiseError(rank, "Invalid task received");
        }
        

    }
    else { //All Slave Processes
        while(1) {
            int task;
            MPI_Recv(&task, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (task == MAP_TASK) {
                char buff[50]; /* Used to store pre-formatted strings */
                int load; /* Which rows of file to map. */
                announceTask(rank, "received", proc_name, "map");
                // /////////////////////////////////////////////////////////////
                // MPI_Recv(&load, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                struct mapLoad received_map_load;
                MPI_Recv(&received_map_load, 1, mapLoad_mpi_type, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                load = received_map_load.lineEnd - received_map_load.lineStart;
                sprintf(buff, "Mapper received matrix rows %d", received_map_load.lineStart);
                debug_logger(debugMode, rank, buff);
                if (load < 1) {
                    logger(rank, "Mapper received no load. Idling.");
                }
                else {
                    
                    bool isMatrixB = (received_map_load.lineStart >= matrixSize);
                    if (isMatrixB)
                        debug_logger(debugMode, rank, "==> I am doing matrix B");
    
                    //First load the matrix lines needed from the file
                    printf("%d - %d", load, matrixSize);
                    printf("Loading matrix lines %d to %d\n", received_map_load.lineStart, received_map_load.lineEnd);
                    int** matrix; 
                    if (isMatrixB)
                        matrix = readMatrixLines(matrixFile_2, received_map_load.lineStart-matrixSize, load);
                    else matrix = readMatrixLines(matrixFile_1, received_map_load.lineStart, load);

                //     //Map the matrix to hashmap
                //     //How mapping works:
                //     //Matrix A: rows are called i, columns are called j
                //     //Matrix B: rows are called j, columns are called k
                //     //Hash map entry are stored as,
                //     // key: (i,k)
                //     // value: (A, j, Aij) for Matrix A
                //     // value: (B, j, Bjk) for Matrix B
                //     //This is done to make it easier to do the multiplication later
                //     //Example Hashmap entry for Matrix A:
                //     // key: (0,0)
                //     // value: (A, 0, 1)
                //     // hashmap_set(map, &(struct entry){ .key="key1", .value="value1" });
                //     // Loop through the matrix and add entries to the hashmap
                    if (isMatrixB) {
                        int row = received_map_load.lineStart - matrixSize;
                        for(int j=0; j<load; j++) {
                            for(int k=0; k<matrixSize; k++) {
                                for(int i=0; i<matrixSize; i++) {
                                    char key[10]; char value[10];

                                    // printf("i=%d, j=%d, k=%d\n", i, j, k);
                                    sprintf(key, "(%d,%d)", i, k);
                                    sprintf(value, "(B,%d,%d)", j+row, matrix[j][k]);
                                    // //Write key-value to file
                                    // Define mapFile
                                    char* mapFileName = malloc(50);
                                    sprintf(mapFileName, "map_%d.txt", rank);
                                    FILE* mapFile = fopen(mapFileName, "a");
                                    fprintf(mapFile, "%s=%s\n", key, value);
                                    fclose(mapFile);
                                    printf("(rank: %d) %s=%s\n", rank, key, value);
                                    // hashmap_set(map, &(struct entry){ .key=key, .value=value });
                                    
                                    // Create a new entry with the key and value
                                    struct entry* new_entry = malloc(sizeof(struct entry));
                                    new_entry->key = strdup(key);
                                    new_entry->value = strdup(value);
                                    
                                    // Insert the entry into the hashmap
                                    // hashmap_set(map, new_entry);
                                    hashmap_set_concat(map, new_entry->key, new_entry->value);
                                }
                            }
                        }
                    }
                    else { //Matrix A
                        for(int i=0; i<load; i++) {
                            for(int j=0; j<matrixSize; j++) {
                                for(int k=0; k<matrixSize; k++) {
                                    char key[10];
                                    sprintf(key, "(%d,%d)", received_map_load.lineStart+i, k);
                                    char value[10];
                                    sprintf(value, "(A,%d,%d)", j, matrix[i][j]);
                                    //Print key-value
                                    // Define mapFile
                                    char* mapFileName = malloc(50);
                                    sprintf(mapFileName, "map_%d.txt", rank);
                                    FILE* mapFile = fopen(mapFileName, "a");
                                    fprintf(mapFile, "%s=%s\n", key, value);
                                    fclose(mapFile);
                                    printf("(rank: %d) %s=%s\n", rank, key, value);
                                    // hashmap_set(map, &(struct entry){ .key=key, .value=value });
                                    
                                    // Create a new entry with the key and value
                                    struct entry* new_entry = malloc(sizeof(struct entry));
                                    new_entry->key = strdup(key);
                                    new_entry->value = strdup(value);
                                    
                                    // Insert the entry into the hashmap
                                    // hashmap_set(map, new_entry);
                                    hashmap_set_concat(map, new_entry->key, new_entry->value);
                                }
                            }
                        }
                    }
                    printf("\n-- iterate over all entries (rank: %d) --\n", rank);
                    size_t iter = 0;
                    void *item;
                    while (hashmap_iter(map, &iter, &item)) {
                        const struct entry *entry = item;
                        printf("%s=%s\n", entry->key, entry->value);
                    }

                }
                // /////////////////////////////////////////////////////////////
                MPI_Send(&task, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD); // Send confirmation that the task has been completed.
                //Send Back the map
                
            }
            else if (task == REDUCE_TASK) {
                announceTask(rank, "received", proc_name, "reduce");
                /////////////////////////////////////////////////////////////
                // Do Reduction
                /////////////////////////////////////////////////////////////
                MPI_Send(&task, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD);
            }
            else if (task == SHUFFLE_TASK) {
                announceTask(rank, "received", proc_name, "shuffle");
                /////////////////////////////////////////////////////////////
                // Do Shuffle
                /////////////////////////////////////////////////////////////
                MPI_Send(&task, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD);
            }
            else if (task == EXIT_CALL) {
                announceTask(rank, "received", proc_name, "exit");
                //Echo back that you've ended.
                MPI_Send(&task, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD);
                break;
            }
            else raiseError(rank, "Invalid task received");
        }
        MPI_Finalize();
        logger(rank, "Exiting");
        exit(0);
    }

//Finish MPI
    hashmap_free(map);
    MPI_Finalize();
}