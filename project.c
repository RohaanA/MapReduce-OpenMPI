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
#include "mpi_ftns.c"

#define MASTER_RANK 0
#define debugMode true

//TASK DICTIONARY
#define MAP_TASK 1 
#define SHUFFLE_TASK 2
#define REDUCE_TASK 3
#define EXIT_CALL 4 //Ask slaves to exit.
#define HASHMAP_DELIM "|" //Delimiter for hashmap entries

// MATRICES
// #define matrixFile_1 "m1.txt"
// #define matrixFile_2 "m2.txt"
// #define matrixSize 2

#define matrixFile_1 "m1_4.txt"
#define matrixFile_2 "m2_4.txt"
#define matrixSize 4

// #define matrixFile_1 "random_matrix_1.txt"
// #define matrixFile_2 "random_matrix_2.txt"
// #define matrixSize 4

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
    /////// MPI Datatype: MPI_MAPRECEIVE
    /////// Description: Used to store hashmap data for each process
    /////// Elements: key, value
    //////////////////////////////////


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
            MPI_Send(&assignedLoad, 1, mapLoad_mpi_type, mapperRank, 0, MPI_COMM_WORLD);

        }
        for(int mapperRank=0; mapperRank<size; mapperRank++) {
            if (mapperRank == MASTER_RANK) continue;  int task;

            MPI_Recv(&task, 1, MPI_INT, mapperRank, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (task == MAP_TASK) {
                announceCompletion(mapperRank+1, "map");
                int entryCount;
                //Receive entryCount from mapper
                MPI_Recv(&entryCount, 1, MPI_INT, mapperRank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                if (entryCount == 0) //Idle mapper returns 0 entries
                    continue;

                // printf("Recieved Entry Count: %d from rank: %d \n", entryCount, mapperRank);
                //Allocate memory for receiving entries
                struct entry *entries = malloc(sizeof(struct entry) * entryCount);
                for(int i=0; i<entryCount; i++) {
                    int keyLength, valueLength;
                    MPI_Recv(&keyLength, 1, MPI_INT, mapperRank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    MPI_Recv(&valueLength, 1, MPI_INT, mapperRank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                    entries[i].key = malloc(sizeof(char) * keyLength);
                    entries[i].value = malloc(sizeof(char) * valueLength);

                    MPI_Recv(entries[i].key, keyLength, MPI_CHAR, mapperRank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    MPI_Recv(entries[i].value, valueLength, MPI_CHAR, mapperRank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                    // printf("Recieved Entry: %s, %s from rank: %d \n", entries[i].key, entries[i].value, mapperRank);

                    //Append to hashmap
                    hashmap_set_concat(map, entries[i].key, entries[i].value);
                }
            }
            else raiseError(rank, "Invalid task received");
        }
        
        logger(rank, "=========================================================================");
        logger(rank, "                           Finished Map Phase                            ");
        logger(rank, "=========================================================================");
        logger(rank, "=========================================================================");
        logger(rank, "                       Starting Combine/Shuffle Phase                    ");
        logger(rank, "=========================================================================");
            printf("\n-- iterate over all entries (rank: %d) --\n", rank);
            size_t iter = 0;
            void *item;
            while (hashmap_iter(map, &iter, &item)) {
                const struct entry *entryRow = item;
                printf("key: %s, value: %s\n", entryRow->key, entryRow->value);
            }
        logger(rank, "=========================================================================");
        logger(rank, "                           Finished Combine Phase                        ");
        logger(rank, "=========================================================================");
        logger(rank, "=========================================================================");
        logger(rank, "                           Starting Reduce Phase                         ");
        logger(rank, "=========================================================================");

        int totalEntries = (int) hashmap_count(map); int entriesPerReducer = 0;

        if (size > totalEntries)
            logger(rank, "Less data, not all reducers required.");
        sprintf(buff, "Total Entries: %d", totalEntries);
        debug_logger(debugMode, rank, buff);
                if (totalEntries > (size-1))
                    entriesPerReducer = totalEntries / (size-1);
                else entriesPerReducer = 1;
        sprintf(buff, "Entries Per Reducer: %d", entriesPerReducer);
        debug_logger(debugMode, rank, buff);
        
        int currentEntriesDone = entriesPerReducer;
        int remainingEntries = totalEntries;
        //Initialize iterator
        iter = 0;
        int reducerRank = 0; int task = REDUCE_TASK;
        if (reducerRank == MASTER_RANK) reducerRank++;
        
        //Send reduce task to first
        {
        MPI_Send(&task, 1, MPI_INT, reducerRank, 0, MPI_COMM_WORLD);
        MPI_Send(&entriesPerReducer, 1, MPI_INT, reducerRank, 0, MPI_COMM_WORLD);
        announceTask(reducerRank, "assigned", proc_name, "reduce");
        printf("Remaining Entries: %d\n", remainingEntries);
        }
        while(remainingEntries > 0) {
            if (reducerRank == MASTER_RANK) { reducerRank++; continue;}

            if (currentEntriesDone != 0) {
                hashmap_iter(map, &iter, &item);
                printf("Sending to rank: %d\n", reducerRank);
                const struct entry *entryRow = item;

                int keyLength = strlen(entryRow->key)+1;
                int valueLength = strlen(entryRow->value)+1;

                MPI_Send(&keyLength, 1, MPI_INT, reducerRank, 0, MPI_COMM_WORLD);
                MPI_Send(&valueLength, 1, MPI_INT, reducerRank, 0, MPI_COMM_WORLD);
                
                MPI_Send(entryRow->key, keyLength, MPI_CHAR, reducerRank, 0, MPI_COMM_WORLD);
                MPI_Send(entryRow->value, valueLength, MPI_CHAR, reducerRank, 0, MPI_COMM_WORLD);
                currentEntriesDone--;
            }
            else {                
                reducerRank++;
                if (reducerRank > size) break;
                currentEntriesDone = entriesPerReducer;
                remainingEntries -= entriesPerReducer;

                if (remainingEntries <= 0)
                    break;
                printf("Remaining Entries: %d\n", remainingEntries);
                MPI_Send(&task, 1, MPI_INT, reducerRank, 0, MPI_COMM_WORLD);
                MPI_Send(&entriesPerReducer, 1, MPI_INT, reducerRank, 0, MPI_COMM_WORLD);
                announceTask(reducerRank, "assigned", proc_name, "reduce");

            }
        }

        //Receive sums from reducers
        for(int i=1; i<size; i++) {
            //Receive task confirmation
            int task;
            MPI_Recv(&task, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (task != REDUCE_TASK) raiseError(rank, "Invalid task received");
            //Receive sum
            double sum;
            MPI_Recv(&sum, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            printf("[MASTER]: Sum from rank %d: %f\n", i, sum);
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
                    int entryCount = 0;
                    MPI_Send(&task, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD); // Send confirmation that the task has been completed.
                    MPI_Send(&entryCount, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD); // Tell master entryCount 0 so it can skip.
                    continue;
                }
                else {
                    
                    bool isMatrixB = (received_map_load.lineStart >= matrixSize);
                    if (isMatrixB)
                        debug_logger(debugMode, rank, "==> I am doing matrix B");
    
                    //First load the matrix lines needed from the file
                    // printf("%d - %d", load, matrixSize);
                    // printf("Loading matrix lines %d to %d\n", received_map_load.lineStart, received_map_load.lineEnd);
                    int** matrix; 
                    if (isMatrixB)
                        matrix = readMatrixLines(matrixFile_2, received_map_load.lineStart-matrixSize, load);
                    else matrix = readMatrixLines(matrixFile_1, received_map_load.lineStart, load);
                    if (isMatrixB) {
                        int row = received_map_load.lineStart - matrixSize;
                        for(int j=0; j<load; j++) {
                            for(int k=0; k<matrixSize; k++) {
                                for(int i=0; i<matrixSize; i++) {
                                    char* key = malloc(10); char* value = malloc(10);

                                    // printf("i=%d, j=%d, k=%d\n", i, j, k);
                                    sprintf(key, "%d,%d", i, k);
                                    sprintf(value, "B,%d,%d", j+row, matrix[j][k]);
                                    // hashmap_set_concat(map, new_entry->key, new_entry->value);
                                    // printf("(rank: %d) %s=%s\n", rank, key, value);
                                    hashmap_set_concat(map, key, value);
                                    // hashmap_set(map, &(struct entry){ .key=key, .value=value });
                                }
                            }
                        }
                    }
                    else { //Matrix A
                        for(int i=0; i<load; i++) {
                            for(int j=0; j<matrixSize; j++) {
                                for(int k=0; k<matrixSize; k++) {
                                    char* key = malloc(10); char* value = malloc(10);
                                    sprintf(key, "%d,%d", received_map_load.lineStart+i, k);
                                    sprintf(value, "A,%d,%d", j, matrix[i][j]);
                                    // printf("(rank: %d) %s=%s\n", rank, new_entry->key, new_entry->value);
                                    // printf("(rank: %d) %s=%s\n", rank, key, value);

                                    hashmap_set_concat(map, key, value);
                                    // hashmap_set(map, &(struct entry){ .key=key, .value=value });
                                }
                            }
                        }
                    }
                }
                int entryCount = (int) hashmap_count(map);
                //Create array of mapReceive with entryCount size
                struct entry *mapReceiveArray = malloc(entryCount * sizeof(struct entry));
                // printf("\n-- iterate over all entries (rank: %d) --\n", rank);
                size_t iter = 0;
                int iterator = 0;
                void *item;
                while (hashmap_iter(map, &iter, &item)) {
                    const struct entry *entryRow = item;
                    // printf("(%d): key: %s, value: %s\n", rank, entryRow->key, entryRow->value);
                    mapReceiveArray[iterator].key = entryRow->key;
                    mapReceiveArray[iterator].value = entryRow->value;
                    iterator++;
                }
                // /////////////////////////////////////////////////////////////
                // for(int i=0; i<entryCount; i++) {
                //     printf("(%d): key: %s, value: %s\n", rank, mapReceiveArray[i].key, mapReceiveArray[i].value);
                // }
                MPI_Send(&task, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD); // Send confirmation that the task has been completed.
                MPI_Send(&entryCount, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD); // Send the entryCount to master
                for(int i=0; i<entryCount; i++) {
                    //First send lengths of key and value
                    int keyLength = strlen(mapReceiveArray[i].key)+1;
                    int valueLength = strlen(mapReceiveArray[i].value)+1;
                    MPI_Send(&keyLength, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD);
                    MPI_Send(&valueLength, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD);
                    //Then send the key and value
                    // printf("Sent Entry %s, %s from rank: %d \n", mapReceiveArray[i].key, mapReceiveArray[i].value, rank);
                    MPI_Send(mapReceiveArray[i].key, keyLength, MPI_CHAR, MASTER_RANK, 0, MPI_COMM_WORLD);
                    MPI_Send(mapReceiveArray[i].value, valueLength, MPI_CHAR, MASTER_RANK, 0, MPI_COMM_WORLD);
                    //Recieved Entry: (0,1), (B,1,1) from rank: 4
                }

                
            }
            else if (task == REDUCE_TASK) {
                announceTask(rank, "received", proc_name, "reduce");
                /////////////////////////////////////////////////////////////
                int entriesPerReducer;
                MPI_Recv(&entriesPerReducer, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                // Clear hashmap
                hashmap_clear(map, true);
                //Receive hashmap entries
                for(int i=0; i<entriesPerReducer; i++) {
                    //First receive lengths of key and value
                    int keyLength;
                    int valueLength;
                    MPI_Recv(&keyLength, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    MPI_Recv(&valueLength, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    //Then receive the key and value
                    char* key = malloc(keyLength);
                    char* value = malloc(valueLength);
                    MPI_Recv(key, keyLength, MPI_CHAR, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    MPI_Recv(value, valueLength, MPI_CHAR, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    // printf("Received Entry: %s, %s from rank: %d \n", key, value, rank);
                    // hashmap_set(map, &(struct entry){ .key=key, .value=value });
                    hashmap_set_concat(map, key, value);
                }
                //Perform reduction
                   printf("\n-- reducer printing received hashmap (rank: %d) --\n", rank);
                   size_t iter = 0;
                   void *item;
                   //Initialize hashmap for reducer
                     struct hashmap *reducerMap = hashmap_new(sizeof(struct entry), 0, 0, 0, entry_hash, entry_compare, NULL, NULL);
                     while (hashmap_iter(map, &iter, &item)) {
                        const struct entry *user = item;
                        printf("key: %s, value: %s\n", user->key, user->value);
                        //Split value based on | delimiter
                        char* value = user->value;
                        char* saveptr;
                        char* token = strtok_r(value, HASHMAP_DELIM, &saveptr);
                        while (token != NULL) {
                            char* token2;
                            char* second_saveptr;
                            int count = 0;

                            char* jValue; char* pValue;
                            token2 = strtok_r(token, ",", &second_saveptr);
                            while(token2 != NULL) {
                                count++;
                                if (count == 2)
                                    jValue = (token2);
                                else if (count == 3)
                                    pValue = (token2);
                                
                                token2 = strtok_r(NULL, ",", &second_saveptr);
                            }
                            hashmap_set_concat(reducerMap, jValue, pValue);
                            token = strtok_r(NULL, HASHMAP_DELIM, &saveptr);
                        }
                    }
                    
                    printf("\n-- reducer printing reduced hashmap (rank: %d) --\n", rank);
                    iter = 0; double sum = 0;
                    while (hashmap_iter(reducerMap, &iter, &item)) {
                        const struct entry *user = item;
                        printf("key: %s, value: %s\n", user->key, user->value);
                        char* token = strtok(user->value, HASHMAP_DELIM);
                        double product = 1;
                        while (token != NULL) {
                            // printf("token: %s\n", token);
                            product *= atof(token);
                            token = strtok(NULL, HASHMAP_DELIM);
                        }
                        //Replace the current value with the product
                        char* productString = malloc(100);
                        sprintf(productString, "%f", product);
                        hashmap_set(reducerMap, &(struct entry){ .key=user->key, .value=productString});
                    }
                    printf("\n-- reducer printing final hashmap (rank: %d) --\n", rank);
                    iter = 0;
                    while (hashmap_iter(reducerMap, &iter, &item)) {
                        const struct entry *user = item;
                        printf("key: %s, value: %s\n", user->key, user->value);
                        sum += atof(user->value);
                    }
                    printf("Sum: %f\n", sum);
                    // We can now send back the sum to the master
                    ////////////////////////////////////////////
                MPI_Send(&task, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD);
                MPI_Send(&sum, 1, MPI_DOUBLE, MASTER_RANK, 0, MPI_COMM_WORLD);
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