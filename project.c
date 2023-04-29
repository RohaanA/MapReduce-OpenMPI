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
#include "utils.h"
#include "hashmap.h"

#define MASTER_RANK 0

//TASK DICTIONARY
#define MAP_TASK 1 
#define SHUFFLE_TASK 2
#define REDUCE_TASK 3
#define EXIT_CALL 4 //Ask slaves to exit.

// MATRICES
#define matrixFile_1 "random_matrix_1.txt"
#define matrixFile_2 "random_matrix_2.txt"

void matrix_multiply_map(int rank, int num_processes, int n, double *matrixA, double *matrixB, double *map_output)
{
    int rows_per_process = n / num_processes;
    int start_row = rank * rows_per_process;
    int end_row = start_row + rows_per_process;

    for (int i = start_row; i < end_row; i++)
    {
        for (int j = 0; j < n; j++)
        {
            double sum = 0;
            for (int k = 0; k < n; k++)
            {
                sum += matrixA[i * n + k] * matrixB[k * n + j];
            }
            map_output[i * n + j] = sum;
        }
    }
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
    //  struct hashmap *map = hashmap_new(sizeof(struct user), 0, 0, 0, 
    //                                  user_hash, user_compare, NULL, NULL);

    if (rank == MASTER_RANK) {
        char buff [50];

        sprintf(buff, "Master with process id %d, running on %s", rank, proc_name);
        logger(rank, buff);
        //Based on size, utilize all processes to map
        int mapperCount = size - 1;
        //Assign mappers to map
        logger(rank, "=========================================================================");
        logger(rank, "                           Starting Map Phase                            ");
        logger(rank, "=========================================================================");
        for(int mapperRank=0; mapperRank<size; mapperRank++) {
            if (mapperRank == MASTER_RANK) continue;

            int task = EXIT_CALL;
            announceTask(mapperRank, "assigned", proc_name, "map");
            MPI_Send(&task, 1, MPI_INT, mapperRank, 0, MPI_COMM_WORLD);
            // In the Map phase, each mapper is assigned a block of rows from matrix A and a block of columns from matrix B. 
            // The mapper multiplies the two blocks and produces a block of the resulting matrix C.
            // Now wait till all mappers complete their task
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
                announceTask(rank, "received", proc_name, "map");
                /////////////////////////////////////////////////////////////
                // Do Mapping
                /////////////////////////////////////////////////////////////
                MPI_Send(&task, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD);
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
    MPI_Finalize();
}