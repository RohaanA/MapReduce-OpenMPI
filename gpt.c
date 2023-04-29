#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

#define MATRIX_SIZE 4096
#define comm_sz 8
#define BLOCK_SIZE MATRIX_SIZE / comm_sz

double **read_matrix(char *filename, int n) {
    double **matrix = (double **) malloc(n * sizeof(double *));
    for (int i = 0; i < n; i++) {
        matrix[i] = (double *) malloc(n * sizeof(double));
    }

    FILE *fp;
    fp = fopen(filename, "r");

    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            fscanf(fp, "%lf", &matrix[i][j]);
        }
    }

    fclose(fp);
    return matrix;
}

void map(int matrix1[BLOCK_SIZE][MATRIX_SIZE], int matrix2[MATRIX_SIZE][BLOCK_SIZE], int result[BLOCK_SIZE][BLOCK_SIZE], int size) {
    int i, j, k;
    for (i = 0; i < BLOCK_SIZE; i++) {
        for (j = 0; j < BLOCK_SIZE; j++) {
            result[i][j] = 0;
            for (k = 0; k < size; k++) {
                result[i][j] += matrix1[i][k] * matrix2[k][j];
            }
        }
    }
}

void reduce(int rank, int result[BLOCK_SIZE][BLOCK_SIZE]) {
    int* recvcounts = malloc(sizeof(int) * comm_sz);
    int* displs = malloc(sizeof(int) * comm_sz);
    for (int i = 0; i < comm_sz; i++) {
        recvcounts[i] = BLOCK_SIZE * BLOCK_SIZE;
        displs[i] = i * BLOCK_SIZE * BLOCK_SIZE;
    }

    int* buffer = malloc(sizeof(int) * MATRIX_SIZE * MATRIX_SIZE);
    MPI_Gatherv(result, BLOCK_SIZE * BLOCK_SIZE, MPI_INT, buffer, recvcounts, displs, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        int output[MATRIX_SIZE][MATRIX_SIZE];
        for (int i = 0; i < MATRIX_SIZE; i++) {
            for (int j = 0; j < MATRIX_SIZE; j++) {
                output[i][j] = 0;
            }
        }

        for (int i = 0; i < comm_sz; i++) {
            int row_offset = (i / (MATRIX_SIZE / BLOCK_SIZE)) * BLOCK_SIZE;
            int col_offset = (i % (MATRIX_SIZE / BLOCK_SIZE)) * BLOCK_SIZE;

            for (int j = 0; j < BLOCK_SIZE; j++) {
                for (int k = 0; k < BLOCK_SIZE; k++) {
                    output[row_offset + j][col_offset + k] = buffer[i * BLOCK_SIZE * BLOCK_SIZE + j * BLOCK_SIZE + k];
                }
            }
        }

        // Output result
        printf("Result:\n");
        for (int i = 0; i < MATRIX_SIZE; i++) {
            for (int j = 0; j < MATRIX_SIZE; j++) {
                printf("%d ", output[i][j]);
            }
            printf("\n");
        }
    }
}

int main(int argc, char** argv) {
    int rank, num_processes;

    // Initialize MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &num_processes);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // Read the matrices from file
    int* matrix1 = NULL;
    int* matrix2 = NULL;
    int* result = NULL;
    int n;

    if (rank == 0) {
        matrix1 = read_matrix("random_matrix_1.txt", &n);
        matrix2 = read_matrix("random_matrix_2.txt", &n);
        result = (int*) malloc(n * n * sizeof(int));
    }

    // Broadcast the matrix size to all processes
    MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD);

    // Scatter the matrix1 to all processes
    int* sub_matrix1 = (int*) malloc((n * n / num_processes) * sizeof(int));
    MPI_Scatter(matrix1, n * n / num_processes, MPI_INT, sub_matrix1, n * n / num_processes, MPI_INT, 0, MPI_COMM_WORLD);

    // Broadcast matrix2 to all processes
    int* sub_matrix2 = (int*) malloc(n * n * sizeof(int));
    MPI_Bcast(matrix2, n * n, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Scatter(matrix2, n * n / num_processes, MPI_INT, sub_matrix2, n * n / num_processes, MPI_INT, 0, MPI_COMM_WORLD);

    // Perform map and reduce
    int* sub_result = map(sub_matrix1, sub_matrix2, n / num_processes, n);
    free(sub_matrix1);
    free(sub_matrix2);

    // Gather the sub-results from all processes
    MPI_Gather(sub_result, n * n / num_processes, MPI_INT, result, n * n / num_processes, MPI_INT, 0, MPI_COMM_WORLD);
    free(sub_result);

    // Print the result
    if (rank == 0) {
        print_matrix(result, n);
        free(result);
        free(matrix1);
        free(matrix2);
    }

    // Finalize MPI
    MPI_Finalize();
    return 0;
}
