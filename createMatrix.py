import random

n = 2 ** 12  # Set the size of the matrix to 5x5 (you can change this value as desired)
matrix_count = 2 #Create n random matrices

for i in range(matrix_count):
    # Create an n x n matrix filled with random integers between 0 and 99
    matrix = [[random.randint(0, 99) for i in range(n)] for j in range(n)]

    # Save the matrix to a file called "random_matrix_i.txt"
    with open("random_matrix_" + str(i+1) + ".txt", "w") as f:
        for row in matrix:
            for number in row:
                f.write(str(number) + " ")
            f.write("\n")
