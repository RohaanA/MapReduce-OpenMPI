import random

n = [2, 4, 8, 16, 32]
#Create 2 nxn matrix and their product for n sizes
for i in n:
    #Create matrix A
    matrixA = []
    for j in range(i):
        row = []
        for k in range(i):
            row.append(random.randint(0, 10))
        matrixA.append(row)
    #Create matrix B
    matrixB = []
    for j in range(i):
        row = []
        for k in range(i):
            row.append(random.randint(0, 10))
        matrixB.append(row)
    #Create matrix C
    matrixC = []
    for j in range(i):
        row = []
        for k in range(i):
            row.append(0)
        matrixC.append(row)
    #Matrix multiplication
    for j in range(i):
        for k in range(i):
            for l in range(i):
                matrixC[j][k] += matrixA[j][l] * matrixB[l][k]
    #Write matrix A
    file = open("matrixA" + str(i) + ".txt", "w")
    for j in range(i):
        for k in range(i):
            file.write(str(matrixA[j][k]) + " ")
        file.write("\n")
    file.close()
    #Write matrix B
    file = open("matrixB" + str(i) + ".txt", "w")
    for j in range(i):
        for k in range(i):
            file.write(str(matrixB[j][k]) + " ")
        file.write("\n")
    file.close()
    #Write matrix C
    file = open("matrixC" + str(i) + ".txt", "w")
    for j in range(i):
        for k in range(i):
            file.write(str(matrixC[j][k]) + " ")
        file.write("\n")
    file.close()
