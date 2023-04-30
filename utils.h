/* 

Utility Header File by Rohaan 
Contains standard functions (with color codes)
Designed Specifically for PDC Project. 

*/
#ifndef UTILS_H
#define UTILS_H

#define MASTER_RANK 0
// COLOR CODES
#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_BRIGHT_RED     "\x1b[91m"
#define ANSI_COLOR_RESET   "\x1b[0m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_BRIGHT_GREEN   "\x1b[92m"
#define ANSI_COLOR_YELLOW  "\x1b[33m"
#define ANSI_COLOR_BRIGHT_YELLOW  "\x1b[93m"
#define ANSI_COLOR_BLACK   "\x1b[30m"
#define ANSI_COLOR_WHITE   "\x1b[37m"
#define ANSI_COLOR_BRIGHT_WHITE   "\x1b[97m"
#define ANSI_COLOR_BLUE    "\x1b[34m"
#define ANSI_COLOR_BRIGHT_BLUE    "\x1b[94m"
#define ANSI_COLOR_MAGENTA "\x1b[35m"
#define ANSI_COLOR_BRIGHT_MAGENTA "\x1b[95m"
#define ANSI_COLOR_CYAN    "\x1b[36m"
#define ANSI_COLOR_BRIGHT_CYAN    "\x1b[96m"
#define ANSI_COLOR_GRAY           "\x1b[90m"

// MESSAGE COLORS
#define ANNOUNCE_COLOR ANSI_COLOR_CYAN
#define ERROR_COLOR ANSI_COLOR_RED
#define SUCCESS_COLOR ANSI_COLOR_GREEN
#define LOGGING_COLOR_MASTER ANSI_COLOR_YELLOW
#define LOGGING_COLOR_SLAVE ANSI_COLOR_BRIGHT_YELLOW
#define INFO_COLOR ANSI_COLOR_BLUE
#define DEBUG_COLOR ANSI_COLOR_MAGENTA
#define DEBUG_COLOR_TEXT ANSI_COLOR_BRIGHT_MAGENTA


/* 
 * -=-=-=- Utility functions -=-=-=-
 * announceTask: announces an assigned/received task to the console
 * announceCompletion: announces completion of a task to the console
 * logger: logs a message to the console 
 */
void announceTask(int rank, char *taskType, char *machineName, char* taskName) {
    if (taskType == "assigned")
        printf(ANNOUNCE_COLOR "[ANNOUNCEMENT]" ANSI_COLOR_RESET " Task %s assigned to process %d \n", taskName, rank);
    else if (taskType == "received")
        printf(ANNOUNCE_COLOR "[ANNOUNCEMENT]" ANSI_COLOR_RESET " Process %d received task %s on %s \n", rank, taskName, machineName);
}
void announceCompletion(int rank, char *taskType) {
    printf(SUCCESS_COLOR "[SUCCESS]: " ANSI_COLOR_RESET "Process %d completed task %s \n", rank, taskType);
}
void logger(int rank, char *message) {
    if (rank == MASTER_RANK)
        printf(LOGGING_COLOR_MASTER "[MASTER]: " ANSI_COLOR_GRAY "%s \n" ANSI_COLOR_RESET, message);
    else printf(LOGGING_COLOR_SLAVE "[SLAVE %d]:" ANSI_COLOR_BRIGHT_CYAN " %s \n" ANSI_COLOR_RESET, rank, message);
}
void raiseError(int rank, char *message) {    
    if (rank == -1)
        printf(ERROR_COLOR "[ERROR]: " ANSI_COLOR_RESET "%s \n", message);
    if (rank == MASTER_RANK)
        printf(LOGGING_COLOR_MASTER "[MASTER]: " ERROR_COLOR "[ERROR]: " ANSI_COLOR_RESET "%s \n", message);
    else printf(LOGGING_COLOR_SLAVE "[SLAVE]: " ERROR_COLOR "[ERROR]: " ANSI_COLOR_RESET "%s \n", message);
}
void debug_logger(bool debugMode, int rank, char* message) {
    if (debugMode){
        if (rank == -1)
            printf(DEBUG_COLOR "[DEBUG] -- " DEBUG_COLOR_TEXT "%s \n" ANSI_COLOR_RESET, message);
        else if (rank == MASTER_RANK)
            printf(DEBUG_COLOR "[DEBUG] -- " LOGGING_COLOR_MASTER "[MASTER] " DEBUG_COLOR_TEXT "%s \n" ANSI_COLOR_RESET, message);
        else printf(DEBUG_COLOR "[DEBUG] -- " LOGGING_COLOR_SLAVE "[SLAVE %d] " DEBUG_COLOR_TEXT "%s \n" ANSI_COLOR_RESET, rank, message);
    }
}
void raiseWarning(int rank, char *message) {
    if (rank == -1)
        printf(LOGGING_COLOR_MASTER "[WARNING]: " ANSI_COLOR_RESET "%s \n", message);
    if (rank == MASTER_RANK)
        printf(LOGGING_COLOR_MASTER "[MASTER]: " ANSI_COLOR_YELLOW "[WARNING]: " ANSI_COLOR_RESET "%s \n", message);
    else printf(LOGGING_COLOR_SLAVE "[SLAVE]: " ANSI_COLOR_YELLOW "[WARNING]: " ANSI_COLOR_RESET "%s \n", message);
}

#endif