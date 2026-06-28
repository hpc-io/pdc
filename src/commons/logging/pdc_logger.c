#include "pdc_logger.h"
#include "pdc_timing.h"
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>

void
setLogFile(PDC_LogLevel level, const char *fileName)
{
    FUNC_ENTER(NULL);

    if (logFiles[level] && logFiles[level] != stdout && logFiles[level] != stderr) {
        fclose(logFiles[level]);
    }
    if (fileName) {
        if (strcmp(fileName, "stderr") == 0) {
            logFiles[level] = stderr;
        }
        else if (strcmp(fileName, "stdout") == 0) {
            logFiles[level] = stdout;
        }
        else {
            strncpy(logFilenames[level], fileName, sizeof(logFilenames[level]) - 1);
            logFilenames[level][sizeof(logFilenames[level]) - 1] = '\0';
            int fd = open(fileName, O_WRONLY | O_CREAT | O_APPEND, 0600);
            if (fd < 0) {
                logFiles[level] = NULL;
            }
            else {
                logFiles[level] = fdopen(fd, "a");
                if (logFiles[level] == NULL)
                    close(fd);
            }
        }
    }
    else {
        logFiles[level] = stdout;
    }

    FUNC_LEAVE_VOID();
}

void
setLogLevel(PDC_LogLevel level)
{
    FUNC_ENTER(NULL);

    logLevel = level;

    FUNC_LEAVE_VOID();
}

void
rotate_log_file(PDC_LogLevel level)
{
    FUNC_ENTER(NULL);

    if (logFiles[level]) {
        if (logFiles[level] == stdout || logFiles[level] == stderr) {
            FUNC_LEAVE_VOID(); // for stdout and stderr, we don't rotate
        }
        fclose(logFiles[level]);
        logFiles[level] = NULL;
    }

    char      newFilename[MAX_LOG_FILE_NAME_LENGTH];
    char      timeStr[20];
    time_t    rawtime = time(NULL);
    struct tm timeinfo;

    // Use localtime_r for thread safety
    localtime_r(&rawtime, &timeinfo);

    strftime(timeStr, sizeof(timeStr), "%Y%m%d%H:%M:%S", &timeinfo);

    snprintf(newFilename, MAX_LOG_FILE_NAME_LENGTH, "%s_%s", logFilenames[level], timeStr);
    if (rename(logFilenames[level], newFilename) != 0) {
        logFiles[level] = NULL;
        FUNC_LEAVE_VOID();
    }
    int fd = open(logFilenames[level], O_WRONLY | O_CREAT | O_APPEND, 0600);
    if (fd < 0) {
        logFiles[level] = NULL;
        FUNC_LEAVE_VOID();
    }
    logFiles[level] = fdopen(fd, "a");
    if (logFiles[level] == NULL)
        close(fd);

    FUNC_LEAVE_VOID();
}

static FILE *
get_cur_log_file(PDC_LogLevel level)
{
    FUNC_ENTER(NULL);

    // Rotate log file if it exceeds the maximum size, but this doesn't apply to stdout and stderr
    if (logFiles[level] != stdout && logFiles[level] != stderr) {
        struct stat st;
        stat(logFilenames[level], &st);
        if (st.st_size >= MAX_LOG_FILE_SIZE) {
            rotate_log_file(level);
        }
    }

    FUNC_LEAVE(logFiles[level] ? logFiles[level] : stdout);
}

static void
_log_message(bool is_server, PDC_LogLevel level, const char *file, const char *func, int line_number,
             const char *format, va_list args, bool just_print)
{
    FUNC_ENTER(NULL);

    if (level > logLevel) {
        FUNC_LEAVE_VOID();
    }

    FILE *logFile = get_cur_log_file(level);
    if (!just_print) {
        char prefix[16];
        switch (level) {
            case LOG_LEVEL_ERROR:
                strcpy(prefix, "ERROR");
                break;
            case LOG_LEVEL_WARNING:
                strcpy(prefix, "WARNING");
                break;
            case LOG_LEVEL_INFO:
                strcpy(prefix, "INFO");
                break;
            case LOG_LEVEL_DEBUG:
                strcpy(prefix, "DEBUG");
                break;
        }

        // Extract only the filename from the full path
        const char *filename = strrchr(file, '/');
        filename             = filename ? filename + 1 : file;

        // Format timestamp
        struct timeval tv;
        gettimeofday(&tv, NULL);
        struct tm timeinfo;
        localtime_r(&tv.tv_sec, &timeinfo);

        char timestr[30];
        strftime(timestr, sizeof(timestr), "%H:%M:%S", &timeinfo);

        char message[MAX_LOG_MSG_LENGTH + 1];
        vsnprintf(message, MAX_LOG_MSG_LENGTH, format, args);

#ifdef ENABLE_MPI
        static int my_rank = -1;
        if (my_rank == -1) {
            int mpi_initialized = 0;
            MPI_Initialized(&mpi_initialized);
            if (mpi_initialized)
                my_rank = PDC_get_rank();
        }
#endif

        // Print differently based on log level
        if (level == LOG_LEVEL_ERROR || level == LOG_LEVEL_DEBUG) {
#ifdef ENABLE_MPI
            if (is_server)
                fprintf(logFile, "[%s.%06ld] [%s] [%s:%d] PDC_SERVER[%d]: %s", timestr, tv.tv_usec, prefix,
                        filename, line_number, my_rank, message);
            else
                fprintf(logFile, "[%s.%06ld] [%s] [%s:%d] PDC_CLIENT[%d]: %s", timestr, tv.tv_usec, prefix,
                        filename, line_number, my_rank, message);
#else
            if (is_server)
                fprintf(logFile, "[%s.%06ld] [%s] [%s:%d] PDC_SERVER: %s", timestr, tv.tv_usec, prefix,
                        filename, line_number, message);
            else
                fprintf(logFile, "[%s.%06ld] [%s] [%s:%d] PC_CLIENT: %s", timestr, tv.tv_usec, prefix,
                        filename, line_number, message);
#endif
        }
        else {
#ifdef ENABLE_MPI
            if (is_server)
                fprintf(logFile, "[%s] PDC_SERVER[%d]: %s", prefix, my_rank, message);
            else
                fprintf(logFile, "[%s] PDC_CLIENT[%d]: %s", prefix, my_rank, message);
#else
            if (is_server)
                fprintf(logFile, "[%s] PDC_SERVER: %s", prefix, message);
            else
                fprintf(logFile, "[%s] PC_CLIENT: %s", prefix, message);
#endif
        }
    }
    else {
        const char *log_format = "%s";
        char        message[MAX_LOG_MSG_LENGTH + 1];
        vsnprintf(message, MAX_LOG_MSG_LENGTH, format, args);
        fprintf(logFile, log_format, message);
    }

    fflush(logFile);

    FUNC_LEAVE_VOID();
}

void
log_message(bool is_server, bool just_print, PDC_LogLevel level, const char *file, const char *func,
            int line_number, const char *format, ...)
{
    FUNC_ENTER(NULL);

    va_list args;
    va_start(args, format);
    _log_message(is_server, level, file, func, line_number, format, args, just_print);
    va_end(args);

    FUNC_LEAVE_VOID();
}
