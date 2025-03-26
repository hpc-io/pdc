#include "pdc_logger.h"
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>

void
setLogFile(PDC_LogLevel level, const char *fileName)
{

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
            logFiles[level]                                      = fopen(fileName, "a");
        }
    }
    else {
        logFiles[level] = stdout;
    }
}

void
setLogLevel(PDC_LogLevel level)
{
    logLevel = level;
}

void
rotate_log_file(PDC_LogLevel level)
{
    if (logFiles[level]) {
        if (logFiles[level] == stdout || logFiles[level] == stderr) {
            return; // for stdout and stderr, we don't rotate
        }
        fclose(logFiles[level]);
        logFiles[level] = NULL;
    }

    char       newFilename[MAX_LOG_FILE_NAME_LENGTH];
    char       timeStr[20];
    time_t     rawtime  = time(NULL);
    struct tm *timeinfo = localtime(&rawtime);

    strftime(timeStr, 20, "%Y%m%d%H:%M:%S", timeinfo);
    newFilename[strlen(newFilename) - 1] = '\0'; // Remove trailing newline

    snprintf(newFilename, MAX_LOG_FILE_NAME_LENGTH, "%s_%s", logFilenames[level], timeStr);
    rename(logFilenames[level], newFilename);
    logFiles[level] = fopen(logFilenames[level], "a");
}

static FILE *
get_cur_log_file(PDC_LogLevel level)
{
    // Rotate log file if it exceeds the maximum size, but this doesn't apply to stdout and stderr
    if (logFiles[level] != stdout && logFiles[level] != stderr) {
        struct stat st;
        stat(logFilenames[level], &st);
        if (st.st_size >= MAX_LOG_FILE_SIZE) {
            rotate_log_file(level);
        }
    }
    return logFiles[level] ? logFiles[level] : stdout;
}

void
_log_message(PDC_LogLevel level, const char *file, const char *func, int line_number, const char *format,
             va_list args, bool just_print)
{
    if (level > logLevel) {
        return;
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

        // Extract only the filename (stem) from the full path
        const char *filename = strrchr(file, '/');
        if (filename) {
            filename++;
        }
        else {
            filename = file;
        }

        // Properly format timestamp
        struct timeval tv;
        gettimeofday(&tv, NULL);
        struct tm timeinfo;
        localtime_r(&tv.tv_sec, &timeinfo);

        char timestr[30];
        strftime(timestr, sizeof(timestr), "%Y-%m-%d %H:%M:%S", &timeinfo);

        const char *log_format = "[%s.%06ld] [%s] [%s:%s:%d] %s";

        char message[MAX_LOG_MSG_LENGTH + 1];
        vsnprintf(message, MAX_LOG_MSG_LENGTH, format, args);

        fprintf(logFile, log_format, timestr, tv.tv_usec, prefix, filename, func, line_number, message);
    }
    else {
        const char *log_format = "%s";
        char        message[MAX_LOG_MSG_LENGTH + 1];
        vsnprintf(message, MAX_LOG_MSG_LENGTH, format, args);

        fprintf(logFile, log_format, message);
    }

    fflush(logFile);
}

void
log_message(bool just_print, PDC_LogLevel level, const char *file, const char *func, int line_number,
            const char *format, ...)
{

    va_list args;
    va_start(args, format);
    _log_message(level, file, func, line_number, format, args, just_print);
    va_end(args);
}