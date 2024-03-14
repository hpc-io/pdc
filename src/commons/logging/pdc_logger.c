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

void
_log_message(int lf, PDC_LogLevel level, const char *format, va_list args)
{
    if (level > logLevel) {
        return;
    }

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

    char *log_format = (lf == 1) ? "[%s.%06ld] [%s] %s\n" : "[%s.%06ld] [%s] %s";

    char *message = (char *)calloc(MAX_LOG_MSG_LENGTH + 1, sizeof(char));
    vsnprintf(message, MAX_LOG_MSG_LENGTH, format, args);

    struct timeval tv;
    gettimeofday(&tv, NULL);
    struct tm *timeinfo = localtime(&tv.tv_sec);
    char *     timestr  = (char *)calloc(20, sizeof(char));
    strftime(timestr, 19, "%Y-%m-%d %H:%M:%S", timeinfo);

    // Rotate log file if it exceeds the maximum size, but this doesn't apply to stdout and stderr
    if (logFiles[level] != stdout && logFiles[level] != stderr) {
        struct stat st;
        stat(logFilenames[level], &st);
        if (st.st_size >= MAX_LOG_FILE_SIZE) {
            rotate_log_file(level);
        }
    }
    FILE *logFile = logFiles[level] ? logFiles[level] : stdout;
    fprintf(logFile, log_format, timestr, tv.tv_usec, prefix, message);
}

void
log_message_nlf(PDC_LogLevel level, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    _log_message(0, level, format, args);
    va_end(args);
}

void
log_message(PDC_LogLevel level, const char *format, ...)
{
    va_list args;
    va_start(args, format);
    _log_message(1, level, format, args);
    va_end(args);
}