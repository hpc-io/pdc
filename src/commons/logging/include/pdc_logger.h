#ifndef PDC_LOGGER_H
#define PDC_LOGGER_H

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LOG_MSG_LENGTH       1024
#define MAX_LOG_FILE_SIZE        (10 * 1024 * 1024) // 10 MB
#define MAX_LOG_FILE_NAME_LENGTH 256

typedef enum { LOG_LEVEL_ERROR, LOG_LEVEL_WARNING, LOG_LEVEL_INFO, LOG_LEVEL_DEBUG } PDC_LogLevel;

static FILE *       logFiles[4] = {NULL}; // Log files for each log level
static char         logFilenames[4][MAX_LOG_FILE_NAME_LENGTH];
static PDC_LogLevel logLevel = LOG_LEVEL_INFO;

void setLogFile(PDC_LogLevel level, const char *fileName);

void setLogLevel(PDC_LogLevel level);

void log_message(PDC_LogLevel level, const char *format, ...);

void log_message_nlf(PDC_LogLevel level, const char *format, ...);

#define LOG_ERROR(format, ...)   log_message(LOG_LEVEL_ERROR, format, ##__VA_ARGS__)
#define LOG_WARNING(format, ...) log_message(LOG_LEVEL_WARNING, format, ##__VA_ARGS__)
#define LOG_INFO(format, ...)    log_message(LOG_LEVEL_INFO, format, ##__VA_ARGS__)
#define LOG_DEBUG(format, ...)   log_message(LOG_LEVEL_DEBUG, format, ##__VA_ARGS__)

#define NLF_LOG_ERROR(format, ...)   log_message_nlf(LOG_LEVEL_ERROR, format, ##__VA_ARGS__)
#define NLF_LOG_WARNING(format, ...) log_message_nlf(LOG_LEVEL_WARNING, format, ##__VA_ARGS__)
#define NLF_LOG_INFO(format, ...)    log_message_nlf(LOG_LEVEL_INFO, format, ##__VA_ARGS__)
#define NLF_LOG_DEBUG(format, ...)   log_message_nlf(LOG_LEVEL_DEBUG, format, ##__VA_ARGS__)

#endif // PDC_LOGGER_H