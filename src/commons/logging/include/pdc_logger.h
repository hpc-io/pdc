#ifndef PDC_LOGGER_H
#define PDC_LOGGER_H

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#define MAX_LOG_MSG_LENGTH       1024
#define MAX_LOG_FILE_SIZE        (10 * 1024 * 1024) // 10 MB
#define MAX_LOG_FILE_NAME_LENGTH 256

typedef enum { LOG_LEVEL_ERROR, LOG_LEVEL_WARNING, LOG_LEVEL_INFO, LOG_LEVEL_DEBUG } PDC_LogLevel;

static FILE *       logFiles[4] = {NULL}; // Log files for each log level
static char         logFilenames[4][MAX_LOG_FILE_NAME_LENGTH];
static PDC_LogLevel logLevel = LOG_LEVEL_DEBUG;

void setLogFile(PDC_LogLevel level, const char *fileName);
void setLogLevel(PDC_LogLevel level);

/**
 * just_print is equivalent to calling printf("%s", args) meaning no extra information
 * such as the file and line number will be printed
 */
void log_message(bool is_server, bool just_print, PDC_LogLevel level, const char *file, const char *func,
                 int line_number, const char *format, ...);

#ifdef IS_PDC_SERVER
#define LOG_ERROR(format, ...)                                                                               \
    log_message(true, false, LOG_LEVEL_ERROR, __FILE__, __func__, __LINE__, format, ##__VA_ARGS__)
#define LOG_WARNING(format, ...)                                                                             \
    log_message(true, false, LOG_LEVEL_WARNING, __FILE__, __func__, __LINE__, format, ##__VA_ARGS__)
#define LOG_INFO(format, ...)                                                                                \
    log_message(true, false, LOG_LEVEL_INFO, __FILE__, __func__, __LINE__, format, ##__VA_ARGS__)
#define LOG_DEBUG(format, ...)                                                                               \
    log_message(true, false, LOG_LEVEL_DEBUG, __FILE__, __func__, __LINE__, format, ##__VA_ARGS__)
#define LOG_JUST_PRINT(format, ...)                                                                          \
    log_message(true, true, LOG_LEVEL_INFO, __FILE__, __func__, __LINE__, format, ##__VA_ARGS__)
#else
#define LOG_ERROR(format, ...)                                                                               \
    log_message(false, false, LOG_LEVEL_ERROR, __FILE__, __func__, __LINE__, format, ##__VA_ARGS__)
#define LOG_WARNING(format, ...)                                                                             \
    log_message(false, false, LOG_LEVEL_WARNING, __FILE__, __func__, __LINE__, format, ##__VA_ARGS__)
#define LOG_INFO(format, ...)                                                                                \
    log_message(false, false, LOG_LEVEL_INFO, __FILE__, __func__, __LINE__, format, ##__VA_ARGS__)
#define LOG_DEBUG(format, ...)                                                                               \
    log_message(false, false, LOG_LEVEL_DEBUG, __FILE__, __func__, __LINE__, format, ##__VA_ARGS__)
#define LOG_JUST_PRINT(format, ...)                                                                          \
    log_message(false, true, LOG_LEVEL_INFO, __FILE__, __func__, __LINE__, format, ##__VA_ARGS__)
#endif

#endif // PDC_LOGGER_H
