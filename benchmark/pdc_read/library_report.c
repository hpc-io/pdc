#define _GNU_SOURCE
#include <link.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static int
report(struct dl_phdr_info *info, size_t size, void *arg)
{
    (void)size;
    (void)arg;
    const char *name = strrchr(info->dlpi_name, '/');
    name             = name ? name + 1 : info->dlpi_name;
    if (strncmp(name, "libpdc", 6) == 0 || strncmp(name, "libmercury", 10) == 0 ||
        strncmp(name, "libna.so", 8) == 0)
        fprintf(stderr, "LIBMAP %s\n", info->dlpi_name);
    return 0;
}
__attribute__((constructor)) static void
start(void)
{
    const char *enabled = getenv("PDC_REPORT_LIBRARIES");
    const char *rank    = getenv("SLURM_PROCID");
    if (enabled && (!rank || strcmp(rank, "0") == 0))
        dl_iterate_phdr(report, NULL);
}
