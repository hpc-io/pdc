/* PDC's loader hooks must not bind to previously loaded foreign symbols. */
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
int
main(void)
{
    void *foreign = dlopen(PDC_TEST_FOREIGN_LIBRARY, RTLD_NOW | RTLD_GLOBAL);
    if (!foreign) {
        fprintf(stderr, "foreign library: %s\n", dlerror());
        return EXIT_FAILURE;
    }
    int (*init_calls)(void) = (int (*)(void))dlsym(foreign, "foreign_profile_init_calls");
    int (*fini_calls)(void) = (int (*)(void))dlsym(foreign, "foreign_profile_fini_calls");
    if (!init_calls || !fini_calls)
        return EXIT_FAILURE;
    void *pdc = dlopen(PDC_TEST_LIBRARY, RTLD_NOW | RTLD_GLOBAL);
    if (!pdc) {
        fprintf(stderr, "PDC library: %s\n", dlerror());
        return EXIT_FAILURE;
    }
    if (init_calls() != 0) {
        fprintf(stderr, "PDC invoked a foreign profiler initializer\n");
        return EXIT_FAILURE;
    }
    if (dlclose(pdc) != 0 || fini_calls() != 0) {
        fprintf(stderr, "PDC invoked a foreign profiler finalizer\n");
        return EXIT_FAILURE;
    }
    dlclose(foreign);
    puts("PDC loader hooks are isolated from foreign profiler symbols");
    return EXIT_SUCCESS;
}
