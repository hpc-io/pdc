#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

static uint32_t pdc_hash_djb2(const char *pc)
{
    uint32_t hash = 5381, c;
    while (c = *pc++)
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    if (hash < 0)
        hash *= -1;

    return hash;
}

int main(int argc, char *argv[])
{
    char *in_fname;
    char out_fname[128];
    int n_server;
    uint32_t sid;

    in_fname = argv[1];
    sprintf(out_fname, "%s.sid", in_fname); 

    FILE *fp_in = fopen(in_fname, "r");
    /* FILE *fp_out = fopen(argv[1], "w"); */
    n_server = atoi(argv[2]);

    char dset_name[128];
    while (EOF != fscanf(fp_in, "%s", dset_name)) {
        sid = pdc_hash_djb2(dset_name) % n_server;
        printf("%d\n", (int)sid);
    }


    fclose(fp_in);

    
    return 0;
}
