#include <stdio.h>
#include <stdlib.h>

#include "pdc.h"

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#define PDC_UNDEFINED_INT           -123456
#define PDC_LABEL_INIT_ALLOC_SIZE   32

#define ADDR_MAX 128
#define PDC_MAX(x, y) (((x) > (y)) ? (x) : (y))
#define PDC_MIN(x, y) (((x) < (y)) ? (x) : (y))

typedef struct PDC_label_t {
    void     *attached_addr;

    int       from;
    int       to;
    int       n_from_arr;
    int       n_to_arr;
    int       n_from_arr_alloc;
    int       n_to_arr_alloc;
    int      *from_arr;               // If more than 1 from, values will be stored here
    int      *to_arr;                 // If more than 1 to,   values will be stored here

    /* int      *from_ptr; */
    /* int      *to_ptr; */
    /* int       n_from_ptr_arr; */
    /* int       n_to_ptr_arr; */
    /* int       n_from_ptr_arr_alloc; */
    /* int       n_to_ptr_arr_alloc; */
    /* void    **from_ptr_arr;           // If more than 1 from_ptr, values will be stored here */
    /* void    **to_ptr_arr;             // If more than 1 from_ptr, values will be stored here */
} PDC_label_t;

typedef struct PDC_IO_segment_t {
    char     filepath[ADDR_MAX];
    uint64_t start;
    uint64_t size;
    PDC_label_t *label;
} PDC_IO_segment_t;

void PDC_label_print(PDC_label_t* a)
{
    int i;

    if (a == NULL) {
        printf("==PDC_ERROR: %s - NULL input!\n", __func__);
        return;
    }

    if (a->n_from_arr > 0) {
        printf("from array:\n");
        for (i = 0; i < a->n_from_arr; i++) {
            printf("%d ,", a->from_arr[i]);
        }
        printf("\n");
    }
    else if (a->from != PDC_UNDEFINED_INT) {
        printf("from elem: %d\n", a->from);
    }

    if (a->n_to_arr > 0) {
        printf("to array:\n");
        for (i = 0; i < a->n_to_arr; i++) {
            printf("%d ,", a->to_arr[i]);
        }
        printf("\n");
    }
    else if (a->to != PDC_UNDEFINED_INT) {
        printf("to elem: %d\n", a->to);
    }
}

perr_t PDC_label_init(PDC_label_t* a)
{
    if (a == NULL) {
        printf("==PDC_ERROR: %s - NULL input!\n", __func__);
        return FAIL;
    }

    memset(a, 0, sizeof(PDC_label_t));
    a->from = PDC_UNDEFINED_INT;
    a->to   = PDC_UNDEFINED_INT;

    return SUCCEED;
}

PDC_label_t *PDC_label_create()
{
    PDC_label_t *a = (PDC_label_t*)malloc(sizeof(PDC_label_t));
    PDC_label_init(a);

    return a;
}

void PDC_label_destroy(PDC_label_t *a)
{
    if (a->n_from_arr_alloc > 0) 
        free(a->from_arr);
    
    if (a->n_to_arr_alloc > 0) 
        free(a->to_arr);

    /* if (a->n_from_ptr_arr_alloc > 0) */ 
    /*     free(a->from_ptr_arr); */
    
    /* if (a->n_to_ptr_arr_alloc > 0) */ 
    /*     free(a->to_ptr_arr); */

    free(a);
}

perr_t PDC_label_attach(void *obj, PDC_label_t **label_ptr, PDC_label_t* a)
{
    if (label_ptr == NULL || a == NULL) {
        printf("==PDC_ERROR: %s - NULL input!\n", __func__);
        return FAIL;
    }

    *label_ptr = a;
    a->attached_addr = obj;

    return SUCCEED;
}

perr_t PDC_label_add_to(PDC_label_t *label_ptr, int n, int *data)
{
    perr_t ret_value = SUCCEED;
    if (label_ptr == NULL || n == 0 || data == NULL) {
        printf("==PDC_ERROR: %s - invalid input!\n", __func__);
        ret_value = FAIL;
        goto done;
    }

    if (n == 1) {
        if (label_ptr->to == PDC_UNDEFINED_INT) {
            label_ptr->to = *data;
            ret_value = SUCCEED;
            goto done;
        }
        else {
            // There is already data in to, need to allocate space dynamically
            label_ptr->n_to_arr_alloc = PDC_LABEL_INIT_ALLOC_SIZE;
            label_ptr->to_arr         = (int*)calloc(PDC_LABEL_INIT_ALLOC_SIZE, sizeof(int));
            label_ptr->to_arr[0]      = label_ptr->to;
            label_ptr->to_arr[1]      = *data;
            label_ptr->n_to_arr = 2;
        }
    }
    else {
        // If there is no existing data in to, then to_arr should also be NULL
        if (label_ptr->to == PDC_UNDEFINED_INT) {
            // Double check
            if (label_ptr->n_to_arr_alloc > 0 || label_ptr->n_to_arr != 0) {
                printf("==PDC_ERROR: %s - space allocation not expected!\n", __func__);
                ret_value = FAIL;
                goto done;
            }
            // Fill the to with a valid value
            label_ptr->to = data[0];

            label_ptr->n_to_arr_alloc = PDC_LABEL_INIT_ALLOC_SIZE > n ? PDC_LABEL_INIT_ALLOC_SIZE : n;
            label_ptr->to_arr         = (int*)calloc(label_ptr->n_to_arr_alloc, sizeof(int));
            memcpy(label_ptr->to_arr, data, n*sizeof(int));
            label_ptr->n_to_arr = n;

            goto done;
        }
        // If there is existing data in to, but no allocated space
        else if (label_ptr->n_to_arr_alloc == 0) {
            label_ptr->n_to_arr_alloc = PDC_LABEL_INIT_ALLOC_SIZE > n+1 ? PDC_LABEL_INIT_ALLOC_SIZE : n+1;
            label_ptr->to_arr         = (int*)calloc(label_ptr->n_to_arr_alloc, sizeof(int));

            label_ptr->to_arr[0] = label_ptr->to;
            memcpy(&label_ptr->to_arr[1], data, n*sizeof(int));
            label_ptr->n_to_arr = n+1;

            goto done;
        }
        // There is already data dynamically allocated
        else {
            // Allocate new space if old + add exceeds already allocated space
            if (label_ptr->n_to_arr_alloc < label_ptr->n_to_arr + n) {
                label_ptr->n_to_arr_alloc = PDC_MAX(2*label_ptr->n_to_arr_alloc, label_ptr->n_to_arr+n); 
                label_ptr->to_arr = (int*)realloc(label_ptr->to_arr, label_ptr->n_to_arr_alloc*sizeof(int));
            }

            memcpy(&label_ptr->to_arr[label_ptr->n_to_arr], data, n*sizeof(int));
            label_ptr->n_to_arr += n;

            goto done;
        }

    }

done:
    return ret_value;
}


perr_t PDC_label_add_from(PDC_label_t *label_ptr, int n, int *data)
{
    perr_t ret_value = SUCCEED;
    if (label_ptr == NULL || n == 0 || data == NULL) {
        printf("==PDC_ERROR: %s - invalid input!\n", __func__);
        ret_value = FAIL;
        goto done;
    }

    if (n == 1) {
        if (label_ptr->from == PDC_UNDEFINED_INT) {
            label_ptr->from = *data;
            ret_value = SUCCEED;
            goto done;
        }
        else {
            // There is already data in from, need to allocate space dynamically
            label_ptr->n_from_arr_alloc = PDC_LABEL_INIT_ALLOC_SIZE;
            label_ptr->from_arr         = (int*)calloc(PDC_LABEL_INIT_ALLOC_SIZE, sizeof(int));
            label_ptr->from_arr[0]      = label_ptr->from;
            label_ptr->from_arr[1]      = *data;
            label_ptr->n_from_arr = 2;
        }
    }
    else {
        // If there is no existing data in from, then from_arr should also be NULL
        if (label_ptr->from == PDC_UNDEFINED_INT) {
            // Double check
            if (label_ptr->n_from_arr_alloc > 0 || label_ptr->n_from_arr != 0) {
                printf("==PDC_ERROR: %s - space allocation not expected!\n", __func__);
                ret_value = FAIL;
                goto done;
            }
            // Fill the from with a valid value
            label_ptr->from = data[0];

            label_ptr->n_from_arr_alloc = PDC_LABEL_INIT_ALLOC_SIZE > n ? PDC_LABEL_INIT_ALLOC_SIZE : n;
            label_ptr->from_arr         = (int*)calloc(label_ptr->n_from_arr_alloc, sizeof(int));
            memcpy(label_ptr->from_arr, data, n*sizeof(int));
            label_ptr->n_from_arr = n;

            goto done;
        }
        // If there is existing data in from, but no allocated space
        else if (label_ptr->n_from_arr_alloc == 0) {
            label_ptr->n_from_arr_alloc = PDC_LABEL_INIT_ALLOC_SIZE > n+1 ? PDC_LABEL_INIT_ALLOC_SIZE : n+1;
            label_ptr->from_arr         = (int*)calloc(label_ptr->n_from_arr_alloc, sizeof(int));

            label_ptr->from_arr[0] = label_ptr->from;
            memcpy(&label_ptr->from_arr[1], data, n*sizeof(int));
            label_ptr->n_from_arr = n+1;

            goto done;
        }
        // There is already data dynamically allocated
        else {
            // Allocate new space if old + add exceeds already allocated space
            if (label_ptr->n_from_arr_alloc < label_ptr->n_from_arr + n) {
                label_ptr->n_from_arr_alloc = PDC_MAX(2*label_ptr->n_from_arr_alloc, label_ptr->n_from_arr+n); 
                label_ptr->from_arr = (int*)realloc(label_ptr->from_arr, label_ptr->n_from_arr_alloc*sizeof(int));
            }

            memcpy(&label_ptr->from_arr[label_ptr->n_from_arr], data, n*sizeof(int));
            label_ptr->n_from_arr += n;

            goto done;
        }

    }

done:
    return ret_value;
}

perr_t PDC_print_io_segments(PDC_IO_segment_t *segments, int n)
{
    perr_t ret_value = SUCCEED;
    int i;

    if (segments == NULL || n == 0) {
        ret_value = FAIL;
        printf("==PDC: %s - Invalid input\n", __func__);
        goto done;
    }

    for (i = 0; i < n; i++) {
        printf("%s, %5llu, %5llu\n", segments[i].filepath, segments[i].start, segments[i].size);
    }
 
done:
    return ret_value;
}

// Comparison function to sort with **decreasing** order
int PDC_compare_io_segments_incr(const void *a, const void *b)
{
    int res;
    if (a == NULL || b == NULL || 
            ((PDC_IO_segment_t *)a)->filepath == NULL || ((PDC_IO_segment_t *)b)->filepath == NULL ||
            ((PDC_IO_segment_t *)a)->filepath[0] == 0 || ((PDC_IO_segment_t *)a)->size == 0) 
        return 1;
    else if (((PDC_IO_segment_t *)b)->filepath[0] == 0 || ((PDC_IO_segment_t *)b)->size == 0) 
        return -1;

    // Check if the two filepath pointers are the same
    if (((PDC_IO_segment_t *)a)->filepath == ((PDC_IO_segment_t *)b)->filepath) 
        return ((PDC_IO_segment_t *)b)->start - ((PDC_IO_segment_t *)a)->start;

    // Compare filepath first, then start
    res = strcmp( ((PDC_IO_segment_t *)a)->filepath, ((PDC_IO_segment_t *)b)->filepath );
    if (res == 0) 
        res = ((PDC_IO_segment_t *)a)->start - ((PDC_IO_segment_t *)b)->start;

    return res;
}


// Comparison function to sort with **decreasing** order
int PDC_compare_io_segments_desc(const void *a, const void *b)
{
    int res;
    if (a == NULL || b == NULL || 
            ((PDC_IO_segment_t *)a)->filepath == NULL || ((PDC_IO_segment_t *)b)->filepath == NULL ||
            ((PDC_IO_segment_t *)a)->filepath[0] == 0 || ((PDC_IO_segment_t *)a)->size == 0) 
        return 1;
    else if (((PDC_IO_segment_t *)b)->filepath[0] == 0 || ((PDC_IO_segment_t *)b)->size == 0) 
        return -1;

    // Check if the two filepath pointers are the same
    if (((PDC_IO_segment_t *)a)->filepath == ((PDC_IO_segment_t *)b)->filepath) 
        return ((PDC_IO_segment_t *)b)->start - ((PDC_IO_segment_t *)a)->start;

    // Compare filepath first, then start
    res = strcmp( ((PDC_IO_segment_t *)a)->filepath, ((PDC_IO_segment_t *)b)->filepath );
    if (res == 0) 
        res = ((PDC_IO_segment_t *)b)->start - ((PDC_IO_segment_t *)a)->start;

    return res;
}

perr_t PDC_swap_io_segment(PDC_IO_segment_t *a, PDC_IO_segment_t *b)
{
    perr_t ret_value = SUCCEED;
    PDC_IO_segment_t tmp;


    if (a == NULL || b == NULL) {
        printf("==PDC: %s - NULL input\n", __func__);
        goto done;
    }

    memcpy(&tmp, a, sizeof(PDC_IO_segment_t));
    memcpy(a,    b, sizeof(PDC_IO_segment_t));
    memcpy(b, &tmp, sizeof(PDC_IO_segment_t));

done:
    return ret_value;
}

// **In-place** Merge all overlap segments within the same file
// This has O(nlogn) time complexity for qsort
perr_t PDC_merge_overlap_io_segments_inplace(PDC_IO_segment_t *segments, int n, int *n_res)
{
    perr_t ret_value = SUCCEED;
    int i, j, m, begin, index;

    if (segments == NULL || n == 0 || n_res == NULL) {
        ret_value = FAIL;
        printf("==PDC: %s - Invalid input\n", __func__);
        goto done;
    }
    
    *n_res = 0;

    // First sort the segments based on filepath and start
    qsort(segments, n, sizeof(PDC_IO_segment_t), PDC_compare_io_segments_desc);

    /* printf("After qsort\n"); */
    /* PDC_print_io_segments(segments, n); */

    begin = 0;
    for (i = 1; i <= n; i++) {
        // Find the number of segments belonging to the same file
        if (i == n || strcmp(segments[i].filepath, segments[begin].filepath) != 0) {
            index = begin;
            // Now we want to merge all segments from segments[begin] to segments[i-1]
            for (j = begin; j < i; j++) {
                // If this is not first Interval and overlaps with the previous one
                if (index != begin && segments[index-1].start <= segments[j].start + segments[j].size) {
                    while (index != begin && segments[index-1].start <= segments[j].start + segments[j].size) {
                        // Merge previous and current Intervals
                        segments[index-1].size  = PDC_MAX(segments[index-1].size + segments[index-1].start,
                                                          segments[j].size       + segments[j].start);
                        segments[index-1].start = PDC_MIN(segments[index-1].start, segments[j].start);
                        segments[index-1].size -= segments[index-1].start;
                        index--;
                    }
                }
                else {
                    // Doesn't overlap with previous, add to solution
                    segments[index] = segments[j];
                }
         
                index++;
            }
            
            *n_res += index - begin;
            // Mark the merged ones as all 0s
            memset(&segments[index], 0, sizeof(PDC_IO_segment_t) * (i - index));

            begin = i;
        }
    }

    // Now merged segments are stored in segments, with gaps and reversed sorted
    // Reverse the order and gaps
    qsort(segments, n, sizeof(PDC_IO_segment_t), PDC_compare_io_segments_incr);

done:
    return ret_value;
}


// Merge all overlap segments within the same file, 
// segments must be **sorted** with desc order on filepath and offset
// Allocate space for the output inside this function
perr_t PDC_merge_overlap_io_segments(PDC_IO_segment_t *segments, int n, PDC_IO_segment_t **merged, int *n_res)
{
    perr_t ret_value = SUCCEED;
    int i, j, m, begin, index;

    if (segments == NULL || n == 0 || n_res == NULL || merged == NULL) {
        ret_value = FAIL;
        printf("==PDC: %s - Invalid input\n", __func__);
        goto done;
    }
    
    *n_res = 0;
    PDC_IO_segment_t *tmp_merged = (PDC_IO_segment_t*)malloc(n * sizeof(PDC_IO_segment_t));
    memcpy(tmp_merged, segments, n * sizeof(PDC_IO_segment_t));

    // First sort the segments based on filepath and start
    /* qsort(tmp_merged, n, sizeof(PDC_IO_segment_t), PDC_compare_io_segments_desc); */

    /* printf("After qsort\n"); */
    /* PDC_print_io_segments((*tmp_merged), n); */

    begin = 0;
    for (i = 1; i <= n; i++) {
        // Find the number of segments belonging to the same file
        if (i == n || strcmp(tmp_merged[i].filepath, tmp_merged[begin].filepath) != 0) {
            index = begin;
            // Now we want to merge all segments from segments[begin] to segments[i-1]
            for (j = begin; j < i; j++) {
                // If this is not first Interval and overlaps with the previous one
                if (index != begin && tmp_merged[index-1].start <= tmp_merged[j].start + tmp_merged[j].size) {
                    while (index != begin && tmp_merged[index-1].start <= 
                            tmp_merged[j].start + tmp_merged[j].size) {

                        // Merge previous and current Intervals
                        tmp_merged[index-1].size  = PDC_MAX(tmp_merged[index-1].size + tmp_merged[index-1].start,
                                                          tmp_merged[j].size       + tmp_merged[j].start);
                        tmp_merged[index-1].start = PDC_MIN(tmp_merged[index-1].start, tmp_merged[j].start);
                        tmp_merged[index-1].size -= tmp_merged[index-1].start;

                        index--;
                    }
                }
                else {
                    // Doesn't overlap with previous, add to solution
                    tmp_merged[index] = tmp_merged[j];
                }
         

                index++;
            }
            
            *n_res += index - begin;
            // Mark the merged ones as all 0s
            memset(&tmp_merged[index], 0, sizeof(PDC_IO_segment_t) * (i - index));

            begin = i;
        }
    }

    *merged = (PDC_IO_segment_t*)malloc( (*n_res) * sizeof(PDC_IO_segment_t));
    j = 0;
    for (i = n-1; i >= 0 ; i--) {
        if (tmp_merged[i].size == 0) 
            continue;
        memcpy(&((*merged)[j]), &tmp_merged[i], sizeof(PDC_IO_segment_t)); 
        j++;
    }

done:
    free(tmp_merged);
    return ret_value;
}


int test0()
{
    uint64_t starts[5] = {9,  3, 2, 5, 8 };
    uint64_t ends[5]   = {11, 7, 4, 7, 10};

    int i, n = 5, m;
    PDC_IO_segment_t *merged;
    PDC_IO_segment_t *segments = (PDC_IO_segment_t*)calloc(n, sizeof(PDC_IO_segment_t));
    for (i = 0; i < n; i++) {
        sprintf(segments[i].filepath, "/file/path/to/file%d", i/5);
        segments[i].start = starts[i];
        segments[i].size  = ends[i] - starts[i];
    }

    printf("After init\n");
    PDC_print_io_segments(segments, n);

    PDC_merge_overlap_io_segments(segments, n, &merged, &m);

    printf("After merge\n");
    PDC_print_io_segments(merged, m);

    free(merged);
    free(segments);
}

int test1()
{
    uint64_t starts[10] = {0, 5, 25, 10, 12,  3, 0,  5, 10, 17};
    uint64_t ends[10]   = {5, 7, 40, 15, 20,  5, 7, 40, 15, 20};

    int i, n = 10, m;
    PDC_IO_segment_t *merged;
    PDC_IO_segment_t *segments = (PDC_IO_segment_t*)calloc(n, sizeof(PDC_IO_segment_t));
    for (i = 0; i < n; i++) {
        sprintf(segments[i].filepath, "/file/path/to/file%d", i/5);
        segments[i].start = starts[i];
        segments[i].size  = ends[i] - starts[i];
    }


    PDC_merge_overlap_io_segments(segments, n, &merged, &m);

    printf("Original:\n");
    PDC_print_io_segments(segments, n);

    printf("Merged: \n");
    PDC_print_io_segments(merged, m);

    free(merged);
    free(segments);
}

int test2_inplace()
{
    uint64_t starts[10] = {0, 6, 5,     10, 17, 3,      0, 7, 1,      1};
    uint64_t ends[10]   = {5, 7, 6,     15, 20, 5,      7, 9, 15,     2};

    int i, n = 10, m;
    PDC_IO_segment_t *segments = (PDC_IO_segment_t*)calloc(n, sizeof(PDC_IO_segment_t));
    for (i = 0; i < n; i++) {
        sprintf(segments[i].filepath, "/file/path/to/file%d", i/3);
        segments[i].start = starts[i];
        segments[i].size  = ends[i] - starts[i];
    }

    printf("Original:\n");
    PDC_print_io_segments(segments, n);

    PDC_merge_overlap_io_segments_inplace(segments, n, &m);

    printf("Merged: \n");
    PDC_print_io_segments(segments, m);

    free(segments);
}


int test2()
{
    uint64_t starts[10] = {0, 6, 5,     10, 17, 3,      0, 7, 1,      1};
    uint64_t ends[10]   = {5, 7, 6,     15, 20, 5,      7, 9, 15,     2};

    int i, n = 10, m;
    PDC_IO_segment_t *merged;
    PDC_IO_segment_t *segments = (PDC_IO_segment_t*)calloc(n, sizeof(PDC_IO_segment_t));
    for (i = 0; i < n; i++) {
        sprintf(segments[i].filepath, "/file/path/to/file%d", i/3);
        segments[i].start = starts[i];
        segments[i].size  = ends[i] - starts[i];
    }


    PDC_merge_overlap_io_segments(segments, n, &merged, &m);

    printf("Original:\n");
    PDC_print_io_segments(segments, n);

    printf("Merged: \n");
    PDC_print_io_segments(merged, m);

    free(merged);
    free(segments);
}

int label_test0()
{
    int i;
    PDC_label_t *label1 = PDC_label_create();
    PDC_label_t *label2 = PDC_label_create();
    PDC_label_t *label3 = PDC_label_create();
    PDC_IO_segment_t segment;

    PDC_label_print(label1); 
    PDC_label_attach(&segment, &segment.label, label1);

    if (segment.label->attached_addr != &segment || segment.label != label1) {
        printf("Error with PDC_label atach!\n");
        return -1;
    }
    else
        printf("PDC_label_atach SUCCEED!\n");

    int from = 2;
    int from_arr[5] = {7, 6, 5, 4, 3};
    PDC_label_add_from(label1, 1, &from);

    printf("Expecting: %d\n", from);
    PDC_label_print(label1); 

    PDC_label_add_from(label1, 5, from_arr);
    printf("Expecting: 2, 7, 6, 5, 4, 3\n", from);
    PDC_label_print(label1); 


    for (i = 0; i < 10; i++) {
        PDC_label_add_from(label2, 5, from_arr);
    }

    printf("Expecting: 7, 6, 5, 4, 3 repeat 10 times\n", from);
    PDC_label_print(label2); 
    printf("Allocate size = %d(%d)\n", label2->n_from_arr_alloc, 2*PDC_LABEL_INIT_ALLOC_SIZE);

    int to_arr[40];
    for (i = 0; i < 40; i++) {
        to_arr[i] = 100 + i;
    }

    PDC_label_add_to(label3, 5, from_arr);
    printf("Expecting: 7, 6, 5, 4, 3 \n", from);
    PDC_label_print(label3); 
    printf("Allocate size = %d(%d)\n", label3->n_to_arr_alloc, PDC_LABEL_INIT_ALLOC_SIZE);

    PDC_label_add_to(label3, 40, to_arr);
    printf("Expecting: 7, 6, 5, 4, 3, 100, 101, 102, ..., 139\n", from);
    PDC_label_print(label3); 
    printf("Allocate size = %d(%d)\n", label3->n_to_arr_alloc, 2*PDC_LABEL_INIT_ALLOC_SIZE);

    PDC_label_destroy(label1);
    PDC_label_destroy(label2);
    PDC_label_destroy(label3);
}

int main(int argc, const char *argv[])
{
    int size = 1, rank = 0;

#ifdef ENABLE_MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &proc_num);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
#endif

    
    printf("Test 0\n");
    test0();
    printf("\n\nTest 1\n");
    test1();
    printf("\n\nTest 2\n");
    test2();
    printf("\n\nTest 2 - Inplace\n");
    test2_inplace();


    printf("Label Test 0\n");
    label_test0();

#ifdef ENABLE_MPI
    MPI_Finalize();
#endif
    return 0;
}
