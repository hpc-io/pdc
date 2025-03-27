/*
 * Copyright Notice for
 * Proactive Data Containers (PDC) Software Library and Utilities
 * -----------------------------------------------------------------------------

 *** Copyright Notice ***

 * Proactive Data Containers (PDC) Copyright (c) 2017, The Regents of the
 * University of California, through Lawrence Berkeley National Laboratory,
 * UChicago Argonne, LLC, operator of Argonne National Laboratory, and The HDF
 * Group (subject to receipt of any required approvals from the U.S. Dept. of
 * Energy).  All rights reserved.

 * If you have questions about your rights to use or distribute this software,
 * please contact Berkeley Lab's Innovation & Partnerships Office at  IPO@lbl.gov.

 * NOTICE.  This Software was developed under funding from the U.S. Department of
 * Energy and the U.S. Government consequently retains certain rights. As such, the
 * U.S. Government has been granted for itself and others acting on its behalf a
 * paid-up, nonexclusive, irrevocable, worldwide license in the Software to
 * reproduce, distribute copies to the public, prepare derivative works, and
 * perform publicly and display publicly, and to permit other to do so.
 */

#ifndef PDC_REGION_H
#define PDC_REGION_H

#include "pdc_public.h"
#include "pdc_obj.h"

#ifdef ENABLE_MPI
#include "mpi.h"
#endif

#define DIM_MAX                      4

/**************************/
/* Library Public Struct */
/**************************/
struct pdc_region_info {
    pdcid_t               local_id;
    struct _pdc_obj_info *obj;
    size_t                ndim;
    uint64_t *            offset;
    uint64_t *            size;
    bool                  mapping;
    int                   registered_op;
    void *                buf;
    size_t                unit;
};

// Similar structure PDC_region_info_t defined in pdc_obj_pkg.h
typedef struct region_info_transfer_t {
    size_t   ndim;
    uint64_t start[DIM_MAX];
    uint64_t count[DIM_MAX];
} region_info_transfer_t;

typedef enum {
    PDC_TRANSFER_STATUS_COMPLETE  = 0,
    PDC_TRANSFER_STATUS_PENDING   = 1,
    PDC_TRANSFER_STATUS_NOT_FOUND = 2
} pdc_transfer_status_t;

/*********************/
/* Public Prototypes */
/*********************/

/**
 * Region utility functions.
 */
int check_overlap(int ndim, uint64_t *offset1, uint64_t *size1, uint64_t *offset2, uint64_t *size2);

int PDC_region_overlap_detect(int ndim, uint64_t *offset1, uint64_t *size1, uint64_t *offset2,
                              uint64_t *size2, uint64_t **output_offset, uint64_t **output_size);

int memcpy_subregion(int ndim, uint64_t unit, pdc_access_t access_type, char *buf, uint64_t *size,
                     char *sub_buf, uint64_t *sub_offset, uint64_t *sub_size);

int memcpy_overlap_subregion(int ndim, uint64_t unit, char *buf, uint64_t *offset, uint64_t *size, char *buf2,
                             uint64_t *offset2, uint64_t *size2, uint64_t *overlap_offset,
                             uint64_t *overlap_size);

int detect_region_contained(uint64_t *offset, uint64_t *size, uint64_t *offset2, uint64_t *size2, int ndim);

/**
 * Create a region
 *
 * \param ndims [IN]            Number of dimensions
 * \param offset [IN]           Offset of each dimension
 * \param size [IN]             Size of each dimension
 *
 * \return Object id on success/Zero on failure
 */
pdcid_t PDCregion_create(psize_t ndims, uint64_t *offset, uint64_t *size);

/**
 * Close a region
 *
 * \param region_id [IN]        ID of the object
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCregion_close(pdcid_t region_id);

/**
 * ********
 *
 * \param region [IN]           *********
 */
void PDCregion_free(struct pdc_region_info *region);

/**
 * Create a region transfer request (asynchronously)
 *
 * \param buf [IN]              Start point of an application buffer
 * \param access_type[IN]       Read or write operation
 * \param obj_id [IN]           Object ID
 * \param local_reg  [IN]       ID of the source region
 * \param remote_reg [IN]       ID of the target region
 *
 * \return ID of the newly create region transfer request
 */
pdcid_t PDCregion_transfer_create(void *buf, pdc_access_t access_type, pdcid_t obj_id, pdcid_t local_reg,
                                  pdcid_t remote_reg);

/**
 * Start a region transfer from local region to remote region for an object on buf.
 *
 * \param transfer_request_id [IN]           ID of the region transfer request
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCregion_transfer_start(pdcid_t transfer_request_id);

/**
 * Start several region transfer requests (asynchronously), can be for different objects.
 *
 * \param transfer_request_id [IN]           ID pointer array of the region transfer requests
 * \param size [IN]                          Number of requests in transfer_request_id
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCregion_transfer_start_all(pdcid_t *transfer_request_id, int size);

#ifdef ENABLE_MPI
/**
 * Start a region transfer request (asynchronously), MPI collective version for better performance at scale.
 *
 * \param transfer_request_id [IN]           ID of the region transfer request
 * \param comm [IN]                          MPI communicator
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCregion_transfer_start_mpi(pdcid_t transfer_request_id, MPI_Comm comm);

/**
 * Start several region transfer requests (asynchronously), MPI collective version for better performance at
 * scale.
 *
 * \param transfer_request_id [IN]           ID pointer array of the region transfer requests
 * \param size [IN]                          Number of requests in transfer_request_id
 * \param comm [IN]                          MPI communicator
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCregion_transfer_start_all_mpi(pdcid_t *transfer_request_id, int size, MPI_Comm comm);
#endif

/**
 * Retrieve the status of a region transfer request
 *
 * \param transfer_request_id [IN]           ID of the region transfer request
 * \param completed [OUT]                    Result
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCregion_transfer_status(pdcid_t transfer_request_id, pdc_transfer_status_t *completed);

/**
 * Block and wait for a region transfer request to finish
 *
 * \param transfer_request_id [IN]           ID of the region transfer request
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCregion_transfer_wait(pdcid_t transfer_request_id);

/**
 * Block and wait for several region transfer request to finish
 *
 * \param transfer_request_id [IN]           ID of the region transfer request
 * \param size [IN]                          Number of requests in transfer_request_id
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCregion_transfer_wait_all(pdcid_t *transfer_request_id, int size);

/**
 * Close a transfer request, free internal resources
 *
 * \param transfer_request_id [IN]           ID of the region transfer request
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCregion_transfer_close(pdcid_t transfer_request_id);

/**
 * Map an application buffer to an object
 *
 * \param buf [IN]              Start point of an application buffer
 * \param local_type [IN]       Data type of data in memory
 * \param local_reg  [IN]       ID of the source region
 * \param remote_obj [IN]       ID of the target object
 * \param remote_reg [IN]       ID of the target region
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCbuf_obj_map(void *buf, pdc_var_type_t local_type, pdcid_t local_reg, pdcid_t remote_obj,
                      pdcid_t remote_reg);

/**
 * Get region information
 *
 * \param reg_id [IN]           ID of the region
 * \param obj_id [IN]           ID of the object
 *
 * \return Pointer to pdc_region_info struct on success/Null on failure
 */
struct pdc_region_info *PDCregion_get_info(pdcid_t reg_id);

/**
 * Unmap all regions within the object from a buffer (write unmap)
 *
 * \param remote_obj_id [IN]    ID of the target object
 * \param remote_reg_id [IN]    ID of the target region
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCbuf_obj_unmap(pdcid_t remote_obj_id, pdcid_t remote_reg_id);

/**
 * Obtain the region lock
 *
 * \param obj_id [IN]           ID of the object
 * \param reg_id [IN]           ID of the region
 * \param access_type [IN]      Region access type: READ or WRITE
 * \param lock_mode [IN]        Lock mode of the region: BLOCK or NOBLOCK
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCreg_obtain_lock(pdcid_t obj_id, pdcid_t reg_id, pdc_access_t access_type,
                          pdc_lock_mode_t lock_mode);


/**
 * Check if two region info transfer are identical
 *
 * \param reg1 [IN]             Pointer to region info transfer to compare
 * \param reg2 [IN]             Pointer to region info transfer to compare
 *
 * \return 1 if they are the same/-1 otherwise
 */
pbool_t PDC_region_info_transfer_is_equal(const region_info_transfer_t* reg1, const region_info_transfer_t* reg2);

/**
 * Set region info transfer
 *
 * \param src_reg [IN]            Pointer to src region info transfer
 * \param dest_reg [IN]             Pointer to region to compare
 *
 * \return Non-negative on Success/Negative on failure
 */
perr_t PDC_set_region_info_transfer(const region_info_transfer_t* src_reg, region_info_transfer_t* dest_reg);

/**
 * Get size in bytes of region with dimensions
 * \param src_reg [IN] Pointer to the source region
 * \param unit [IN] Size of each element in source and destination regions
 * \param ndim [IN] Number of dimensions in source region
 * 
 * \return size in bytes of src_reg
 */
uint64_t get_region_with_dims_size_bytes(uint64_t* src_reg, int unit, int ndim);

/**
 * Get the size in elements of region with dimensions from region with dimentions and bytes
 * \param src_reg [IN] Pointer to the source region
 * \param unit [IN] Size of each element in source and destination regions
 * \param ndim [IN] Number of dimensions in source region
 * 
 * \return size in bytes of src_reg
 */
uint64_t get_region_with_dims_size_from_region_size_bytes(const uint64_t* src_reg, int unit, int ndim);

/**
 * Get the size in elements of region with dimensions
 * \param src_reg [IN] Pointer to the source region
 * \param ndim [IN] Number of dimensions in source region
 * 
 * \return size in elements of src_reg
 */
uint64_t get_region_with_dims_size(const uint64_t* src_reg, int ndim);

/**
 * set size in bytes of region with dimensions
 * \param src_reg [IN] Pointer to the source region
 * \param dest_reg [IN] Pointer to the destination region
 * \param unit [IN] Size of each element in source and destion regions
 * \param ndim [IN] Number of dimensions in destination region
 * 
 * \return Non-negative on Success/Negative on failure
 */
perr_t set_region_in_elements_from_region_size_bytes(const uint64_t* src_reg, uint64_t* dest_reg, int unit, int ndim);

/**
 * Set dest region  equal to source region taking into account the dimensions 
 * Validates that the source and destination addresses are not null
 * \param src_reg [IN] Pointer to the start address of source region
 * \param dest_rewg [IN] Point to the start address of the destination region
 * \param unit [IN] Size of each element in source and destion regions
 * 
 * \return Non-negative on Success/Negative on failure
 */
perr_t set_region_with_dims(const uint64_t* src_reg, uint64_t* dest_reg, int src_ndim, int dest_ndim);

/**
 * Set dest region equal to source region taking into account the dimensions in bytes
 * E.g., it sets the dest region sizes equal to the src region size * unit
 * Validates that the source and destination addresses are not null
 * \param src_reg [IN] Pointer to the start address of source region
 * \param dest_reg [IN] Point to the start address of the destination region
 * \param unit [IN] Size of each element in source and destion regions
 * \param dest_ndim [IN] number of dimensions of destination region
 * 
 * \return Non-negative on Success/Negative on failure
 */
perr_t set_region_with_dims_to_size_bytes(const uint64_t* src_reg, uint64_t* dest_reg, int unit, int dest_ndim);

#endif /* PDC_REGION_H */
