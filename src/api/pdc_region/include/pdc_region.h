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

#define DIM_MAX 4

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
perr_t PDCregion_transfer_start_coll(pdcid_t transfer_request_id, MPI_Comm comm);

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
perr_t PDCregion_transfer_start_all_coll(pdcid_t *transfer_request_id, int size, MPI_Comm comm);
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
 * Block and wait for several region transfer requests to finish
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
 * Release the region lock
 *
 * \param obj_id [IN]           ID of the object
 * \param reg_id [IN]           ID of the region
 * \param access_type [IN]      Region access type
 *
 * \return Non-negative on success/Negative on failure
 */
perr_t PDCreg_release_lock(pdcid_t obj_id, pdcid_t reg_id, pdc_access_t access_type);

/**
 * @brief Check if two region info transfers are identical.
 *
 * Compares two region info transfer structures (`reg1` and `reg2`) to determine if they
 * are equal. The comparison checks if all bytes within the structures match.
 *
 * @param reg1 [IN] Pointer to the first region info transfer to compare.
 * @param reg2 [IN] Pointer to the second region info transfer to compare.
 *
 * @return 1 if the region info transfers are identical, -1 otherwise.
 */
pbool_t PDC_region_info_transfer_t_is_equal(const region_info_transfer_t *reg1,
                                            const region_info_transfer_t *reg2);

/**
 * @brief Copy a region info transfer to another region.
 *
 * Copies the data from one region info transfer structure (`src_reg`) to another (`dest_reg`).
 *
 * @param src_reg [IN] Pointer to the source region info transfer to copy from.
 * @param dest_reg [OUT] Pointer to the destination region info transfer to copy to.
 *
 * @return Non-negative on success, negative on failure.
 */
perr_t PDC_copy_region_info_transfer_t(const region_info_transfer_t *src_reg,
                                       region_info_transfer_t *      dest_reg);

/**
 * @brief Calculate the size of a region descriptor in bytes.
 *
 * This function computes the size of the region described by `src_reg` in bytes by
 * multiplying the size of each dimension by the element size (`unit`).
 * The size in bytes is computed as the product of all dimensions, scaled by `unit`.
 *
 * @param src_reg [IN] Pointer to the source region descriptor (in elements)
 * @param unit [IN] Size of each element in bytes
 * @param ndim [IN] Number of dimensions in the region descriptor
 *
 * @return The size of the region in bytes.
 */
uint64_t PDC_get_region_desc_size_bytes(uint64_t *src_reg, int unit, int ndim);

/**
 * @brief Calculate the size of a region descriptor in elements from region desccriptor in bytes.
 *
 * This function computes the size of the region described by `src_reg` in terms of the
 * number of elements by dividing the given byte size by the element size (`unit`).
 *
 * @param src_reg [IN] Pointer to the source region descriptor (in bytes)
 * @param unit [IN] Size of each element in bytes
 * @param ndim [IN] Number of dimensions in the region descriptor
 *
 * @return The size of the region in terms of the number of elements.
 */
uint64_t PDC_get_region_desc_size_from_bytes_to_elements(const uint64_t *src_reg, int unit, int ndim);

/**
 * @brief Calculate the total size of a region descriptor in elements.
 *
 * This function computes the total size of the region described by `src_reg`,
 * by multiplying the dimensions of the region. The size is returned in terms
 * of the number of elements, not bytes.
 *
 * @param src_reg [IN] Pointer to the source region descriptor (in elements)
 * @param ndim [IN] Number of dimensions in the region descriptor
 *
 * @return The total size of the region in terms of the number of elements.
 */
uint64_t PDC_get_region_desc_size(const uint64_t *src_reg, int ndim);

/**
 * @brief Convert a region descriptor from byte units to element units.
 *
 * Copies the region dimensions from the source descriptor to the destination descriptor,
 * converting each dimension from bytes to elements by dividing by the element size.
 * For each dimension i:
 * dest_reg[i] = src_reg[i] / unit
 *
 * Both source and destination pointers must be non-null.
 *
 * @param src_reg [IN] Pointer to the source region descriptor (in bytes)
 * @param dest_reg [OUT] Pointer to the destination region descriptor (in elements)
 * @param unit [IN] Size of each element in bytes
 * @param ndim [IN] Number of dimensions in the region
 *
 * @return Non-negative on success, negative on failure.
 */
perr_t PDC_copy_region_desc_bytes_to_elements(const uint64_t *src_reg, uint64_t *dest_reg, int unit,
                                              int ndim);

/**
 * @brief Copy a region descriptor from source to destination without unit scaling.
 *
 * Copies each dimension from the source region descriptor to the destination region
 * descriptor without applying any scaling. The number of dimensions may differ between
 * source and destination, in which case only the common number of dimensions is copied.
 *
 * Validates that both source and destination pointers are not NULL.
 *
 * @param src_reg [IN] Pointer to the source region descriptor
 * @param dest_reg [OUT] Pointer to the destination region descriptor
 * @param src_ndim [IN] Number of dimensions in the source region
 * @param dest_ndim [IN] Number of dimensions in the destination region
 *
 * @return Non-negative on success, negative on failure.
 */
perr_t PDC_copy_region_desc(const uint64_t *src_reg, uint64_t *dest_reg, int src_ndim, int dest_ndim);

/**
 * @brief Copy a region descriptor from element units to byte units.
 *
 * Sets each dimension of the destination region descriptor equal to the
 * corresponding dimension of the source descriptor multiplied by the element size in bytes.
 * For the i-th dimension: dest_reg[i] = src_reg[i] * unit
 *
 * @param src_reg [IN] Pointer to the source region descriptor (in elements)
 * @param dest_reg [OUT] Pointer to the destination region descriptor (in bytes)
 * @param unit [IN] Size of each element in bytes
 * @param dest_ndim [IN] Number of dimensions in the destination region
 *
 * @return Non-negative on success, negative on failure.
 */
perr_t PDC_copy_region_desc_elements_to_bytes(const uint64_t *src_reg, uint64_t *dest_reg, int unit,
                                              int dest_ndim);

#endif /* PDC_REGION_H */
