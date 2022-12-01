#include "multidataset_plugin.h"

#ifdef PDC_PATCH
#include <set>

static pdcid_t pdc;
static pdcid_t cont;

static std::map<std::string, int> dp2event;

static std::vector<pdcid_t> cached_objs;
static std::vector<pdcid_t> cached_requests;
static std::vector<char*> cached_bufs;
static std::set<std::string> existing_objs;

static int hdf5_method_g = -1;
#endif

static std::map<std::string, multidataset_array*> multi_datasets;

int init_multidataset() {
    char *p = getenv("HEP_IO_TYPE");
    if ( p != NULL ) {
        set_hdf5_method(atoi(p));
    } else {
        set_hdf5_method(1);
    }
#ifdef PDC_PATCH
    pdcid_t cont_prop;
    pdc = PDCinit("pdc");
    cont_prop = PDCprop_create(PDC_CONT_CREATE, pdc);
    cont = PDCcont_create("C", cont_prop);
    PDCprop_close(cont_prop);
#endif
    return 0;
}

int finalize_multidataset() {
    unsigned i;
    std::map<std::string, multidataset_array*>::iterator it;
    std::vector<char*>::iterator it2;
    double start_time;
#ifdef PDC_PATCH
    flush_multidatasets();

    if (cached_objs.size()) {
#ifdef H5_TIMING_ENABLE
        register_timer_start(&start_time);
#endif
        //PDCregion_transfer_wait_all(&cached_requests[0], cached_requests.size());
#ifdef H5_TIMING_ENABLE
        register_PDCwait_timer_end(start_time);
#endif
        for ( i = 0; i < cached_requests.size(); ++i ) {
            PDCregion_transfer_close(cached_requests[i]);
        }
        for ( i = 0; i < cached_bufs.size(); ++i ) {
            free(cached_bufs[i]);
        }
        for ( i = 0; i < cached_objs.size(); ++i ) {
            PDCobj_close(cached_objs[i]);
        }
    }
    PDCcont_close(cont);
    PDCclose(pdc);
#else
    for ( it = multi_datasets.begin(); it != multi_datasets.end(); ++it ) {
        for ( it2 = it->second->temp_mem->begin(); it2 != it->second->temp_mem->end(); ++it2 ) {
            free(*it2);
        }
        if (it->second->did != -1) {
#ifdef H5_TIMING_ENABLE
            register_timer_start(&start_time);
#endif
            H5Dclose(it->second->did);
#ifdef H5_TIMING_ENABLE
            register_H5Dclose_timer_end(start_time);
#endif
        }
	delete it->second->start;
	delete it->second->end;
	delete it->second->temp_mem;
        free(it->second);
    }
#endif
    return 0;
}

int set_hdf5_method(int hdf5_method) {
    hdf5_method_g = hdf5_method;
    return 0;
}

int get_hdf5_method() {
    return hdf5_method_g;
}

static int wrap_hdf5_spaces(int total_requests, hsize_t *start, hsize_t *end, hid_t did, hid_t* dsid_ptr, hid_t *msid_ptr) {
    const hsize_t ndims = 1;
    hsize_t old_dims[ndims]; //our datasets are 1D
    hsize_t new_dims[ndims];
    hsize_t max_dims[ndims]; //= {H5S_UNLIMITED};
    hsize_t max_offset, data_size, total_data_size;
    hid_t dsid, msid;
    int i;

    dsid = H5Dget_space(did);
    H5Sget_simple_extent_dims(dsid, old_dims, max_dims);
    
    max_offset = end[0];
    for ( i = 1; i < total_requests; ++i ) {
        if ( max_offset < end[i] ) {
            max_offset = end[i];
        }
    }
    if (max_offset > old_dims[0]) {
        new_dims[0] = max_offset;
        H5Dset_extent(did, new_dims);
        H5Sclose(dsid);
        dsid = H5Dget_space(did);
    }

    data_size = end[0] - start[0];
    H5Sselect_hyperslab(dsid, H5S_SELECT_SET, start, NULL, &data_size, NULL);
    total_data_size = data_size;
    for ( i = 1; i < total_requests; ++i ) {
        data_size = end[i] - start[i];
        total_data_size += data_size;
        H5Sselect_hyperslab(dsid, H5S_SELECT_OR, start + i, NULL, &data_size, NULL);
    }
    max_dims[0] = H5S_UNLIMITED;
    msid = H5Screate_simple(ndims, &total_data_size, max_dims);

    *dsid_ptr = dsid;
    *msid_ptr = msid;
    return 0;
}
#ifdef PDC_PATCH
int register_multidataset_request(const char *name, void *buf, hsize_t start, hsize_t end, hid_t mtype) {
#else
int register_multidataset_request(const char *name, hid_t gid, void *buf, hsize_t start, hsize_t end, hid_t mtype) {
#endif
    std::string s(name);
    std::map<std::string, multidataset_array*>::iterator it;
    char *temp_mem;
    size_t esize = H5Tget_size (mtype) * (end - start);
    int flag = 0;

    it = multi_datasets.find(s);
    if ( it == multi_datasets.end()) {
        multidataset_array *multi_dataset = (multidataset_array *) malloc(sizeof(multidataset_array));
	multi_dataset->start = new std::vector<hsize_t>;
	multi_dataset->end =new std::vector<hsize_t>;
	multi_dataset->temp_mem = new std::vector<char*>;
        multi_datasets[s] = multi_dataset;
	it = multi_datasets.find(s);
        flag = 1;
    }
#ifdef PDC_PATCH
    int ndim = 1;
    uint64_t dims;
    if (flag) {
        if (existing_objs.find(s) == existing_objs.end()) {
            pdcid_t obj_prop = PDCprop_create(PDC_OBJ_CREATE, pdc);
            PDCprop_set_obj_transfer_region_type(obj_prop, PDC_REGION_DYNAMIC);
            if (mtype == H5T_NATIVE_ULLONG){
                PDCprop_set_obj_type(obj_prop, PDC_UINT64);
            } else {
                PDCprop_set_obj_type(obj_prop, PDC_CHAR);
            }
            dims = end - start;
            PDCprop_set_obj_dims(obj_prop, ndim, &dims);

            it->second->did = PDCobj_create(cont, name, obj_prop);
            PDCprop_close(obj_prop);
            existing_objs.insert(s);
        } else {
            it->second->did = PDCobj_open(name, pdc);
        }
    }
#else
    if (flag) {
        it->second->did = H5Dopen2(gid, name, H5P_DEFAULT);
    }
#endif
    it->second->start->push_back(start);
    it->second->end->push_back(end);
    temp_mem = (char*) malloc(esize);
    it->second->temp_mem->push_back(temp_mem);
    memcpy(temp_mem, buf, esize);
    it->second->last_end = end;
    it->second->mtype = mtype;

    return 0;
}

int register_multidataset_request_append(const char *name, hid_t gid, void *buf, hsize_t data_size, hid_t mtype) {
    std::string s(name);
    hsize_t start, end;

    std::map<std::string, multidataset_array*>::iterator it;

    it = multi_datasets.find(s);
    if ( it != multi_datasets.end() ){
        start = it->second->last_end;
        end = it->second->last_end + data_size;
    } else {
        start = 0;
        end = data_size;
    }

#ifdef PDC_PATCH
    register_multidataset_request(name, buf, start, end, mtype);
#else
    register_multidataset_request(name, gid, buf, start, end, mtype);
#endif
    return 0;
}

static int merge_requests(std::vector<hsize_t> *start, std::vector<hsize_t> *end, int request_size, std::vector<char*> *buf, hsize_t **new_start, hsize_t **new_end, char** new_buf, hid_t mtype, int *request_size_ptr) {
    int i, index;
    int merged_requests = request_size;
    char* ptr;
    size_t esize = H5Tget_size (mtype);
    size_t total_data_size = end[0][0] - start[0][0];

    for ( i = 1; i < request_size; ++i ) {
        total_data_size += end[0][i] - start[0][i];
        if ( end[0][i-1] == start[0][i] ) {
            merged_requests--;
        }
    }

    *new_start = (hsize_t*) malloc(sizeof(hsize_t) * merged_requests * 2);
    *new_end = new_start[0] + merged_requests;

    index = 0;
    new_start[0][0] = start[0][0];
    new_end[0][0] = end[0][0];

    *new_buf = (char*) malloc(esize * total_data_size);
    ptr = *new_buf;
    memcpy(ptr, buf[0][0], (end[0][0] - start[0][0]) * esize);
    ptr += (end[0][0] - start[0][0]) * esize;
    free(buf[0][0]);
    for ( i = 1; i < request_size; ++i ) {
        memcpy(ptr, buf[0][i], (end[0][i] - start[0][i]) * esize);
        ptr += (end[0][i] - start[0][i]) * esize;
        free(buf[0][i]);

        if ( end[0][i-1] < start[0][i] ) {
            index++;
            new_start[0][index] = start[0][i];
        }
        new_end[0][index] = end[0][i];
    }
    *request_size_ptr = merged_requests;
    return 0;
}

int flush_multidatasets() {
    unsigned i;
    int j;
#ifndef PDC_PATCH
    size_t esize;
    hsize_t dims[H5S_MAX_RANK], mdims[H5S_MAX_RANK];
    H5D_rw_multi_t *multi_datasets_temp;
    hid_t msid, dsid;
#endif
    hsize_t *new_start, *new_end;
    int new_request_size;
    std::map<std::string, multidataset_array*>::iterator it;
    int dataset_size = multi_datasets.size();
    char **temp_buf = (char**) malloc(sizeof(char*) * dataset_size);
#ifdef H5_TIMING_ENABLE
    double start_time;
#endif
    i = 0;
    //printf("Rank %d number of datasets to be written %d\n", rank, dataset_size);
#if ENABLE_MULTIDATASET==1
    #ifdef H5_TIMING_ENABLE
    increment_H5Dwrite();
    #endif
    multi_datasets_temp = (H5D_rw_multi_t*) malloc(sizeof(H5D_rw_multi_t) * dataset_size);
    for ( it = multi_datasets.begin(); it != multi_datasets.end(); ++it ) {
        if (it->second->did == -1) {
	    i++;
            continue;
        }

        //MPI_Barrier(MPI_COMM_WORLD);
        #ifdef H5_TIMING_ENABLE
        increment_H5Dwrite();
        #endif

        merge_requests(it->second->start, it->second->end, it->second->start->size(), it->second->temp_mem, &new_start, &new_end, &(temp_buf[i]), it->second->mtype, &new_request_size);
        multi_datasets_temp[i].dset_id = it->second->did;
        multi_datasets_temp[i].mem_type_id = it->second->mtype;
        multi_datasets_temp[i].u.wbuf = temp_buf[i];

        wrap_hdf5_spaces(new_request_size, new_start, new_end, it->second->did, &(multi_datasets_temp[i].dset_space_id), &(multi_datasets_temp[i].mem_space_id));
	i++;
    }

    H5Dwrite_multi(H5P_DEFAULT, dataset_size, multi_datasets_temp);

    for ( it = multi_datasets.begin(); it != multi_datasets.end(); ++it ) {
        if (it->second->did == -1) {
            continue;
        }
        H5Sclose(multi_datasets_temp[i].dset_space_id);
        H5Sclose(multi_datasets_temp[i].mem_space_id);
        H5Dclose(multi_datasets_temp[i].dset_id);
	delete it->second->start;
	delete it->second->end;
	delete it->second->temp_mem;
        free(temp_buf[i]);
    }

    free(multi_datasets_temp);
#else
    //printf("rank %d has dataset_size %lld\n", rank, (long long int) dataset_size);
#ifdef PDC_PATCH

    if (cached_objs.size()) {
#ifdef H5_TIMING_ENABLE
        register_timer_start(&start_time);
#endif
        //PDCregion_transfer_wait_all(&cached_requests[0], cached_requests.size());
#ifdef H5_TIMING_ENABLE
        register_PDCwait_timer_end(start_time);
#endif
        for ( i = 0; i < cached_requests.size(); ++i ) {
            PDCregion_transfer_close(cached_requests[i]);
        }
        for ( i = 0; i < cached_bufs.size(); ++i ) {
            free(cached_bufs[i]);
        }
        for ( i = 0; i < cached_objs.size(); ++i ) {
            PDCobj_close(cached_objs[i]);
        }
        cached_objs.clear();
        cached_requests.clear();
        cached_bufs.clear();
    }
    i = 0;
    for ( it = multi_datasets.begin(); it != multi_datasets.end(); ++it ) {

#ifdef H5_TIMING_ENABLE
        register_timer_start(&start_time);
#endif
        merge_requests(it->second->start, it->second->end, it->second->start->size(), it->second->temp_mem, &new_start, &new_end, temp_buf + i, it->second->mtype, &new_request_size);
#ifdef H5_TIMING_ENABLE
        register_merge_requests_timer_end(start_time);
#endif
        uint64_t max_dim = new_end[new_request_size - 1];
        PDCobj_set_dims(it->second->did, 1, &max_dim);
        char* ptr = temp_buf[i];
        for ( j = 0; j < new_request_size; ++j ) {
            uint64_t offset, offset_length;
            offset = new_start[j];
            offset_length = new_end[j] - new_start[j];
            pdcid_t reg = PDCregion_create(1, &offset, &offset_length);
            pdcid_t transfer_request_id = PDCregion_transfer_create(ptr, PDC_WRITE, it->second->did, reg, reg);
            //printf("did = %lu, starting request offset = %lu, size = %lu, j = %d\n", it->second->did, offset, offset_length, j);
            PDCregion_transfer_start(transfer_request_id);
            PDCregion_transfer_wait(transfer_request_id);

            cached_requests.push_back(transfer_request_id);
            if (it->second->mtype == H5T_NATIVE_CHAR) {
                ptr += new_end[j] - new_start[j];
            } else {
                ptr += (new_end[j] - new_start[j]) * sizeof(uint64_t);
            }
        }
        cached_bufs.push_back(temp_buf[i]);
        cached_objs.push_back(it->second->did);
        free(new_start);
        delete it->second->start;
        delete it->second->end;
        delete it->second->temp_mem;
        #ifdef H5_TIMING_ENABLE
        increment_PDCwrite();
        #endif
        i++;
    }
#ifdef H5_TIMING_ENABLE
    register_timer_start(&start_time);
#endif
    //PDCregion_transfer_start_all(&cached_requests[0], cached_requests.size());
#ifdef H5_TIMING_ENABLE
    register_PDCstart_timer_end(start_time);
#endif

#else
    for ( it = multi_datasets.begin(); it != multi_datasets.end(); ++it ) {
        if (it->second->did == -1) {
	    i++;
            continue;
        }
        #ifdef H5_TIMING_ENABLE
        increment_H5Dwrite();

        #endif
#ifdef H5_TIMING_ENABLE
        register_timer_start(&start_time);
#endif
        merge_requests(it->second->start, it->second->end, it->second->start->size(), it->second->temp_mem, &new_start, &new_end, temp_buf + i, it->second->mtype, &new_request_size);

#ifdef H5_TIMING_ENABLE
        register_merge_requests_timer_end(start_time);
#endif
#ifdef H5_TIMING_ENABLE
        register_timer_start(&start_time);
#endif
        wrap_hdf5_spaces(new_request_size, new_start, new_end, it->second->did, &dsid, &msid);
#ifdef H5_TIMING_ENABLE
        register_wrap_requests_timer_end(start_time);
#endif

#ifdef H5_TIMING_ENABLE
        register_timer_start(&start_time);
#endif
        H5Dwrite (it->second->did, it->second->mtype, msid, dsid, H5P_DEFAULT, temp_buf[i]);
#ifdef H5_TIMING_ENABLE
        register_H5Dwrite_timer_end(start_time);
#endif
        free(new_start);
        H5Sclose(dsid);
        H5Sclose(msid);
#ifdef H5_TIMING_ENABLE
        register_timer_start(&start_time);
#endif
        H5Dclose(it->second->did);
#ifdef H5_TIMING_ENABLE
        register_H5Dclose_timer_end(start_time);
#endif
	delete it->second->start;
	delete it->second->end;
	delete it->second->temp_mem;
        free(temp_buf[i]);
	i++;
    }
#endif
    free(temp_buf);
#endif
    multi_datasets.clear();
    return 0;
}
