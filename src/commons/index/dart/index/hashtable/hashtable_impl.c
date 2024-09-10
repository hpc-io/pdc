
// void
// create_hash_table_for_keyword(char *keyword, char *value, size_t len, void *data)
// {
//     uint32_t hashVal = djb2_hash(keyword, (int)len);
//     printf("%d:", hashVal);
//     gen_obj_id_in_t  in;
//     gen_obj_id_out_t out;

//     in.data.obj_name  = keyword;
//     in.data.time_step = (int32_t)data;
//     in.data.user_id   = (uint32_t)data;
//     char *taglist     = (char *)calloc(256, sizeof(char));
//     printf("%s=%s", keyword, value);
//     sprintf(taglist, "%s=%s", keyword, value);
//     in.data.tags          = taglist;
//     in.data.data_location = " ";
//     in.data.app_name      = " ";
//     in.data.ndim          = 1;
//     in.hash_value         = hashVal;

//     PDC_insert_metadata_to_hash_table(&in, &out);
// }

// int
// brutal_force_partial_search(metadata_query_transfer_in_t *in, uint32_t *n_meta, void ***buf_ptrs,
//                             char *k_query, char *vfrom_query, char *vto_query, uint32_t *hash_value)
// {
//     int result = 0;

//     uint32_t          iter = 0;
//     HashTableIterator hash_table_iter;
//     HashTableValue *  head = NULL;
//     pdc_metadata_t *  elt;
//     int               n_entry;

//     if (metadata_hash_table_g != NULL) {
//         if (hash_value != NULL) {
//             head = hash_table_lookup(metadata_hash_table_g, hash_value);
//             if (head != NULL) {
//                 DL_FOREACH(head->metadata, elt)
//                 {
//                     // List all objects, no need to check other constraints
//                     if (in->is_list_all == 1) {
//                         (*buf_ptrs)[iter++] = elt;
//                     }
//                     // check if current metadata matches search constraint
//                     else if (is_metadata_satisfy_constraint(elt, in) == 1) {
//                         (*buf_ptrs)[iter++] = elt;
//                     }
//                 }
//             }
//         }
//         else {
//             n_entry = hash_table_num_entries(metadata_hash_table_g);
//             hash_table_iterate(metadata_hash_table_g, &hash_table_iter);

//             while (n_entry != 0 && hash_table_iter_has_more(&hash_table_iter)) {
//                 head = hash_table_iter_next(&hash_table_iter);
//                 DL_FOREACH(head->metadata, elt)
//                 {
//                     // List all objects, no need to check other constraints
//                     if (in->is_list_all == 1) {
//                         (*buf_ptrs)[iter++] = elt;
//                     }
//                     // check if current metadata matches search constraint
//                     else if (is_metadata_satisfy_constraint(elt, in) == 1) {
//                         (*buf_ptrs)[iter++] = elt;
//                     }
//                 }
//             }
//         }
//         *n_meta = iter;

//         printf("==PDC_SERVER: brutal_force_partial_search: Total matching results: %d\n", *n_meta);
//         result = 1;
//     } // if (metadata_hash_table_g != NULL)
//     else {
//         printf("==PDC_SERVER: metadata_hash_table_g not initilized!\n");
//         result = 0;
//     }

//     return result;
// }

// void
// search_through_hash_table(char *k_query, uint32_t index_type, pattern_type_t pattern_type,
//                           pdc_art_iterator_param_t *param)
// {

//     metadata_query_transfer_in_t in;
//     in.is_list_all    = -1;
//     in.user_id        = -1;
//     in.app_name       = " ";
//     in.obj_name       = " ";
//     in.time_step_from = -1;
//     in.time_step_to   = -1;
//     in.ndim           = -1;
//     in.tags           = " ";
//     char *   qType_string;
//     uint32_t n_meta;
//     void **  buf_ptrs;
//     char *   tok;

//     uint32_t *hash_ptr   = NULL;
//     uint32_t  hash_value = -1;

//     switch (pattern_type) {
//         case PATTERN_EXACT:
//             qType_string = "Exact";
//             tok          = k_query;
//             if (index_type == 1) {
//                 hash_value = djb2_hash(tok, (int)strlen(tok));
//                 hash_ptr   = &hash_value;
//             }
//             else if (index_type == 2) {
//                 hash_value = djb2_hash(tok, 1);
//                 hash_ptr   = &hash_value;
//             }
//             break;
//         case PATTERN_PREFIX:
//             qType_string = "Prefix";
//             tok          = subrstr(k_query, strlen(k_query) - 1);
//             if (index_type == 2) {
//                 hash_value = djb2_hash(tok, 1);
//                 hash_ptr   = &hash_value;
//             }
//             else {
//                 hash_ptr = NULL;
//             }
//             break;
//         case PATTERN_SUFFIX:
//             qType_string = "Suffix";
//             tok          = substr(k_query, 1);
//             tok          = reverse_str(tok);
//             if (index_type == 2) {
//                 hash_value = djb2_hash(tok, 1);
//                 hash_ptr   = &hash_value;
//             }
//             else {
//                 hash_ptr = NULL;
//             }
//             break;
//         case PATTERN_MIDDLE:
//             qType_string = "Infix";
//             tok          = substring(k_query, 1, strlen(k_query) - 1);
//             break;
//         default:
//             break;
//     }

//     int search_rst = brutal_force_partial_search(&in, &n_meta, &buf_ptrs, k_query, NULL, NULL, hash_ptr);
//     int i          = 0;
//     for (i = 0; i < n_meta; i++) {
//         pdc_metadata_t *metadata = (pdc_metadata_t *)buf_ptrs[i];
//         hashset_add(param->out, (metadata->user_id));
//         param->total_count = param->total_count + 1;
//     }
// }

// void
// delete_hash_table_for_keyword(char *keyword, size_t len, void *data)
// {
//     uint32_t hashVal = djb2_hash(keyword, (int)len);

//     metadata_delete_in_t  in;
//     metadata_delete_out_t out;

//     in.obj_name   = keyword;
//     in.time_step  = (int32_t)data;
//     in.hash_value = hashVal;

//     PDC_delete_metadata_from_hash_table(&in, &out);
// }