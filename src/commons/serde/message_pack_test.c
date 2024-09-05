#include <stdio.h>
#include <msgpack.h>

void
serialize_to_messagepack()
{
    // Initialize a msgpack_sbuffer and a msgpack_packer
    msgpack_sbuffer sbuf;
    msgpack_sbuffer_init(&sbuf);

    msgpack_packer pk;
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    // Start packing the root object, which is a map with 5 key-value pairs
    msgpack_pack_map(&pk, 5);

    // Pack "dataset_name": "BOSS"
    msgpack_pack_str(&pk, strlen("dataset_name"));
    msgpack_pack_str_body(&pk, "dataset_name", strlen("dataset_name"));
    msgpack_pack_str(&pk, strlen("BOSS"));
    msgpack_pack_str_body(&pk, "BOSS", strlen("BOSS"));

    // Pack "dataset_description": "LLSM dataset"
    msgpack_pack_str(&pk, strlen("dataset_description"));
    msgpack_pack_str_body(&pk, "dataset_description", strlen("dataset_description"));
    msgpack_pack_str(&pk, strlen("LLSM dataset"));
    msgpack_pack_str_body(&pk, "LLSM dataset", strlen("LLSM dataset"));

    // Pack "source_URL": ""
    msgpack_pack_str(&pk, strlen("source_URL"));
    msgpack_pack_str_body(&pk, "source_URL", strlen("source_URL"));
    msgpack_pack_str(&pk, strlen(""));
    msgpack_pack_str_body(&pk, "", strlen(""));

    // Pack "collector": "Wei Zhang"
    msgpack_pack_str(&pk, strlen("collector"));
    msgpack_pack_str_body(&pk, "collector", strlen("collector"));
    msgpack_pack_str(&pk, strlen("Wei Zhang"));
    msgpack_pack_str_body(&pk, "Wei Zhang", strlen("Wei Zhang"));

    // Pack "objects" (array of maps)
    msgpack_pack_str(&pk, strlen("objects"));
    msgpack_pack_str_body(&pk, "objects", strlen("objects"));

    // Start packing the objects array (1 element in the array)
    msgpack_pack_array(&pk, 1);

    // Pack the first object map with 4 key-value pairs
    msgpack_pack_map(&pk, 4);

    // Pack "name": "3551-55156-1-coadd"
    msgpack_pack_str(&pk, strlen("name"));
    msgpack_pack_str_body(&pk, "name", strlen("name"));
    msgpack_pack_str(&pk, strlen("3551-55156-1-coadd"));
    msgpack_pack_str_body(&pk, "3551-55156-1-coadd", strlen("3551-55156-1-coadd"));

    // Pack "type": "file"
    msgpack_pack_str(&pk, strlen("type"));
    msgpack_pack_str_body(&pk, "type", strlen("type"));
    msgpack_pack_str(&pk, strlen("file"));
    msgpack_pack_str_body(&pk, "file", strlen("file"));

    // Pack "full_path": "/pscratch/sd/h/houhun/h5boss_v2/3551-55156.hdf5"
    msgpack_pack_str(&pk, strlen("full_path"));
    msgpack_pack_str_body(&pk, "full_path", strlen("full_path"));
    msgpack_pack_str(&pk, strlen("/pscratch/sd/h/houhun/h5boss_v2/3551-55156.hdf5"));
    msgpack_pack_str_body(&pk, "/pscratch/sd/h/houhun/h5boss_v2/3551-55156.hdf5",
                          strlen("/pscratch/sd/h/houhun/h5boss_v2/3551-55156.hdf5"));

    // Pack "properties" (array of maps with 4 elements)
    msgpack_pack_str(&pk, strlen("properties"));
    msgpack_pack_str_body(&pk, "properties", strlen("properties"));

    // Start packing the properties array
    msgpack_pack_array(&pk, 4);

    // Property 1: {"name": "AIRMASS", "value": 1.19428, "class": "singleton", "type": "float"}
    msgpack_pack_map(&pk, 4);
    msgpack_pack_str(&pk, strlen("name"));
    msgpack_pack_str_body(&pk, "name", strlen("name"));
    msgpack_pack_str(&pk, strlen("AIRMASS"));
    msgpack_pack_str_body(&pk, "AIRMASS", strlen("AIRMASS"));

    msgpack_pack_str(&pk, strlen("value"));
    msgpack_pack_str_body(&pk, "value", strlen("value"));
    msgpack_pack_float(&pk, 1.19428);

    msgpack_pack_str(&pk, strlen("class"));
    msgpack_pack_str_body(&pk, "class", strlen("class"));
    msgpack_pack_str(&pk, strlen("singleton"));
    msgpack_pack_str_body(&pk, "singleton", strlen("singleton"));

    msgpack_pack_str(&pk, strlen("type"));
    msgpack_pack_str_body(&pk, "type", strlen("type"));
    msgpack_pack_str(&pk, strlen("float"));
    msgpack_pack_str_body(&pk, "float", strlen("float"));

    // Property 2: {"name": "ALT", "value": 54.0012, "class": "singleton", "type": "float"}
    msgpack_pack_map(&pk, 4);
    msgpack_pack_str(&pk, strlen("name"));
    msgpack_pack_str_body(&pk, "name", strlen("name"));
    msgpack_pack_str(&pk, strlen("ALT"));
    msgpack_pack_str_body(&pk, "ALT", strlen("ALT"));

    msgpack_pack_str(&pk, strlen("value"));
    msgpack_pack_str_body(&pk, "value", strlen("value"));
    msgpack_pack_float(&pk, 54.0012);

    msgpack_pack_str(&pk, strlen("class"));
    msgpack_pack_str_body(&pk, "class", strlen("class"));
    msgpack_pack_str(&pk, strlen("singleton"));
    msgpack_pack_str_body(&pk, "singleton", strlen("singleton"));

    msgpack_pack_str(&pk, strlen("type"));
    msgpack_pack_str_body(&pk, "type", strlen("type"));
    msgpack_pack_str(&pk, strlen("float"));
    msgpack_pack_str_body(&pk, "float", strlen("float"));

    // Property 3: {"name": "ARCOFFX", "value": 0.001891, "class": "singleton", "type": "float"}
    msgpack_pack_map(&pk, 4);
    msgpack_pack_str(&pk, strlen("name"));
    msgpack_pack_str_body(&pk, "name", strlen("name"));
    msgpack_pack_str(&pk, strlen("ARCOFFX"));
    msgpack_pack_str_body(&pk, "ARCOFFX", strlen("ARCOFFX"));

    msgpack_pack_str(&pk, strlen("value"));
    msgpack_pack_str_body(&pk, "value", strlen("value"));
    msgpack_pack_float(&pk, 0.001891);

    msgpack_pack_str(&pk, strlen("class"));
    msgpack_pack_str_body(&pk, "class", strlen("class"));
    msgpack_pack_str(&pk, strlen("singleton"));
    msgpack_pack_str_body(&pk, "singleton", strlen("singleton"));

    msgpack_pack_str(&pk, strlen("type"));
    msgpack_pack_str_body(&pk, "type", strlen("type"));
    msgpack_pack_str(&pk, strlen("float"));
    msgpack_pack_str_body(&pk, "float", strlen("float"));

    // Property 4: {"name": "ARCOFFY", "value": 0.001101, "class": "singleton", "type": "float"}
    msgpack_pack_map(&pk, 4);
    msgpack_pack_str(&pk, strlen("name"));
    msgpack_pack_str_body(&pk, "name", strlen("name"));
    msgpack_pack_str(&pk, strlen("ARCOFFY"));
    msgpack_pack_str_body(&pk, "ARCOFFY", strlen("ARCOFFY"));

    msgpack_pack_str(&pk, strlen("value"));
    msgpack_pack_str_body(&pk, "value", strlen("value"));
    msgpack_pack_float(&pk, 0.001101);

    msgpack_pack_str(&pk, strlen("class"));
    msgpack_pack_str_body(&pk, "class", strlen("class"));
    msgpack_pack_str(&pk, strlen("singleton"));
    msgpack_pack_str_body(&pk, "singleton", strlen("singleton"));

    msgpack_pack_str(&pk, strlen("type"));
    msgpack_pack_str_body(&pk, "type", strlen("type"));
    msgpack_pack_str(&pk, strlen("float"));
    msgpack_pack_str_body(&pk, "float", strlen("float"));

    // Save the serialized data to a file
    FILE *file = fopen("dataset.msgpack", "wb");
    if (file) {
        fwrite(sbuf.data, sbuf.size, 1, file);
        fclose(file);
    }

    // Clean up
    msgpack_sbuffer_destroy(&sbuf);

    printf("Dataset serialized to MessagePack format and saved to dataset.msgpack\n");
}

int
main()
{
    serialize_to_messagepack();
    return 0;
}
