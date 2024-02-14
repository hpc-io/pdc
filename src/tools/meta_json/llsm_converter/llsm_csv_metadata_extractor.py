import pandas as pd
import os
import json

def extract_metadata(input_directory, output_directory, object_replica_number=100):
    object_replica_number = 100 # number of replicas for each object

    output_dict = {
        "dataset_name": "LLSM",
        "dataset_description": "LLSM dataset",
        "source_URL":"",
        "collector": "Wei Zhang",
        "objects":[]
    }

    for filename in os.listdir(input_directory):
        if filename.endswith('.csv'):
            filepath = os.path.join(input_directory, filename)
            df = pd.read_csv(filepath, delimiter=',')
            for incr in range(object_replica_number):
                for index, row in df.iterrows():
                    obj_id = str(index) + str(incr)
                    new_obj = {
                        "name": "object" + str(obj_id),
                        "type": "file",
                        "full_path": row['Filepath'],
                        "properties":[]
                    }
                    for column_name, value in row.items():
                        # format: %s/%sScan_Iter_%s_Cam%s_ch%d_CAM1_stack%04d_%dnm_%07dmsec_%010dmsecAbs%s
                        # variables used in original LLSM: stitching_rt, prefix, fullIter{n}, Cam(ncam), Ch(c), stackn(s), laser, abstime, fpgatime, z_str);
                        # example: Scan_Iter_0000_CamA_ch0_CAM1_stack0000_488nm_0000000msec_0067511977msecAbs_000x_000y_015z_0000t.tif
                        if column_name == 'Filename':
                            attr_in_fn = value.split('_')
                            # Scan Iter
                            scanIter = {
                                "name": "Scan Iter",
                                "value": int(attr_in_fn[2]) + incr,
                                "class" : "singleton",
                                "type": "int",
                            }
                            # CAM
                            CAM = {
                                "name": "Cam",
                                "value": attr_in_fn[3].replace('Cam', ''),
                                "class" : "singleton",
                                "type": "str",
                            }
                            # Ch
                            Ch = {
                                "name": "Ch",
                                "value": int(attr_in_fn[4].replace('ch', '')),
                                "class" : "singleton",
                                "type": "int",
                            }
                            # stackn
                            stackn = {
                                "name": "stackn",
                                "value": int(attr_in_fn[6].replace('stack', '')),
                                "class" : "singleton",
                                "type": "int",
                            }
                            # laser
                            laser = {
                                "name": "laser_nm",
                                "value": int(attr_in_fn[7].replace('nm', '')),
                                "class" : "singleton",
                                "type": "int",
                            }
                            # abstime
                            abstime = {
                                "name": "abstime",
                                "value": int(attr_in_fn[8].replace('msec', '')),
                                "class" : "singleton",
                                "type": "int",
                            }
                            # fpgatime
                            fpgatime = {
                                "name": "fpgatime",
                                "value": int(attr_in_fn[9].replace('msecAbs', '')),
                                "class" : "singleton",
                                "type": "int",
                            }
                            # x_str
                            x_str = {
                                "name": "x_str",
                                "value": int(attr_in_fn[10].replace('x', '')),
                                "class" : "singleton",
                                "type": "int",
                            }
                            # y_str
                            y_str = {
                                "name": "y_str",
                                "value": int(attr_in_fn[11].replace('y', '')),
                                "class" : "singleton",
                                "type": "int",
                            }
                            # z_str
                            z_str = {
                                "name": "z_str",
                                "value": int(attr_in_fn[12].replace('z', '')),
                                "class" : "singleton",
                                "type": "int",
                            }
                            # t_str
                            t_str = {
                                "name": "t_str",
                                "value": int(attr_in_fn[13].replace('t.tif', '')),
                                "class" : "singleton",
                                "type": "int",
                            }
                            new_obj["properties"].append(scanIter)
                            new_obj["properties"].append(CAM)
                            new_obj["properties"].append(Ch)
                            new_obj["properties"].append(stackn)
                            new_obj["properties"].append(laser)
                            new_obj["properties"].append(abstime)
                            new_obj["properties"].append(fpgatime)
                            new_obj["properties"].append(x_str)
                            new_obj["properties"].append(y_str)
                            new_obj["properties"].append(z_str)
                            new_obj["properties"].append(t_str)
                            continue # this will avoid adding the filename as a property, instead, we add all the attributes in the filename as properties
                        original_prop = {
                            "name": column_name,
                            "value": value,
                            "class" : "singleton",
                            "type": str(type(value).__name__)
                        }
                        new_obj["properties"].append(original_prop)
                    output_dict["objects"].append(new_obj);
                json_file_path = "{}/{}_{}.json".format(output_directory, filename, incr)
                with open(json_file_path, "w") as json_file:
                    json.dump(output_dict, json_file)
                    print("File {} has been written".format(json_file_path))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="LLSM CSV Metadata Extractor")
    parser.add_argument("-i", "--input_directory", required=True, type=str, help="Directory path containing CSV files")
    parser.add_argument("-o", "--output_directory", required=True, type=str, help="Directory path to save the JSON files")
    parser.add_argument("-n", "--num_replica", required=False, type=int, default=100, help="Number of replicas for each object")

    args = parser.parse_args()

    extract_metadata(args.input_directory, args.output_directory , args.num_replica)
