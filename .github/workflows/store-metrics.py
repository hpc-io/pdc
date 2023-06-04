import os
import csv
import sys
import glob
import gspread

import numpy as np
import pandas as pd

from datetime import date
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

SPREADSHEET_ID = os.getenv('GOOGLE_SPREADSHEET_ID')
CREDENTIALS = os.getenv('GOOGLE_CREDENTIALS')

dataset = None

scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

credentials = Credentials.from_service_account_file(
    CREDENTIALS,
    scopes=scopes
)

gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

gs = gc.open_by_key(SPREADSHEET_ID)

obj_create_time = np.zeros(6)
xfer_create_time = np.zeros(6)
xfer_start_time = np.zeros(6)
xfer_wait_time = np.zeros(6)
xfer_close_time = np.zeros(6)
obj_close_time = np.zeros(6)
total_time = np.zeros(6)
server_close_time = np.zeros(6)

xfer_create_time_node = []
xfer_start_time_node = []
xfer_wait_time_node = []
xfer_close_time_node = []

obj_create_time_node = []
obj_close_time_node = []

sleep_time_node = []

xfer_create_time_file = []
xfer_start_time_file = []
xfer_wait_time_file = []
xfer_close_time_file = []

obj_create_time_file = []
obj_close_time_file = []

sleep_time_file = []

total_time_file = []
observed_time_file = []

today = date.today()

pdc_metadata_servers = None
pdc_metadata_clients = None

# Parsing

obj_create_time_file_agg = 0
obj_close_time_file_agg = 0

xfer_create_time_file_agg = 0
xfer_start_time_file_agg = 0
xfer_wait_time_file_agg = 0
xfer_close_time_file_agg = 0

sleep_time_file_agg = 0

total_time_file_agg = 0
observed_time_file_agg = 0

with open(sys.argv[2], 'r') as f:
    lines = f.readlines()

    for line in lines:
        if 'Obj create time' in line:
            t = float(line.split(':')[1])
            obj_create_time_file_agg += t
            total_time_file_agg += t
            observed_time_file_agg += t
            obj_create_time_node.append(t)
        elif 'Transfer create time' in line:
            t = float(line.split(':')[1])
            xfer_create_time_file_agg += t
            total_time_file_agg += t
            observed_time_file_agg += t
            xfer_create_time_node.append(t)                     
        elif 'Transfer start time' in line:
            t = float(line.split(':')[1])
            xfer_start_time_file_agg += t
            total_time_file_agg += t
            observed_time_file_agg += t
            xfer_start_time_node.append(t)
        elif 'Transfer wait time' in line:
            t = float(line.split(':')[1])
            xfer_wait_time_file_agg += t
            total_time_file_agg += t
            observed_time_file_agg += t
            xfer_wait_time_node.append(t)        
        elif 'Transfer close time' in line:
            t = float(line.split(':')[1])
            xfer_close_time_file_agg += t
            total_time_file_agg += t
            observed_time_file_agg += t
            xfer_close_time_node.append(t)
        elif 'Obj close time' in line:
            t = float(line.split(':')[1])
            obj_close_time_file_agg += t
            total_time_file_agg += t
            observed_time_file_agg += t
            obj_close_time_node.append(t)
        elif 'Sleep time' in line:
            t = float(line.split(':')[1])
            sleep_time_file_agg += t
            observed_time_file_agg += t
            sleep_time_node.append(t)

        if 'PDC Metadata servers, running with' in line and (pdc_metadata_servers is None or pdc_metadata_clients is None):
            parts = line.split()
            
            pdc_metadata_servers = parts[2]
            pdc_metadata_clients = parts[8]

    obj_create_time_file.append(obj_create_time_file_agg)
    obj_close_time_file.append(obj_close_time_file_agg)

    xfer_create_time_file.append(xfer_create_time_file_agg)
    xfer_start_time_file.append(xfer_start_time_file_agg)
    xfer_wait_time_file.append(xfer_wait_time_file_agg)
    xfer_close_time_file.append(xfer_close_time_file_agg)

    sleep_time_file.append(sleep_time_file_agg)

    total_time_file.append(total_time_file_agg)
    observed_time_file.append(observed_time_file_agg)

if len(obj_create_time_node) > 0:
    observations = {
        'pdc_metadata_servers': pdc_metadata_servers,
        'pdc_metadata_clients': pdc_metadata_clients,

        'date': str(today),

        'obj_create_time_min_file': np.min(obj_create_time_file) if obj_create_time_file else None,
        'xfer_create_time_min_file': np.min(xfer_create_time_file) if xfer_create_time_file else None,
        'xfer_start_time_min_file': np.min(xfer_start_time_file) if xfer_start_time_file else None,
        'xfer_wait_time_min_file': np.min(xfer_wait_time_file) if xfer_wait_time_file else None,
        'xfer_close_time_min_file': np.min(xfer_close_time_file) if xfer_close_time_file else None,
        'obj_close_time_min_file': np.min(obj_close_time_file) if obj_close_time_file else None,
        'sleep_time_min_file': np.min(sleep_time_file) if sleep_time_file else None,

        'obj_create_time_median_file': np.median(obj_create_time_file) if obj_create_time_file else None,
        'xfer_create_time_median_file': np.median(xfer_create_time_file) if xfer_create_time_file else None,
        'xfer_start_time_median_file': np.median(xfer_start_time_file) if xfer_start_time_file else None,
        'xfer_wait_time_median_file': np.median(xfer_wait_time_file) if xfer_wait_time_file else None,
        'xfer_close_time_median_file': np.median(xfer_close_time_file) if xfer_close_time_file else None,
        'obj_close_time_median_file': np.median(obj_close_time_file) if obj_close_time_file else None,
        'sleep_time_median_file': np.median(sleep_time_file) if sleep_time_file else None,

        'obj_create_time_max_file': np.max(obj_create_time_file) if obj_create_time_file else None,
        'xfer_create_time_max_file': np.max(xfer_create_time_file) if xfer_create_time_file else None,
        'xfer_start_time_max_file': np.max(xfer_start_time_file) if xfer_start_time_file else None,
        'xfer_wait_time_max_file': np.max(xfer_wait_time_file) if xfer_wait_time_file else None,
        'xfer_close_time_max_file': np.max(xfer_close_time_file) if xfer_close_time_file else None,
        'obj_close_time_max_file': np.max(obj_close_time_file) if obj_close_time_file else None,
        'sleep_time_max_file': np.max(sleep_time_file) if sleep_time_file else None,

        'obj_create_time_min_node': np.min(obj_create_time_node) if obj_create_time_node else None,
        'xfer_create_time_min_node': np.min(xfer_create_time_node) if xfer_create_time_node else None,
        'xfer_start_time_min_node': np.min(xfer_start_time_node) if xfer_start_time_node else None,
        'xfer_wait_time_min_node': np.min(xfer_wait_time_node) if xfer_wait_time_node else None,
        'xfer_close_time_min_node': np.min(xfer_close_time_node) if xfer_close_time_node else None,
        'obj_close_time_min_node': np.min(obj_close_time_node) if obj_close_time_node else None,
        'sleep_time_min_node': np.min(sleep_time_node) if sleep_time_node else None,

        'obj_create_time_median_node': np.median(obj_create_time_node) if obj_create_time_node else None,
        'xfer_create_time_median_node': np.median(xfer_create_time_node) if xfer_create_time_node else None,
        'xfer_start_time_median_node': np.median(xfer_start_time_node) if xfer_start_time_node else None,
        'xfer_wait_time_median_node': np.median(xfer_wait_time_node) if xfer_wait_time_node else None,
        'xfer_close_time_median_node': np.median(xfer_close_time_node) if xfer_close_time_node else None,
        'obj_close_time_median_node': np.median(obj_close_time_node) if obj_close_time_node else None,
        'sleep_time_median_node': np.median(sleep_time_node) if sleep_time_node else None,

        'obj_create_time_max_node': np.max(obj_create_time_node) if obj_create_time_node else None,
        'xfer_create_time_max_node': np.max(xfer_create_time_node) if xfer_create_time_node else None,
        'xfer_start_time_max_node': np.max(xfer_start_time_node) if xfer_start_time_node else None,
        'xfer_wait_time_max_node': np.max(xfer_wait_time_node) if xfer_wait_time_node else None,
        'xfer_close_time_max_node': np.max(xfer_close_time_node) if xfer_close_time_node else None,
        'obj_close_time_max_node': np.max(obj_close_time_node) if obj_close_time_node else None,
        'sleep_time_max_node': np.max(sleep_time_node) if sleep_time_node else None,

        'total_time_min_file': np.min(total_time_file) if total_time_file else None,
        'total_time_median_file': np.median(total_time_file) if total_time_file else None,
        'total_time_max_file': np.max(total_time_file) if total_time_file else None,

        'observed_time_min_file': np.min(observed_time_file) if observed_time_file else None,
        'observed_time_median_file': np.median(observed_time_file) if observed_time_file else None,
        'observed_time_max_file': np.max(observed_time_file) if observed_time_file else None
    }

    # dataset = pd.DataFrame.from_dict([observations])

    # print(dataset)

    # dataset.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # df_values = dataset.values.tolist()

    # if False:
    #     # Submit the header to the spreadsheet
    #     gs.values_append(
    #         sys.argv[1],
    #         {
    #             'valueInputOption': 'USER_ENTERED'
    #         },
    #         {
    #             'values': [dataset.columns.tolist()]
    #         }
    #     )

    # gs.values_append(
    #     sys.argv[1],
    #     {
    #         'valueInputOption': 'USER_ENTERED'
    #     },
    #     {
    #         'values': df_values
    #     }
    # )

    lines = []

    # Record all the steps
    for step in range(0, len(obj_create_time_node)):
        line = {
            'branch': sys.argv[2],
            'JOBID': sys.argv[3],

            'pdc_metadata_servers': pdc_metadata_servers,
            'pdc_metadata_clients': pdc_metadata_clients,

            'date': str(today),

            'step': step + 1,

            'obj_create_time_step': obj_create_time_node[step],
            'xfer_create_time_step': xfer_create_time_node[step],
            'xfer_start_time_step': xfer_start_time_node[step],
            'xfer_wait_time_step': xfer_wait_time_node[step],
            'xfer_close_time_step': xfer_close_time_node[step],
            'obj_close_time_step': obj_close_time_node[step],
            'sleep_time_step': sleep_time_node[step] if len(sleep_time_node) < step else 0
        }

        lines.append(line)

    dataset = pd.DataFrame.from_dict(lines)

    print(dataset)

    dataset.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    df_values = dataset.values.tolist()

    if False:
        # Submit the header to the spreadsheet
        gs.values_append(
            '{} - Steps'.format(sys.argv[1]),
            {
                'valueInputOption': 'USER_ENTERED'
            },
            {
                'values': [dataset.columns.tolist()]
            }
        )

    gs.values_append(
        '{} - Steps'.format(sys.argv[1]),
        {
            'valueInputOption': 'USER_ENTERED'
        },
        {
            'values': df_values
        }
    )
