from pathlib import Path
from datetime import datetime

from functions.file_util import get_checkpoint

def era5_config():
    variables = ["t2m", "tp"]

    # TO DO: check which data exists, determine which months are new and can be downloaded
    year = "2024"
    month = "06"

    path = "/net/nitrogen/c2sm-scratch/rlorenz/era5_dkrz"
    path_proc = "/net/atmos/data/era5_cds/processed/v2"
    work_dir = "/net/nitrogen/c2sm-scratch/rlorenz/era5_dkrz/work"

    checkpoint = get_checkpoint(work_dir)

    for var in variables:
        # check if case has been run already or not
        start_file = Path(f'{checkpoint}/{year}_{month}_{var}.started')
        if start_file.is_file():
            print(f'{start_file} exists')
            continue
        else:
            config = {
                'case': {
                    'year': year,
                    'month': month,
                    'ShortName': var
                    'overwrite': False,
                },
                'paths': {
                    'download_dir': path,
                    'end_dir': path_proc,
                    'work_dir': work_dir
                }
                'chunking':{
                    'time_chk': 1,
                    'lat_chk': 46,
                    'lon_chk': 22
                }
            }
            return config
    print("No new cases found")
    exit()