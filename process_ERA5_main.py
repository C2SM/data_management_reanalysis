#!/usr/bin/env python3
import argparse

from configuration import era5_config
from functions.file_util import get_checkpoint

vars_2D_1D = ["t2m", "tp"]
vars_2D_maxmin = ["tasmax", "tasmin"]
vars_2D_1H = ["sfcWind"]
vars_2D_calc = ["hurs", "rlds"]

vars_3D_1D = ["q", "r", "t", "u", "v", "cc"]
vars_3D_1H = ["z"]

def main():
    parser = argparse.ArgumentParser(
        description="Download and process ERA5 data from DKRZ and CDS."
    )
    parser.add_argument(
    "--dry-run",
    action="store_true",
    default=False,
    help="Perform a dry run without executing commands"
    )

    args = parser.parse_args()

    dry_run = args.dry_run

    config = era5_config()

    year = config['case']['year']
    month = config['case']['month']
    var = config['case']['ShortName']
    overwrite = config['case']['overwrite']

    download_dir = config['paths']['download_dir']
    end_dir = config['paths']['end_dir']
    work_dir = config['paths']['work_dir']

    time_chk = config['chunking']['time_chk']
    lat_chk = config['chunking']['lat_chk']
    lon_chk = config['chunking']['lon_chk']

    # -------------------------------------------------
    # Create directories if do not exist yet
    # -------------------------------------------------
    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(end_dir, exist_ok=True)
    os.makedirs(work_dir, exist_ok=True)

    if var in vars_2D_1D:
        proc_var_2D = process_2D_from_dkrz_daily_files(var, year, month, config)

    if var in vars_3D_1D:
        proc_var_3D = process_3D_from_dkrz_daily_files(config)


    # Write empty start job stamp file
    checkpoint = get_checkpoint(work_dir)
    startstamp = f'{checkpoint}/{year}_{month}_{var}.started'
    if not dry_run:
        with open(startstamp, 'w') as file:
            file.write("")

if __name__ == "__main__":
    main()