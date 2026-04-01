#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File Name : process_2D_maxmin_from_cds_daily.py
Author: Ruth Lorenz (ruth.lorenz@c2sm.ethz.ch)
Created: 06/02/2026
Purpose: process ERA5, ERA5-Land data downloaded from cdsapi
        to cmip like format, with renaming of variables and dimensions,
        and conversion of units if necessary.
        The script is designed to be flexible and can be adapted
        to variable names and units as in cmip
        env: iacpy3_2025
"""

# -------------------------------------------------
# Getting libraries and utilities
# -------------------------------------------------
import logging
import os
import subprocess
import sys
import time
import argparse
from datetime import datetime
from pathlib import Path
import cdsapi
import xarray as xr
from functions.file_util import read_era5_info
from functions.read_config import read_yaml_config
from functions.general_functions import *

# -------------------------------------------------
# Create a simple logger
# -------------------------------------------------

# Define logfile and logger
seconds = time.time()
local_time = time.localtime(seconds)
# Name the logfile after first of all inputs
LOG_FILENAME = (
    f"logfiles/logging_ERA5-Land_cds_daily_stats"
    f"_{local_time.tm_year}{local_time.tm_mon}"
    f"{local_time.tm_mday}{local_time.tm_hour}{local_time.tm_min}"
    f".out"
)

logging.basicConfig(
    filename=LOG_FILENAME,
    filemode="w",
    format="%(asctime)s | %(levelname)s : %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# -------------------------------------------------


def main():
    # -------------------------------------------------
    # Parse command line input
    # -------------------------------------------------
    parser = argparse.ArgumentParser(
        description="Download ERA5 data and process to CMIP like"
    )
    parser.add_argument(
        "-v",
        "--var",
        help="ERA5 variable to be processed, e.g. 2t, tcc, tp, ssrd, strd, str, ssr",
        required=True,
    )
    parser.add_argument(
        "-c",
        "--configname",
        help="Name of the config yaml file",
        required=True,
    )
    args = parser.parse_args()

    configname = args.configname

    # -------------------------------------------------
    # Read config
    # -------------------------------------------------

    config = read_yaml_config(configname)
    logger.info(f"Read configuration as {config}")

    store = config['dataset']['store']
    dataname = config['dataset']['name'].lower()

    # variable to be processed
    var = args.var
    freq = config['variables']['freq']
    family = config['variables']['family']
    level = config['variables']['level']
    statistic = config['variables']['statistic']

    # configured paths
    origin = config['paths']['origin']
    download_path = config['paths']['download']
    work_all_path = config['paths']['work']
    proc_path = config['paths']['proc']

    # time span to download and process
    startyr = config['time']['startyr']
    endyr = config['time']['endyr']

    # months is optional, can be one month or list of months
    try:
        c_months = config['time']['months']
        months = convert_month_list(c_months)

        all_months = False
    except KeyError:
        months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
        all_months = True

    # overwrite files, download from CDS if already exists and reprocess
    overwrite = config['flags']['overwrite']

    # -------------------------------------------------
    # read ERA5_variables.json
    # -------------------------------------------------
    logger.info(f"{config['dataset']['name']} variable info red from json file.")
    era5_info = read_era5_info(var)
    if statistic != "daily_mean":
        if var == "2t":
            varout = "tasmax" if statistic == "daily_maximum" else "tasmin"
            era5_info["cmip_name"] = varout
            logger.info(f'cmip_name: {era5_info["cmip_name"]},')
        else:
            logger.error(f"Variable {var} with statistic {statistic} not implemented yet.")
            sys.exit(1)

    work_path = f"{work_all_path}/{varout}/"
    grib_path = f"{download_path}/{varout}/"

    # -------------------------------------------------
    # Create directories if do not exist yet
    # -------------------------------------------------
    os.makedirs(work_path, exist_ok=True)
    os.makedirs(proc_path, exist_ok=True)

    # download and process for all years in configuration
    for year in range(startyr, endyr + 1):
        logger.info(f"Processing year {year}.")
        logger.info(f"Copying variable {var}")

        print(store)
        if store == 'cds':
            download_file = download_data_cds(
                dataname=dataname, era5_info=era5_info, origin=origin, workdir=work_path, year=year, months=months, overwrite=overwrite, statistic=statistic)
            download_success = f"Data download successful!"
        else:
            download_success = f"Warning, download from store {store} not implemented."
        logger.info(download_success)
        print(download_file)

        proc_archive = f'{proc_path}/{varout}/day/native/{year}'
        os.makedirs(proc_archive, exist_ok=True)
        proc_mon_archive = proc_archive.replace("day", "mon")
        os.makedirs(proc_mon_archive, exist_ok=True)

        # process the downloaded file to cmip like format
        for month in months:
            file = download_file.replace("MM", month)
            tmp_outfile = convert_valid_time_latitude_longitude(
                    file, work_path, era5_info, dataname, year, month)
            print(tmp_outfile)
            outfile_name = convert_era5_to_cmip(
                tmp_outfile, store, proc_archive, era5_info, dataname, year, month,
                config['chunking']['time_chk'], config['chunking']['lon_chk'], config['chunking']['lat_chk']
            )
            logger.info(f"File {outfile_name} written.")
            os.remove(tmp_outfile)
            logger.info(f"Temporary file {tmp_outfile} removed.")

            # calculate monthly mean
            outfile_mon = outfile_name.replace("day", "mon")
            print(outfile_mon)
            try:
                cmd = [
                    "cdo",
                    "monmean",
                    f'{outfile_name}',
                    f'{outfile_mon}'
                ]
                subprocess.run(cmd, check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError as e:
                print(f"Command failed with return code {e.returncode}")
                print(f"Standard output:\n{e.stdout}")

        # -------------------------------------------------
        # Clean up
        # -------------------------------------------------
        os.system(f"rm {work_path}/{var}_*")
        os.system(f"rm {work_path}/tmp*")
        logger.info(f"Clean up of work directory {work_path} done.")


if __name__ == "__main__":
    main()