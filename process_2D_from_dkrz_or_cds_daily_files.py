#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File Name : process_2D_frm_dkrz_daily_files.py
Author: Ruth Lorenz (ruth.lorenz@c2sm.ethz.ch)
Created: 08/12/2023
Purpose: process ERA5 data downloaded from dkrz
        to variable names and units as in cmip
        env: cdsapi_10_2024
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
    f"logfiles/logging_ERA5_dkrz_cds_daily"
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

    # configured paths
    origin = config['paths']['origin']
    download_path = config['paths']['download']
    work_all_path = config['paths']['work']
    proc_path = config['paths']['proc']
    work_path = f"{work_all_path}/{var}/"
    grib_path = f"{download_path}/{var}/"

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
    # Create directories if do not exist yet
    # -------------------------------------------------
    os.makedirs(work_path, exist_ok=True)
    os.makedirs(proc_path, exist_ok=True)

    # -------------------------------------------------
    # read ERA5_variables.json
    # -------------------------------------------------
    logger.info(f"{config['dataset']['name']} variable info red from json file.")
    era5_info = read_era5_info(var)

    logger.info(f"Copying variable {var}")
    # download and process for all years in configuration
    for year in range(startyr, endyr + 1):
        logger.info(f"Processing year {year}, month(s) {months}.")

        print(store)
        if store == 'dkrz':
            download_file = download_data_dkrz(
                freq=freq, era5_info=era5_info, origin=origin, iac_path=grib_path, year=year, months=months, all_months=all_months, family=family, level=level)
            download_success = f"Data download successful!"
        elif store == 'cds':
            download_file = download_data_cds(
                dataname=dataname, era5_info=era5_info, origin=origin, workdir=work_path, year=year, months=months, overwrite=overwrite)
            download_success = f"Data download successful!"
        else:
            download_success = f"Warning, download from store {store} not implemented."
        logger.info(download_success)


        proc_archive = f'{proc_path}/{era5_info["cmip_name"]}/day/native/{year}'
        os.makedirs(proc_archive, exist_ok=True)
        proc_mon_archive = proc_archive.replace("day", "mon")
        os.makedirs(proc_mon_archive, exist_ok=True)

        for month in months:

            #grib_file = f'{grib_path}{family}{level}{typeid}_{freq}_{year}-{month}_{vparam}.grb'
            file = download_file.replace("MM", month)
            if file.endswith('.grb'):
                logger.info(f"Converting grib file {file} to netcdf and adding ERA5 variable info.")
                tmp_outfile = convert_netcdf_add_era5_info(
                    file, work_path, era5_info, dataname, year, month
                )
            else:
                logger.info(f"File {file} is not a grib file, skipping conversion to netcdf, convert valid_time to time and lon lat info.")
                tmp_outfile = convert_valid_time_latitude_longitude(
                    file, work_path, era5_info, dataname, year, month)

            # check if tmp_outfile was created successfully
            if not os.path.isfile(f"{tmp_outfile}"):
                logger.error(f"Output file {tmp_outfile} was not created successfully.")
                sys.exit(1)

            # check if unit needs to be changed from era5 variable to cmip variable
            if era5_info["unit"] != era5_info["cmip_unit"]:
                logger.info(
                    f'Unit for {era5_info["short_name"]} needs to be changed from {era5_info["unit"]} to {era5_info["cmip_unit"]}.'
                )
                if var == "tcc":
                    tmp_outfile = convert_tcc(
                        tmp_outfile, work_path, era5_info, dataname, year, month
                    )
                elif var == "tp":
                    tmp_outfile = convert_tp(
                        tmp_outfile, work_path, era5_info, dataname, year, month
                    )
                elif var == "ssrd" or var == "strd" or var == "str" or var == "ssr":
                    tmp_outfile = convert_radiation(
                        tmp_outfile, work_path, era5_info, dataname, year, month
                    )
                else:
                    logger.error(
                        f"Conversion of unit for variable {var} is not implemented!"
                    )
                    sys.exit(1)
                if not os.path.isfile(f"{tmp_outfile}"):
                    logger.error(f"Output file {tmp_outfile} atfer unit conversion was not created successfully.")
                    sys.exit(1)

            outfile_name = convert_era5_to_cmip(
                tmp_outfile, store, proc_archive, era5_info, dataname, year, month,
                config['chunking']['time_chk'], config['chunking']['lon_chk'], config['chunking']['lat_chk']
            )
            if not os.path.isfile(f"{outfile_name}"):
                logger.error(f"Output file {outfile_name} after conversion to cmip format was not created successfully.")
                sys.exit(1)
            else:
                logger.info(f"File {outfile_name} written successfully.")

            # calculate monthly mean
            outfile_mon = outfile_name.replace("day", "mon")
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
            if not os.path.isfile(f"{outfile_mon}"):
                logger.error(f"Output file {outfile_mon} after conversion to cmip format was not created successfully.")
                sys.exit(1)
            else:
                logger.info(f"File {outfile_mon} written successfully.")

        # -------------------------------------------------
        # Clean up
        # -------------------------------------------------
        #os.system(f"rm {work_path}/{var}_*")
        #os.system(f"rm {work_path}/tmp*")
        if store == 'dkrz':
            os.system(f"rm {grib_path}/*")



if __name__ == "__main__":
    main()
