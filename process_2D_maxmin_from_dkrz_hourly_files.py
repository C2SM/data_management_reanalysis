#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File Name : process_2D_maxmin_from_dkrz_hourly_files.py
Author: Ruth Lorenz (ruth.lorenz@c2sm.ethz.ch)
Created: 08/12/2023
Purpose: process ERA5 data downloaded from dkrz hourly files to daily max/min and
         convert to variable names and units as in cmip
        env: cdsapi_10_2024
"""

# -------------------------------------------------
# Getting libraries and utilities
# -------------------------------------------------
import calendar
import json
import logging
import time
import argparse
import os
import subprocess
from datetime import datetime

import xarray as xr

from functions.file_util import read_config, read_era5_info
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
    f"logfiles/logging_maxmin_ERA5_dkrz"
    f"_{local_time.tm_year}{local_time.tm_mon}"
    f"{local_time.tm_mday}{local_time.tm_hour}{local_time.tm_min}"
    f".out"
)

logging.basicConfig(
    filename=LOG_FILENAME,
    filemode="w",
    format="%(levelname)s %(asctime)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# -------------------------------------------------
# Define functions
# -------------------------------------------------

def calc_minmax(infile, minmax_file, dayagg, year, month, day_str):
    try:
        cmd = [
            "cdo",
            f"{dayagg}",
            f"{infile}.nc",
            f"{minmax_file}{day_str}.nc"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with return code {e.returncode}")
        print(f"Standard output:\n{e.stdout}")
        print(f"Standard error:\n{e.stderr}")

    if not os.path.isfile(
        f"{minmax_file}{day_str}.nc"
    ):
        logger.warning(
            f"{minmax_file}{day_str}.nc was not processed properly!"
        )
    else:
        # clean up 1-hr data
        os.system(f"rm {infile}.nc")

    return


def calc_mon_mean(path_work, infile, varout, year, month):
    proc_mon_work = (
        f'{path_work}/{varout}/mon/native/{year}'
    )
    os.makedirs(proc_mon_work, exist_ok=True)
    outfile_mon = (
        f'{proc_mon_work}/{varout}_mon_era5_{year}{month}.nc'
    )

    try:
        cmd = [
            "cdo",
            "monmean",
            f"{infile}",
            f"{outfile_mon}"
        ]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with return code {e.returncode}")
        print(f"Standard output:\n{e.stdout}")

    return outfile_mon

# -------------------------------------------------


def main():
    # -------------------------------------------------
    # Parse command line input
    # -------------------------------------------------
    parser = argparse.ArgumentParser(
        description="Download ERA5 data and process to CMIP like"
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
    var = config['variables']['varname']
    freq = config['variables']['freq']
    family = config['variables']['family']
    level = config['variables']['level']
    varout_list = [config['variables']['varout1'], config['variables']['varout2']]
    print(f"Variable to be processed: {var}, output variables: {varout_list}")

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

    for v, varout in enumerate(varout_list):
        logger.info(f'Processing variable {varout} from {var}:')

        # -------------------------------------------------
        # read ERA5_variables.json
        # -------------------------------------------------
        era5_info = read_era5_info(var)
        logger.info("ERA5 variable info red from json file.")
        logger.info(f'longname: {era5_info["long_name"]},')
        logger.info(f'unit: {era5_info["unit"]},')
        logger.info(f'oldname: {era5_info["param"]},')
        logger.info(f'cmipname: {era5_info["cmip_name"]},')
        logger.info(f'cmipunit: {era5_info["cmip_unit"]}.')

        # calculate daily max or min?
        if "max" in varout:
            dayagg = "daymax"
        elif "min" in varout:
            dayagg = "daymin"

        for year in range(startyr, endyr + 1):
            logger.info(f"Processing year {year}.")

            print(store)
            if store == 'dkrz':
                download_file = download_data_dkrz(
                    freq=freq, era5_info=era5_info, origin=origin, iac_path=grib_path, year=year, months=months, all_months=all_months, family=family, level=level)
                download_success = f"Data download successful!"
            else:
                download_success = f"Warning, download from store {store} not implemented."
            logger.info(download_success)
            print(download_file)


            proc_archive = f'{proc_path}/{varout}/day/native/{year}'
            os.makedirs(proc_archive, exist_ok=True)

            proc_mon_archive = proc_archive.replace("day", "mon")
            os.makedirs(proc_mon_archive, exist_ok=True)

            for month in months:
                logger.info(f"Processing month {month}.")

                outfile = (
                    f'{proc_archive}/{varout}_day_era5_{year}{month}.nc'
                )

                num_days = calendar.monthrange(year, int(month))[1]
                days = [*range(1, num_days + 1)]

                minmax_file = f'{work_path}/{var}_{dayagg}_era5_{year}{month}'

                for day in days:
                    day_str = f"{day:02d}"

                    grib_file = f'{grib_path}/E5sf00_{freq}_{year}-{month}-{day_str}_{era5_info["param"]}.grb'

                    tmp_outfile = convert_netcdf_add_era5_info(
                        grib_file, work_path, era5_info, dataname, year, month, day_str
                    )

                    calc_minmax(tmp_outfile, minmax_file, dayagg, year, month, day_str)

                # concatenate daily files
                daily_file = f"{work_path}/{varout}_day_era5_{year}{month}.nc"
                try:
                    cmd = [
                        "cdo",
                        "-b",
                        "F64",
                        "mergetime",
                        f"{minmax_file}*.nc",
                        f"{daily_file}"
                    ]
                    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
                except subprocess.CalledProcessError as e:
                    print(f"Command failed with return code {e.returncode}")
                    print(f"Standard output:\n{e.stdout}")


                outfile_name = convert_era5_to_cmip(
                    daily_file, varout, outfile, work_path, era5_info, dataname,
                    year, month, config["chunking"]["lat_chk"], config["chunking"]["lon_chk"]
                )

                logger.info(f"File {outfile_name} written.")

                # calculate monthly mean
                outfile_mon = calc_mon_mean(proc_path, outfile_name, varout, year, month)
                logger.info(f"File {outfile_mon} written.")

        # -------------------------------------------------
        # Clean up
        # -------------------------------------------------
        os.system(f"rm {work_path}/{varout}_*")
    os.system(f"rm {work_path}/{var}_*")
    os.system(f"rm {grib_path}/*")


if __name__ == "__main__":
    main()
