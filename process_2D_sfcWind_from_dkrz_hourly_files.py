#!/usr/bin/env python

# -------------------------------------------------
# Getting libraries and utilities
# -------------------------------------------------
import calendar
import numpy as np
import pandas as pd
import json
import logging
import os
from datetime import datetime
import argparse
import subprocess
import time
from pathlib import Path
import xarray as xr
from cdo import Cdo
from datetime import datetime

from functions.file_util import read_era5_info
from functions.read_config import read_yaml_config
from functions.general_functions import *

cdo = Cdo()

# -------------------------------------------------
# Create a simple logger
# -------------------------------------------------

# Define logfile and logger
seconds = time.time()
local_time = time.localtime(seconds)
# Name the logfile after first of all inputs
LOG_FILENAME = (
    f"logfiles/logging_sfcWind_ERA5_dkrz"
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
# Define functions
# -------------------------------------------------


def compute_wind_from_u_v(u_comp, v_comp):
    '''
    Calculates Wind Speed from U and V components

    Input:
    u_comp: U Wind component
    v_comp: V Wind component

    Returns:
    Wind speed in m s-1
    '''

    u_square = np.square(u_comp)
    v_square = np.square(v_comp)
    uv_sum = np.add(u_square, v_square)

    sfcW = np.sqrt(uv_sum)

    return sfcW


def calc_wind_daily(infile1, info1, infile2, info2, wind_file, year, month, day_str):
    ds_1 = xr.open_dataset(infile1, mask_and_scale=True)
    ds_2 = xr.open_dataset(infile2, mask_and_scale=True)

    logger.info('Calculating variable sfcWind')
    da_hourly = compute_wind_from_u_v(ds_1[info1["short_name"]], ds_2[info2["short_name"]])

    da_daily = da_hourly.mean(dim='time')

    return da_daily


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
    freq = config['variables']['freq']
    family = config['variables']['family']
    level = config['variables']['level']
    varout = config['variables']['varout1']
    var1 = config['variables']['var1']
    var2 = config['variables']['var2']

    logger.info(f"Variable to be processed: {var1} and {var2}, output variable: {varout}")

    # configured paths
    origin = config['paths']['origin']
    download_path = config['paths']['download']
    work_all_path = config['paths']['work']
    proc_path = config['paths']['proc']
    work_path = f"{work_all_path}/{varout}/"
    grib_path = f"{download_path}/{varout}/"

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

    standard_name="wind_speed"
    long_name="Near-Surface Wind Speed"
    unit="m s-1"

    dict_attr ={"standard_name": standard_name, "long_name": long_name, "units": unit,
				"_FillValue": 1.e+20}


    # read ERA5_variables.json
    era5_info1 = read_era5_info(var1)

    logger.info("ERA5 variable info red from json file.")
    logger.info(f'longname: {era5_info1["long_name"]},')
    logger.info(f'unit: {era5_info1["unit"]},')
    logger.info(f'parameterid: {era5_info1["param"]},')

    era5_info2 = read_era5_info(var2)
    logger.info(f'longname: {era5_info2["long_name"]},')
    logger.info(f'unit: {era5_info2["unit"]},')
    logger.info(f'parameterid: {era5_info2["param"]},')

    for year in range(startyr, endyr + 1):
        logger.info(f"Processing year {year}.")

        logger.info(f"Copying variable {var1} and {var2}.")
        download_file1 = download_data_dkrz(freq, era5_info1, origin, grib_path, year, months, all_months, family, level)
        download_file2 = download_data_dkrz(freq, era5_info2, origin, grib_path, year, months, all_months, family, level)

        proc_archive = f'{proc_path}/{varout}/day/native/{year}'
        os.makedirs(proc_archive, exist_ok=True)

        for month in months:
            outfile = (
                f'{proc_archive}/{varout}_day_era5_{year}{month}.nc'
            )

            num_days = calendar.monthrange(year, int(month))[1]
            days = [*range(1, num_days + 1)]

            wind_file = f'{work_path}/{varout}_{freq}_era5_{year}{month}'

            daily_list=list()
            for day in days:
                day_str = f"{day:02d}"

                grib_file1 = download_file1.replace("MM-DD", f"{month}-{day_str}")

                tmp_outfile1 = convert_netcdf_add_era5_info(
                    grib_file1, work_path, era5_info1, year, month, day_str
                )

                grib_file2 = download_file2.replace("MM-DD", f"{month}-{day_str}")

                tmp_outfile2 = convert_netcdf_add_era5_info(
                    grib_file2, work_path, era5_info2, year, month, day_str
                )

                daily_wind = calc_wind_daily(tmp_outfile1, era5_info1, tmp_outfile2, era5_info2, wind_file, year, month, day_str)
                print(daily_wind)
                daily_list.append(daily_wind)

            # concatenate daily files
            daily_file = f"{work_path}/{varout}_day_era5_{year}{month}.nc"
            da_daily_wind = xr.concat(daily_list, dim='time')

            dates = pd.date_range(start=f'{year}-{month}-01', periods=num_days, freq='D')

            ds_out = da_daily_wind.assign_attrs(dict_attr).assign_coords(time=dates).to_dataset(name=varout)

            ds_out.to_netcdf(daily_file, encoding={varout: {"chunksizes": (config["chunking"]["time_chk"], config["chunking"]["lat_chk"], config["chunking"]["lon_chk"])}})
            logger.info("Data written to %s", daily_file)

            outfile_name = convert_era5_to_cmip(
                daily_file, store, proc_archive, era5_info, dataname, year, month, config["chunking"]["time_chk"], config["chunking"]["lat_chk"], config["chunking"]["lon_chk"]
            )

            logger.info(f"File {outfile_name} written.")

            # calculate monthly mean
            outfile_mon = calc_mon_mean(proc_archive, outfile_name)
            logger.info(f"File {outfile_mon} written.")

        # -------------------------------------------------
        # Clean up
        # -------------------------------------------------
        os.system(f"rm {work_path}/{varout}_*")
        os.system(f"rm {work_path}/{var}_*")
        os.system(f"rm {grib_path}/*")


if __name__ == "__main__":
    main()
