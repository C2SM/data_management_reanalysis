#!/usr/bin/env python

# -------------------------------------------------
# Getting libraries and utilities
# -------------------------------------------------
import json
import logging
import os
import sys
import subprocess
import argparse
from datetime import datetime
from cdo import Cdo

import xarray as xr

from functions.file_util import read_config, read_era5_info
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
    f"logfiles/logging_ERA5_dkrz_pl_daily"
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


def convert_cc(cc_outfile, workdir, era5_info, year, month):

    cdo.mulc(100, input=cc_outfile, output=f"{workdir}/{era5_info['short_name']}_era5_{year}{month}_mulc.nc", options="-b F64")
    os.system(f'rm {workdir}/{era5_info["short_name"]}_era5_{year}{month}.nc')
    os.system(
        f'ncatted -a units,{era5_info["short_name"]},m,c,"{era5_info["cmip_unit"]}" {workdir}/{era5_info["short_name"]}_era5_{year}{month}_mulc.nc {cc_outfile}'
    )
    try:
        cmd = [
            "ncatted",
            "-a", f'units,{era5_info["short_name"]},m,c,"{era5_info["cmip_unit"]}"',
            f"{workdir}/{era5_info['short_name']}_era5_{year}{month}_mulc.nc",
            f"{cc_outfile}"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with return code {e.returncode}")
        print(f"Error message: {e.stderr}")
        sys.exit(1)

    return cc_outfile


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
    parser.add_argument(
        "-v",
        "--varname",
        help="Name of the variable to be processed",
        required=True,
    )
    args = parser.parse_args()

    configname = args.configname
    var = args.varname
    logger.info(f"Config name is: {configname}")
    logger.info(f"Variable name is: {var}")

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
    print(f"Variable to be processed: {var}")

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
    print(f"Months to be processed: {months}")

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
    era5_info = read_era5_info(var)
    logger.info("ERA5 variable info red from json file.")
    logger.info(f'longname: {era5_info["long_name"]},')
    logger.info(f'unit: {era5_info["unit"]},')
    logger.info(f'oldname: {era5_info["param"]},')
    logger.info(f'cmipname: {era5_info["cmip_name"]},')
    logger.info(f'cmipunit: {era5_info["cmip_unit"]}.')

    for year in range(startyr, endyr + 1):
        logger.info(f"Processing year {year}.")

        if store == 'dkrz':
            download_file = download_data_dkrz(
                freq=freq, era5_info=era5_info, origin=origin, iac_path=grib_path, year=year, months=months, all_months=all_months, family=family, level=level)
            download_success = f"Data download successful!"
        else:
            download_success = f"Warning, download from store {store} not implemented."
        logger.info(download_success)
        print(download_file)

        proc_archive = f'{proc_path}/{era5_info["cmip_name"]}/day/native/{year}'
        os.makedirs(proc_archive, exist_ok=True)

        for month in months:
            grib_file = (
                f'{grib_path}/E5pl00_1D_{year}-{month}_{era5_info["param"]}.grb'
            )
            outfile = (
                f'{proc_archive}/{era5_info["cmip_name"]}_day_{dataname}_{year}{month}.nc'
            )

            tmp_outfile = convert_netcdf_add_era5_info(
                grib_file, work_path, era5_info, dataname, year, month
            )

            # check if unit needs to be changed from era5 variable to cmip variable
            if era5_info["unit"] != era5_info["cmip_unit"]:
                if var == "cc":
                    logger.info(
                        f'Unit for cc needs to be changed from {era5_info["unit"]} to {era5_info["cmip_unit"]}.'
                    )

                    tmp_outfile = convert_cc(
                        tmp_outfile, work_path, era5_info, dataname, year, month
                    )
                else:
                    logger.error(
                        f"Conversion of unit for variable {var} is not implemented!"
                    )
                    sys.exit(1)

            outfile_name = convert_era5_to_cmip_plev(
                tmp_outfile, outfile, work_path, era5_info, year, month, config['chunking']['lat_chk'], config['chunking']['lon_chk']
            )

            logger.info(f"File {outfile_name} written.")

            # calculate monthly mean
            proc_mon_archive = proc_archive.replace("day", "mon")
            os.makedirs(proc_mon_archive, exist_ok=True)
            outfile_mon = outfile_name.replace('day', 'mon')
            cdo.monmean(input=outfile_name, output=outfile_mon)

        # -------------------------------------------------
        # Clean up
        # -------------------------------------------------
        os.system(f"rm {work_path}/{var}_*")
    os.system(f'rm {grib_path}/*')


if __name__ == "__main__":
    main()
