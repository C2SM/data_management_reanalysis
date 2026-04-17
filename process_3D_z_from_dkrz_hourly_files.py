#!/usr/bin/env python

# -------------------------------------------------
# Getting libraries and utilities
# -------------------------------------------------
import calendar
import json
import logging
import argparse
import os
import subprocess
import sys
from datetime import datetime

import xarray as xr
from cdo import Cdo

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
    f"logfiles/logging_ERA5_dkrz_pl_z_hourly_to_daily"
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



def convert_z(tmp_outfile, workdir, era5_info, year, month, day_str):
    # https://confluence.ecmwf.int/display/CKB/ERA5%3A+compute+pressure+and+geopotential+on+model+levels%2C+geopotential+height+and+geometric+height#ERA5:computepressureandgeopotentialonmodellevels,geopotentialheightandgeometricheight-Geopotentialheight
    # Earth's gravitational acceleration [m/s2]
    const = 9.80665
    tmp2_outfile = f"{workdir}/{era5_info['short_name']}_era5_{year}{month}{day_str}_divc.nc"
    try:
        cdo.divc(const, input=tmp_outfile, output=tmp2_outfile)
    except RuntimeError as e:
        logger.error(f"CDO execution failed!")
        logger.error(f"Error details: {e}")
        sys.exit(1)
    os.system(f"rm {tmp_outfile}")
    #os.system(
    #    f'ncatted -a units,{era5_info["short_name"]},m,c,"{era5_info["cmip_unit"]}" {workdir}/{era5_info["short_name"]}_era5_{year}{month}{day_str}_divc.nc {tmp_outfile}'
    #)
    if not os.path.isfile(f"{tmp2_outfile}") or os.path.getsize(f"{tmp2_outfile}") == 0:
        logger.warning(
            f"{tmp2_outfile} was not created or is empty!"
        )
    else:
        logger.info(
            f"{tmp2_outfile} was processed successfully!"
        )
    try:
        cmd = [
            "ncatted",
            "-a",
            "units",
            era5_info["short_name"],
            "m",
            "c",
            era5_info["cmip_unit"],
            f"{tmp2_outfile}",
            f"{tmp_outfile}"
        ]
        subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with return code {e.returncode}")
        logger.error(f"Standard output:\n{e.stdout}")
        logger.error(f"Standard error:\n{e.stderr}")
        sys.exit(1)

    if not os.path.isfile(f"{tmp_outfile}") or os.path.getsize(f"{tmp_outfile}") == 0:
        logger.warning(
            f"{tmp_outfile} was not created or is empty!"
        )
    else:
        logger.info(
            f"{tmp_outfile} was processed successfully!"
        )

    return tmp_outfile


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
    logger.info(f"Config name is: {configname}")

    # -------------------------------------------------
    # Read config
    # -------------------------------------------------
    config = read_yaml_config(configname)
    logger.info(f"Read configuration as {config}")

    store = config['dataset']['store']
    dataname = config['dataset']['name'].lower()

    # variable to be processed
    var = args.varname
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

    # chunking
    lat_chk = config['chunking']['lat_chk']
    lon_chk = config['chunking']['lon_chk']

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
    era5_info = read_era5_info(var)
    logger.info("ERA5 variable info red from json file.")
    logger.info(f'longname: {era5_info["long_name"]},')
    logger.info(f'unit: {era5_info["unit"]},')
    logger.info(f'oldname: {era5_info["param"]},')
    logger.info(f'cmipname: {era5_info["cmip_name"]},')
    logger.info(f'cmipunit: {era5_info["cmip_unit"]}.')

    # read cmip standard_name and long_name from cmip6-cmor-tables
    standard_name, long_name = read_cmip_info(era5_info["cmip_name"])

    for year in range(startyr, endyr + 1):
        logger.info(f"Processing year {year}.")

        if store == 'dkrz':
            download_file = download_data_dkrz(
                freq=freq, era5_info=era5_info, origin=origin, iac_path=grib_path, year=year, months=months, all_months=all_months, family=family, level=level)
            download_success = f"Data download successful!"
        else:
            download_success = f"Warning, download from store {store} not implemented."
        logger.info(download_success)

        proc_archive = f'{proc_path}/{era5_info["cmip_name"]}/day/native/{year}'
        os.makedirs(proc_archive, exist_ok=True)

        for month in months:
            outfile = (
                f'{proc_archive}/{era5_info["cmip_name"]}_day_era5_{year}{month}.nc'
            )

            num_days = calendar.monthrange(year, int(month))[1]
            days = [*range(1, num_days + 1)]

            for day in days:
                day_str = f"{day:02d}"

                grib_file = f'{grib_path}/E5pl00_{freq}_{year}-{month}-{day_str}_{era5_info["param"]}.grb'

                tmp_outfile = convert_netcdf_add_era5_info(
                    grib_file, work_path, era5_info, year, month, day_str
                )

                # check if unit needs to be changed from era5 variable to cmip variable
                if era5_info["unit"] != era5_info["cmip_unit"]:
                    if var == "z":
                        logger.info(
                            f'Unit for z needs to be changed from {era5_info["unit"]} to {era5_info["cmip_unit"]}.'
                        )
                        tmp_outfile = convert_z(
                            tmp_outfile,
                            work_path,
                            era5_info,
                            year,
                            month,
                            day_str,
                        )
                    else:
                        logger.error(
                            f"Conversion of unit for variable {var} is not implemented!"
                        )
                        sys.exit(1)

                # calculate daily means
                outfile_daymean = f'{work_path}/{era5_info["short_name"]}_daymean_era5_{year}{month}{day_str}.nc'
                try:
                    cdo.daymean(input=tmp_outfile, output=outfile_daymean)
                except RuntimeError as e:
                    logger.error(f"CDO execution failed!")
                    logger.error(f"Error details: {e}")
                    sys.exit(1)

                if not os.path.isfile(outfile_daymean) or os.path.getsize(outfile_daymean) == 0:
                    logger.warning(
                        f"{outfile_daymean} was not created or is empty!"
                    )
                else:
                    logger.info(
                        f"{outfile_daymean} was processed successfully!"
                    )
                    # clean up 1-hr data
                    os.system(f"rm {tmp_outfile}")


            # concatenate daily files
            daily_file = f"{work_path}/{var}_day_era5_{year}{month}.nc"
            try:
                cdo.mergetime(f'{work_path}/{var}_daymean_era5_{year}{month}*.nc', daily_file)
            except RuntimeError as e:
                logger.error(f"CDO execution failed!")
                logger.error(f"Error details: {e}")
                sys.exit(1)


            outfile_name = convert_era5_to_cmip_plev(
                daily_file, outfile, work_path, era5_info, year, month, lat_chk, lon_chk
            )

            logger.info(f"File {outfile_name} written.")


            # calculate monthly mean
            outfile_mon = calc_mon_mean(proc_archive, outfile_name)
            logger.info(f"File {outfile_mon} written.")

        # -------------------------------------------------
        # Clean up
        # -------------------------------------------------
        os.system(f"rm {work_path}/{var}_*")
        os.system(f"rm {grib_path}/*")


if __name__ == "__main__":
    main()
