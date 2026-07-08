#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File Name : process_2D_accumulated_daily_multvar_from_cds_hourly.py
Author: Ruth Lorenz (ruth.lorenz@c2sm.ethz.ch)
Created: 29/06/2026
Purpose: process ERA5-Land data downloaded from cdsapi
        to cmip like format, with renaming of variables and dimensions,
        and conversion of units if necessary.
        Retrieve muliple variables at once from cds for efficiency, and process them in one go.
        The script is designed to be flexible and can be adapted
        to variable names and units as in cmip.
        Accumulated daily variables for ERA5-Landcan be downloaded from cds directly
        as the accumulations are over the 24 hours ending at 00 UTC i.e. the accumulation is during the previous day
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
import json
from datetime import datetime
from pathlib import Path
import cdsapi
import xarray as xr
from cdo import Cdo
from functions.file_util import parse_args, read_era5_info_list
from functions.read_config import read_yaml_config
from functions.general_functions import convert_month_list, convert_valid_time_latitude_longitude, convert_era5_to_cmip, calc_mon_mean

cdo = Cdo(debug=True)

# -------------------------------------------------
# Create a simple logger
# -------------------------------------------------

# Define logfile and logger
seconds = time.time()
local_time = time.localtime(seconds)
# Name the logfile after first of all inputs
LOG_FILENAME = (
    f"logfiles/logging_ERA5-Land_cds_multvars_accumulated_24h"
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

def download_data_cds_multvar(dataname, varlist, download_path, year, month, overwrite=False):
    '''
    Download data from CDS for given variables, year and month, and save to target file.
    It will download all variables in varlist in one request, and save to one grib file by default.

    Input:
    dataname: name of the dataset, e.g. "ERA5-Land"
    varlist: list of variable long names to be downloaded, e.g. ["2m temperature", "10m u-component of wind"]
    download_path: path to save the downloaded file
    year: year to be downloaded, e.g. 2020
    month: month to be downloaded, e.g. "01" for January
    overwrite: whether to overwrite existing files
    '''
    target = f'{download_path}/variables_accum_24h_{dataname}_{year}{month}.grib'

    if not os.path.isfile(f'{target}') or overwrite:
        logger.info(f"Downloading data for {dataname} from CDS for year {year} and month {month}.")
        dataset = f"reanalysis-{dataname}"
        request = {
            "variable": varlist,
            "year": year,
            "month": month,
            "day": [
                "02", "03", "04", "05",
                "06", "07", "08", "09", "10",
                "11", "12", "13", "14", "15",
                "16", "17", "18", "19", "20",
                "21", "22", "23 ", "24", "25",
                "26", "27", "28", "29", "30", "31"],
            "time": ["00:00"],
            "data_format": "grib",
            "download_format": "unarchived"
        }

        client = cdsapi.Client()
        client.retrieve(dataset, request, f'{target}')
        return target
    else:
        logger.info(f"File {target} already exists, skipping download.")
        return target



def download_data_cds_multvar_plus1(dataname, varlist, download_path, year, month, overwrite=False):
    '''
    Download data from CDS for given variables, year and month, and save to target file.
    The first timestep of the next month is needed to get the accumulated values for the last day of the month.
    It will download all variables in varlist in one request, and save to one grib file by default.

    Input:
    dataname: name of the dataset, e.g. "ERA5-Land"
    varlist: list of variable long names to be downloaded, e.g. ["2m temperature", "10m u-component of wind"]
    download_path: path to save the downloaded file
    year: year to be downloaded, e.g. 2020
    month: month to be downloaded, e.g. "01" for January
    overwrite: whether to overwrite existing files
    '''
    target_plus1 = f'{download_path}/variables_accum_24h_{dataname}_{year}{month}_plus1.grib'

    month_plus1 = str(int(month) + 1).zfill(2) if int(month) < 12 else "01"
    year_plus1 = year if month != "12" else year + 1

    if not os.path.isfile(f'{target_plus1}') or overwrite:
        logger.info(f"Downloading data for {dataname} from CDS for year {year_plus1}, month {month_plus1}, day 01.")
        dataset = f"reanalysis-{dataname}"
        request = {
            "variable": varlist,
            "year": year_plus1,
            "month": month_plus1,
            "day": ["01"],
            "time": ["00:00"],
            "data_format": "grib",
            "download_format": "unarchived"
        }

        client = cdsapi.Client()
        client.retrieve(dataset, request, f'{target_plus1}')
        return target_plus1
    else:
        logger.info(f"File {target_plus1} already exists, skipping download.")
        return target_plus1



def extract_vars_change_metadata(nc_file, var, era5_info):
    '''
    Extract variable from netcdf file and change metadata to more informative values.

    Input:
    nc_file: path to the netcdf file containing all variables
    var: variable short name to be extracted, e.g. "2d"
    era5_info: dictionary with variable information from ERA5_variables.json

    Return:
    Saves the extracted variable to a new netcdf file with updated metadata.
    '''
    logger.info(f'Extracting variable {var} from {nc_file} and changing metadata.')
    nc_file_path = Path(nc_file)
    basename = nc_file_path.stem
    work_out = nc_file_path.parent

    try:
        cmd = ['ncks',
                '-O',
                '-v',
                f'var{era5_info[var]["param"]}',
                f'{nc_file}',
                f'{work_out}/{var}_{basename}.nc']
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        returncode = result.returncode
        logger.info(f"Command returned code {returncode}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error extracting variable {var} from {nc_file}: {e}")
        returncode = e.returncode
        logger.error(f"Command failed with return code {returncode}")
        logger.error(f"Standard output:\n{e.stdout}")
        logger.error(f"Standard error:\n{e.stderr}")
        sys.exit()

    try:
        cmd = ['ncatted',
                '-O',
                '-a',
                f'standard_name,var{era5_info[var]["param"]},c,c,{era5_info[var]['long_name']}',
                f'{work_out}/{var}_{basename}.nc',
                f'{work_out}/{var}_{basename}_ncatted.nc']
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        returncode = result.returncode
    except subprocess.CalledProcessError as e:
        logger.error(f"Error adding standard_name attribute to {var}: {e}")
        returncode = e.returncode
        logger.error(f"Command failed with return code {returncode}")
        logger.error(f"Standard output:\n{e.stdout}")
        logger.error(f"Standard error:\n{e.stderr}")
        sys.exit()

    try:
        cmd = ['ncatted',
                '-O',
                '-a',
                f'units,var{era5_info[var]["param"]},c,c,"{era5_info[var]['unit']}"',
                f'{work_out}/{var}_{basename}_ncatted.nc',
                f'{work_out}/{var}_{basename}_ncatted2.nc']
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        returncode = result.returncode
    except subprocess.CalledProcessError as e:
        logger.error(f"Error adding units attribute to {var}: {e}")
        returncode = e.returncode
        logger.error(f"Command failed with return code {returncode}")
        logger.error(f"Standard output:\n{e.stdout}")
        logger.error(f"Standard error:\n{e.stderr}")
        sys.exit()

    try:
        cdo.setname(var, input=f'{work_out}/{var}_{basename}_ncatted2.nc', output=f'{work_out}/{var}_{basename}_setname.nc')
    except Exception as e:
        logger.error(f"Error setting name for {var}: {e}")
        sys.exit()

    return f'{work_out}/{var}_{basename}_setname.nc'


def main():
    # -------------------------------------------------
    # Parse command line input
    # -------------------------------------------------
    parser = argparse.ArgumentParser(
        description="Download ERA5-Land data and process to CMIP like"
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
    var_list_short = config['variables']['varlist']

    # configured paths
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
    era5_info = read_era5_info_list(var_list_short)
    print(era5_info)

    # download and process for all years in configuration
    for year in range(startyr, endyr + 1):
        logger.info(f"Processing year {year}.")
        logger.info(f"Copying variables {var_list_short} from {store}.")

        varlist_long = [era5_info[var]['long_name'] for var in var_list_short]
        print(f"Variable list for download: {varlist_long}")
        assert store == 'cds', f"Store {store} is not supported."
        for month in months:
            # retreive data from cds, returns path to downloaded file, all variables in one file
            download_file_multvar = download_data_cds_multvar(dataname, varlist_long, download_path, year, month, overwrite=overwrite)
            download_file_multvar_plus1 = download_data_cds_multvar_plus1(dataname, varlist_long, download_path, year, month, overwrite=overwrite)

            # convert grib file to netcdf
            download_file_path = Path(download_file_multvar)
            nc_file = download_file_path.with_suffix(".nc")
            print(str(nc_file))
            try:
                cdo.sorttaxis(options="-f nc4", input=download_file_multvar, output=str(nc_file))
            except Exception as e:
                logger.error(f"Error converting {download_file_multvar} to netcdf: {e}")
                continue

            # convert plus1 file to netcdf
            download_file_plus1_path = Path(download_file_multvar_plus1)
            nc_file_plus1 = download_file_plus1_path.with_suffix(".nc")
            try:
                cdo.sorttaxis(options="-f nc4", input=download_file_multvar_plus1, output=str(nc_file_plus1))
            except Exception as e:
                logger.error(f"Error converting {download_file_multvar_plus1} to netcdf: {e}")
                continue

            # extract individual variables and change metadata
            for v, var in enumerate(var_list_short):
                logger.info(f"Processing variable {var} for year {year} and month {month}.")
                work_out = f'{work_all_path}/{var}/'
                os.makedirs(work_out, exist_ok=True)
                proc_archive = f'{proc_path}/{era5_info[var]["cmip_name"]}/day/native/{year}'
                os.makedirs(proc_archive, exist_ok=True)

                tmpfile = extract_vars_change_metadata(nc_file, var, era5_info)
                tmpfile_plus1 = extract_vars_change_metadata(nc_file_plus1, var, era5_info)

                # accumulated values are over the 24 hours ending at 00 UTC i.e. the accumulation is during the previous day,
                # so we need to shift the time by -1 second to get the correct calendar day for the accumulation
                tmp_outfile = f'{work_out}/{var}_1day_era5-land_{year}{month}.nc'
                try:
                    cdo.shifttime("-1sec", input=tmpfile, output=tmp_outfile)
                except Exception as e:
                    logger.error(f"Error shifting time for {tmpfile}: {e}")
                    continue
                tmp_outfile_plus1 = f'{work_out}/{var}_1day_era5-land_{year}{month}_plus1.nc'
                try:
                    cdo.shifttime("-1sec", input=tmpfile_plus1, output=tmp_outfile_plus1)
                except Exception as e:
                    logger.error(f"Error shifting time for {tmpfile_plus1}: {e}")
                    continue
                # concatenate plus1 file to the end of the month file to get the last day of the month correct
                tmp_outfile_concat = f'{work_out}/{var}_1day_era5-land_{year}{month}_concat.nc'
                try:
                    cdo.mergetime(input=f"{tmp_outfile} {tmp_outfile_plus1}", output=tmp_outfile_concat)
                except Exception as e:
                    logger.error(f"Error concatenating {tmp_outfile} and {tmp_outfile_plus1}: {e}")
                    continue


                outfile = f'{proc_archive}/{era5_info[var]["cmip_name"]}_day_{dataname}_{year}{month}.nc'
                outfile_name = convert_era5_to_cmip(
                    tmp_outfile_concat, outfile, store, era5_info[var],
                    config['chunking']['time_chk'], config['chunking']['lon_chk'], config['chunking']['lat_chk']
                )
                assert outfile_name == outfile, f"Output file name {outfile_name} does not match expected file name {outfile}."
                if not os.path.isfile(outfile_name) or os.path.getsize(outfile_name) == 0:
                    logger.error(f"Output file {outfile_name} after conversion to cmip format was not created successfully.")
                    sys.exit(1)
                else:
                    logger.info(f"File {outfile_name} written.")
                    os.remove(tmp_outfile_concat)
                    logger.info(f"Temporary file {tmp_outfile_concat} removed.")

                # calculate monthly mean
                outfile_mon = calc_mon_mean(proc_archive, outfile_name)
                if not os.path.isfile(outfile_mon) or os.path.getsize(outfile_mon) == 0:
                    logger.error(f"Output file {outfile_mon} not created successfully.")
                    sys.exit(1)
                else:
                    logger.info(f"File {outfile_mon} written successfully.")


if __name__ == "__main__":
    main()




