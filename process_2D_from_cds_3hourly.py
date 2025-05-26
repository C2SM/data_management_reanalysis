#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File Name : process_2D_frm_cds_3hourly.py
Author: Ruth Lorenz (ruth.lorenz@c2sm.ethz.ch)
Created: 19/05/2025
Purpose: process CERRA data downloaded from CDS
        to variable names and units as in cmip
        env: cdsapi_10_2024
"""
# -------------------------------------------------
# Getting libraries and utilities
# -------------------------------------------------
import logging
import os
import sys
import time
import argparse
from datetime import datetime
from pathlib import Path
import cdsapi
import xarray as xr
from functions.read_config import read_yaml_config
from functions.general_functions import convert_month_list
from functions.file_util import read_era5_info

# -------------------------------------------------
# Create a simple logger
# -------------------------------------------------

# Define logfile and logger
seconds = time.time()
local_time = time.localtime(seconds)
# Name the logfile after first of all inputs
LOG_FILENAME = (
    f"logfiles/logging_process_ERA5_dkrz"
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


def download_data_cds(cerra_info, origin, workdir, year, months, overwrite):
    """
    Download data from CDS
    Convert to netcdf

    Returns:
    Name of the netcdf files in general form (MM instead single months)

    """
    import cdsapi

    long_name = cerra_info["long_name"]

    target_allg = f'{workdir}/{cerra_info["short_name"]}_3hr_cerra_{year}MM.nc'

    for month in months:

        target = target_allg.replace("MM", month)
        grib_file = target.replace(".nc", ".grib")
        logger.info(f'NetCDFfile to download is {target}')

        dataset = origin

        if not os.path.isfile(f'{target}') or overwrite:
            request = {
                "variable": [f'{long_name}'],
                "level_type": "surface_or_atmosphere",
                "data_type": ["reanalysis"],
                "product_type": ["analysis"],
                "year": [f'{year}'],
                'month': [f'{month}'],
                "day": [
                    "01", "02", "03",
                    "04", "05", "06",
                    "07", "08", "09",
                    "10", "11", "12",
                    "13", "14", "15",
                    "16", "17", "18",
                    "19", "20", "21",
                    "22", "23", "24",
                    "25", "26", "27",
                    "28", "29", "30",
                    "31"
                ],
                "time": [
                    "00:00", "03:00", "06:00",
                    "09:00", "12:00", "15:00",
                    "18:00", "21:00"
                ],
                "data_format": "grib"
            }

            client = cdsapi.Client()
            client.retrieve(dataset, request, grib_file)
            os.system(f'cdo -f nc copy {grib_file} {target}')
            os.system(f'rm {target}')

    return target_allg

def convert_cerra_to_cmip(tmp_outfile, store, proc_archive, cerra_info, year, month, time_chk, lon_chk, lat_chk):
    #tmpfile = f'{work_path}/{era5_info["short_name"]}_era5_{year}{month}'
    path_to_tmp = Path(tmp_outfile)
    tmpfile = f'{str(path_to_tmp.parent)}/{path_to_tmp.stem}'
    outfile = f'{proc_archive}/{cerra_info["cmip_name"]}_day_cerra_{year}{month}.nc'


    os.system(
        f"ncks -O -4 -D 4 --cnk_plc=g3d --cnk_dmn=time,{time_chk} --cnk_dmn=lat,{lat_chk} --cnk_dmn=lon,{lon_chk} -L 1 {tmp_outfile} {tmpfile}_chunked.nc"
    )
    os.system(
        f'ncrename -O -v {era5_info["short_name"]},{era5_info["cmip_name"]} {tmpfile}_chunked.nc {outfile}'
    )

    return outfile


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

    # variable to be processed
    var = config['variables']['varname']
    freq = config['variables']['freq']

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
    # read ERA5_variables.json, equivalent for CERRA
    # -------------------------------------------------
    logger.info("Variable info red from json file.")
    cerra_info = read_era5_info(var)


