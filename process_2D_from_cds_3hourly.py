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
import subprocess
import sys
import time
import argparse
from datetime import datetime
from pathlib import Path
import cdsapi
import xarray as xr
from cdo import Cdo
from functions.read_config import read_yaml_config
from functions.general_functions import *
from functions.file_util import read_cerra_info

cdo = Cdo()

# -------------------------------------------------
# Create a simple logger
# -------------------------------------------------

# Define logfile and logger
seconds = time.time()
local_time = time.localtime(seconds)
# Name the logfile after first of all inputs
LOG_FILENAME = (
    f"logfiles/logging_download_CERRA_cds"
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


def download_data_cds_an(dataname, cerra_info, origin, workdir, year, months, overwrite):
    """
    Download data from CDS
    for analysis variables
    Convert to netcdf

    Returns:
    Name of the netcdf files in general form (MM instead single months)

    """
    import cdsapi

    long_name = cerra_info["long_name"]
    data_name = dataname.lower()

    target_allg = f'{workdir}/{cerra_info["short_name"]}_3hr_{data_name}_{year}MM.nc'

    for month in months:

        target = target_allg.replace("MM", month)
        grib_file = target.replace(".nc", ".grib")
        logger.info(f'NetCDFfile to download is {target}')

        dataset = origin

        if not os.path.isfile(f'{target}') or overwrite:
            request = {
                "variable": [long_name],
                "level_type": "surface_or_atmosphere",
                "data_type": ["reanalysis"],
                "product_type": ["analysis"],
                "year": [year],
                'month': [month],
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

            cdo.copy(options =  "-f nc", input=grib_file, output=target)

            try:
                cmd = [
                    'rm',
                    f'{grib_file}'
                ]
                result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError as e:
                print(f"Command failed with return code {e.returncode}")
                print(f"Standard output:\n{e.stdout}")

    return target_allg


def download_data_cds_fc(dataname, cerra_info, origin, workdir, year, months, overwrite):
    """
    Download data from CDS
    for forecast variables
    Convert to netcdf

    Returns:
    Name of the netcdf files in general form (MM instead single months)

    """
    import cdsapi

    long_name = cerra_info["long_name"]
    data_name = dataname.lower()

    target_allg = f'{workdir}/{cerra_info["short_name"]}_3hr_{data_name}_{year}MM.nc'

    for month in months:

        target = target_allg.replace("MM", month)
        grib_file = target.replace(".nc", ".grib")
        logger.info(f'NetCDFfile to download is {target}')

        dataset = origin

        if not os.path.isfile(f'{target}') or overwrite:
            request = {
                "variable": [long_name],
                "level_type": "surface_or_atmosphere",
                "data_type": ["reanalysis"],
                "product_type": ["forecast"],
                "year": [year],
                'month': [month],
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
                "leadtime_hour": ["3"],
                "data_format": "grib"
            }

            client = cdsapi.Client()
            client.retrieve(dataset, request, grib_file)

            cdo.copy("-f nc", input=grib_file, output=target)

            try:
                cmd = [
                    'rm',
                    f'{gribfile}'
                ]
                result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError as e:
                print(f"Command failed with return code {e.returncode}")
                print(f"Standard output:\n{e.stdout}")

    return target_allg


def convert_cerra_to_cmip(tmp_outfile, proc_archive, cerra_info, dataname, year, time_chk, lon_chk, lat_chk):
    #tmpfile = f'{work_path}/{era5_info["short_name"]}_era5_{year}{month}'
    oldname = cerra_info["short_name"]
    newname = cerra_info["cmip_name"]
    path_to_tmp = Path(tmp_outfile)
    tmpfile = f'{str(path_to_tmp.parent)}/{path_to_tmp.stem}'
    outfile = f'{proc_archive}/{newname}_day_{cerra_info["agg_method"]}_{dataname.lower()}_{year}.nc'

    #os.system(
    #    f"ncks -O -4 -D 4 --cnk_plc=g3d --cnk_dmn=time,{time_chk} --cnk_dmn=lat,{lat_chk} --cnk_dmn=lon,{lon_chk} -L 1 {tmp_outfile} {tmpfile}_chunked.nc"
    #)
    try:
        cmd = [
            "ncks",
            "-O", "-4", "-D", "4",
            "--cnk_plc=g3d",
            f"--cnk_dmn=time,{time_chk}",
            f"--cnk_dmn=lat,{lat_chk}",
            f"--cnk_dmn=lon,{lon_chk}",
            "-L", "1",
            f"{tmp_outfile}",
            f"{tmpfile}_chunked.nc"
        ]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with return code {e.returncode}")
        print(f"Standard output:\n{e.stdout}")

    #os.system(
    #    f'ncrename -O -v {cerra_info["short_name"]},{cerra_info["cmip_name"]} {tmpfile}_chunked.nc {outfile}'
    #)
    try:
        cmd = [
            "ncrename",
            "-O","-v",
            f"{oldname},{newname}",
            f"{tmpfile}_chunked.nc",
            f"{outfile}"
        ]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with return code {e.returncode}")
        print(f"Standard output:\n{e.stdout}")

    return outfile


def main():
    # -------------------------------------------------
    # Parse command line input
    # -------------------------------------------------
    parser = argparse.ArgumentParser(
        description="Download CERRA data and process to CMIP like"
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
    dataname = config['dataset']['name']

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
    # read CERRA_variables.json
    # -------------------------------------------------
    logger.info("Variable info red from json file.")
    cerra_info = read_cerra_info(var)

    # -------------------------------------------------
    # Download data and process
    # -------------------------------------------------
    logger.info(f"Downloading variable {var}")

    assert freq=='3hr', "Frequency needs to be 3-hourly, no other download frequencies implemented."

    if int(cerra_info["analysis"]) == 1:
        type = "analysis"
    else:
        type = "forecast"
    agg_method = cerra_info["agg_method"]

    # download and process for all years in configuration
    for year in range(startyr, endyr + 1):
        logger.info(f"Processing year {year}.")

        print(store)
        if store == 'cds':
            if type == "analysis":
                download_file = download_data_cds_an(
                    dataname=dataname, cerra_info=cerra_info, origin=origin, workdir=work_path, year=year, months=months, overwrite=overwrite)
            elif type == "forecast":
                download_file = download_data_cds_fc(
                    dataname=dataname, cerra_info=cerra_info, origin=origin, workdir=work_path, year=year, months=months, overwrite=overwrite)
            else:
                logger.error(f"Wrong product type, needs to be analysis or forecast, type {type} not available.")
            download_success = f"Data download successful!"
        else:
            download_success = f"Warning, download from store {store} not implemented."
        logger.info(download_success)
        print(download_file)

        for month in months:
            infile = download_file.replace("MM", month)
            outfile= infile.replace("3hr", "day")
            if type == "analysis" and agg_method == "mean":
                cdo.daymean(input=infile, output=outfile)
            elif type == "forecast":
                tmpfile=infile.replace("3hr", "tmp")
                cdo.shifttime("-1hour", input=infile, output=tmpfile)
                if agg_method == "max":
                    cdo.daymax(input=tmpfile, output=outfile)
                elif agg_method == "min":
                    cdo.daymin(input=tmpfile, output=outfile)
                elif agg_method == "sum":
                    cdo.daysum(input=tmpfile, output=outfile)
                else:
                    logger.error(f"Variables with type forecast should be aggregated as sum, max or min not {agg_method}.")
            else:
                logger.error(f"Type analysis {type} with aggregation method {agg_method} is not implemented.")

            # check if unit needs to be changed from era5/cerra variable to cmip variable
            if cerra_info["unit"] != cerra_info["cmip_unit"]:
                logger.info(
                    f'Unit for {cerra_info["short_name"]} needs to be changed from {cerra_info["unit"]} to {cerra_info["cmip_unit"]}.'
                )
                if var == "tcc":
                    outfile = convert_tcc(
                        outfile, work_path, cerra_info, dataname, year, month
                    )
                elif var == "tp":
                    print('Converting tp')
                    outfile = convert_tp(
                        outfile, work_path, cerra_info, dataname, year, month
                    )
                elif var == "ssrd" or var == "strd" or var == "str" or var == "ssr":
                    outfile = convert_radiation(
                        outfile, work_path, cerra_info, dataname, year, month
                    )
                else:
                    logger.error(
                        f"Conversion of unit for variable {var} is not implemented!"
                    )
                    sys.exit(1)

        daily_files = download_file.replace("3hr", "day").replace("MM", "*")
        outfile_yearly = daily_files.replace("*", "")
        print(outfile_yearly)
        cdo.mergetime(input=daily_files, output=outfile_yearly)

        logger.info(f"Data processed to daily values with aggregation method {agg_method}.")

        proc_archive = f'{proc_path}/{cerra_info["cmip_name"]}/day/native/'
        os.makedirs(proc_archive, exist_ok=True)
        proc_mon_archive = proc_archive.replace("day", "mon")
        os.makedirs(proc_mon_archive, exist_ok=True)

        # convert name to cmip and chunk
        outfile_name = convert_cerra_to_cmip(outfile_yearly, proc_archive, cerra_info, dataname, year, config['chunking']['time_chk'], config['chunking']['lon_chk'], config['chunking']['lat_chk'])
        logger.info(f"File {outfile_name} written.")

        # calculate monthly mean
        outfile_mon = (
            f'{proc_mon_archive}/{cerra_info["cmip_name"]}_mon_{dataname.lower()}_{year}.nc'
        )
        cdo.monmean(input=outfile_name, output=outfile_mon)

        # -------------------------------------------------
        # Clean up
        # -------------------------------------------------
        os.system(f"rm {work_path}/{var}_*")

if __name__ == "__main__":
    main()






