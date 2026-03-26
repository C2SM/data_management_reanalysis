#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File Name : process_2D_from_cds_3hourly.py
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
import numpy as np
import re
import glob

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

    if dataname == "CERRA-Land":
        level_type="surface"
    else:
        level_type="surface_or_atmosphere"

    target_allg = f'{workdir}/{cerra_info["short_name"]}_3hr_{data_name}_{year}MM.nc'

    dataset = origin

    for month in months:

        target_str = target_allg.replace("MM", month)
        grib_file = target_str.replace(".nc", ".grib")
        target=Path(grib_file)
        target_nc = Path(target_str)
        logger.info(f'NetCDFfile to download is {target_str}')

        if overwrite or (not target.exists() and not target_nc.exists()):
            request = {
                "variable": [long_name],
                "level_type": [level_type],
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
        else:
            logger.info(f"File {target_str} already exists, skipping download.")
            print(f"File {target_str} already exists, skipping download.")

        if not target_nc.is_file() or overwrite:
            cdo.copy(options =  "-f nc4 sorttaxis", input=grib_file, output=target_str)


        #try:
        #    cmd = [
        #        'rm',
        #        f'{grib_file}'
        #    ]
        #    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        #except subprocess.CalledProcessError as e:
        #    print(f"Command failed with return code {e.returncode}")
        #    print(f"Standard output:\n{e.stdout}")

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

    if dataname == "CERRA-Land":
        level_type="surface"
    else:
        level_type="surface_or_atmosphere"

    target_allg = f'{workdir}/{cerra_info["short_name"]}_3hr_{data_name}_{year}MM.nc'

    dataset = origin

    for month in months:

        target_str = target_allg.replace("MM", month)
        grib_file = target_str.replace(".nc", ".grib")
        target = Path(grib_file)
        target_nc = Path(target_str)
        logger.info(f'NetCDFfile to download is {target_str}')

        if overwrite or (not target.exists() and not target_nc.exists()):
            request = {
                "variable": [long_name],
                "level_type": [level_type],
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
        else:
            logger.info(f"File {target_str} already exists, skipping download.")

        if not target_nc.is_file() or overwrite:
            cdo.copy(options =  "-f nc4 sorttaxis", input=grib_file, output=target_str)

        #try:
        #    cmd = [
        #        'rm',
        #        f'{grib_file}'
        #    ]
        #    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        #except subprocess.CalledProcessError as e:
        #    print(f"Command failed with return code {e.returncode}")
        #    print(f"Standard output:\n{e.stdout}")

    return target_allg


def convert_cerra_to_cmip(tmp_outfile, proc_archive, cerra_info, dataname, year, time_chk, lon_chk, lat_chk):
    if "tasmax" in tmp_outfile:
        newname = cerra_info["cmip_name"].replace("tas", "tasmax")
        agg_method = "max"
    elif "tasmin" in tmp_outfile:
        newname = cerra_info["cmip_name"].replace("tas", "tasmin")
        agg_method = "min"
    else:
        newname = cerra_info["cmip_name"]
        agg_method = cerra_info["agg_method"]
    oldname = cerra_info["short_name"]
    path_to_tmp = Path(tmp_outfile)
    tmpfile = f'{str(path_to_tmp.parent)}/{path_to_tmp.stem}'
    outfile = f'{proc_archive}/{newname}_day_{agg_method}_{dataname.lower()}_{year}.nc'


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

    if oldname == "2t":
        # for 2m temperature squeeze height dimension
        try:
            cmd = [
                "ncwa",
                "-O","-a","height",
                f"{tmpfile}_chunked.nc",
                f"{tmpfile}_squeezed.nc"
            ]
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            print(f"Command failed with return code {e.returncode}")
            print(f"Standard output:\n{e.stdout}")
    else:
        try:
            cmd = [
                "mv",
                f"{tmpfile}_chunked.nc",
                f"{tmpfile}_squeezed.nc"
            ]
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            print(f"Command failed with return code {e.returncode}")
            print(f"Standard output:\n{e.stdout}")


    try:
        cmd = [
            "ncrename",
            "-O","-v",
            f"{oldname},{newname}",
            f"{tmpfile}_squeezed.nc",
            f"{outfile}"
        ]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with return code {e.returncode}")
        print(f"Standard output:\n{e.stdout}")

        if oldname == '2t':
            print(f"Try with using t2m instead 2t.")
            try:
                cmd = [
                    "ncrename",
                    "-O",
                    "-v",
                    f't2m,{newname}',
                    f"{tmpfile}_squeezed.nc",
                    f"{outfile}"
                ]
                result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError as e:
                print(f"Command failed with return code {e.returncode}")
                print(f"Standard output:\n{e.stdout}")

    return outfile


def process_daily_stats(work_path, var, proc_archive, cerra_info, months,
                        download_file, dataname, year, config, outfile_mon):
    """
    Processes sub-daily data to daily max/min with error handling and validation.
    """
    stats = ['tasmax', 'tasmin']
    results = {}

    for stat in stats:
        try:
            # Path Setup & Directory Creation
            stat_workpath = work_path.replace(var, stat)
            stat_outpath = proc_archive.replace(cerra_info["cmip_name"], stat)

            for path in [stat_workpath, stat_outpath]:
                os.makedirs(path, exist_ok=True)

            # Daily Processing with Input Validation
            for month in months:
                infile = download_file.replace("MM", month)

                if not os.path.exists(infile):
                    logger.error(f"Input file missing: {infile}")
                    continue

                outfile_daily = infile.replace("3hr", "day").replace("2t", stat)

                try:
                    if stat == 'tasmax':
                        cdo.daymax(input=infile, output=outfile_daily)
                    else:
                        cdo.daymin(input=infile, output=outfile_daily)
                except Exception as e:
                    logger.error(f"CDO daily operation failed for {stat} in month {month}: {e}")
                    raise

            # Merging Monthly Files
            daily_files_wildcard = download_file.replace("3hr", "day").replace("2t", stat).replace("MM", "??")
            outfile_yearly_temp = daily_files_wildcard.replace("??", "")

            # Check if any daily files were actually created before merging
            if not glob.glob(daily_files_wildcard):
                raise FileNotFoundError(f"No daily files found to merge for {stat} using pattern {daily_files_wildcard}")

            cdo.mergetime(input=daily_files_wildcard, output=outfile_yearly_temp)

            # Conversion to CMIP
            # We wrap this in a sub-try because it's often a custom complex function
            try:
                final_yearly_file = convert_cerra_to_cmip(
                    outfile_yearly_temp, stat_outpath, cerra_info,
                    dataname, year, config['chunking']['time_chk'],
                    config['chunking']['lon_chk'], config['chunking']['lat_chk']
                )
            except KeyError as e:
                logger.error(f"Config error: Missing chunking key {e}")
                raise
            except Exception as e:
                logger.error(f"CMIP conversion failed for {stat}: {e}")
                raise

            # Monthly Mean Calculation
            stat_outfile_mon = outfile_mon.replace("2t", stat)
            cdo.monmean(input=final_yearly_file, output=stat_outfile_mon)

            logger.info(f"Successfully processed: {stat_outfile_mon}")
            results[stat] = stat_outfile_mon

        except Exception as e:
            logger.critical(f"Pipeline failed for statistic '{stat}': {e}")
            continue

    return results


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

    if config['variables']['product']:
        type = config['variables']['product'].lower()
    else:
        if int(cerra_info["analysis"]) == 1:
            type = "analysis"
        else:
            type = "forecast"
    agg_method = cerra_info["agg_method"]

    # download and process for all years in configuration
    for year in range(startyr, endyr + 1):
        logger.info(f"Processing year {year}.")

        logger.info(f'Store: {store}')
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
                # shift time by -1 hour to get the correct day aggregation
                cdo.shifttime("-1hour", input=infile, output=tmpfile)
                if agg_method == "max":
                    cdo.daymax(input=tmpfile, output=outfile)
                elif agg_method == "min":
                    cdo.daymin(input=tmpfile, output=outfile)
                elif agg_method == "sum":
                    cdo.daysum(input=tmpfile, output=outfile)
                elif agg_method == "mean":
                    cdo.daymean(input=tmpfile, output=outfile)
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

        daily_files = download_file.replace("3hr", "day").replace("MM", "??")
        outfile_yearly = daily_files.replace("??", "")
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


        # for 2m temperaturea also create tasmax and tasmin files
        if var == "2t":
            daily_stats = process_daily_stats(work_path, var, proc_archive, cerra_info, months,
                            download_file, dataname, year, config, outfile_mon)

            logger.info(f"Daily Max processed: {daily_stats['tasmax']}")
            logger.info(f"Daily Min processed: {daily_stats['tasmin']}")

        # -------------------------------------------------
        # Clean up
        # -------------------------------------------------
        #os.system(f"rm {work_path}/{var}_*")

if __name__ == "__main__":
    main()






