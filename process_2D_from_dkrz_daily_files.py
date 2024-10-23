#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File Name : process_2D_frm_dkrz_daily_files.py
Author: Ruth Lorenz (ruth.lorenz@c2sm.ethz.ch)
Created: 08/12/2023
Purpose: process ERA5 data downloaded from dkrz
        to variable names and units as in cmip
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

from functions.file_util import read_era5_info
from functions.read_config import read_yaml_config

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


def download_data_dkrz(var, freq, family, level, year, era5_info, origin, iac_path):
    """
    Download data from DKRZ
    """

    param = int(era5_info["param"])
    vparam = f"{param:03}"

    if int(era5_info["analysis"]) == 1:
        type = "an"
        typeid = "00"
    else:
        type = "fc"
        typeid = "12"

    dkrz_path = f"{origin}/{type}/{freq}/{vparam}"

    os.makedirs(iac_path, exist_ok=True)

    logger.info(f"rsync data from  {dkrz_path} to {iac_path}")
    files_to_copy = f'{dkrz_path}/{family}{level}{typeid}_{freq}_{year}-??_{vparam}.*'
    os.system(
        f"rsync -av levante:{files_to_copy} {iac_path}"
    )

    return typeid, vparam


def convert_netcdf_add_era5_info(grib_file, workdir, era5_info, year, month):
    """
    Convert grib file to netcdf

    use grib_to_netcdf, adds meaningful variable name and time dimension
    incl. standard_name and long_name
    """

    tmpfile = f'{workdir}/tmp_var{era5_info["param"]}_era5_{year}{month}'
    tmp_outfile = f'{workdir}/{era5_info["short_name"]}_era5_{year}{month}.nc'
    os.system(f"cdo -t ecmwf -setgridtype,regular {grib_file} {tmpfile}.grib")
    os.system(f"grib_to_netcdf -o  {tmp_outfile} {tmpfile}.grib")

    return tmp_outfile


def convert_tcc(tcc_outfile, workdir, era5_info, year, month):
    os.system(
        f'cdo -b F64 mulc,100 {tcc_outfile} {workdir}/{era5_info["short_name"]}_era5_{year}{month}_mulc.nc'
    )
    os.system(f'rm {workdir}/{era5_info["short_name"]}_era5_{year}{month}.nc')
    os.system(
        f'ncatted -a units,{era5_info["short_name"]},m,c,"{era5_info["cmip_unit"]}" {workdir}/{era5_info["short_name"]}_era5_{year}{month}_mulc.nc {tcc_outfile}'
    )

    return tcc_outfile


def convert_tp(tp_outfile, workdir, era5_info, year, month):
    os.system(
        f'cdo -b F64 divc,86400 {tp_outfile} {workdir}/{era5_info["short_name"]}_era5_{year}{month}_divc.nc'
    )
    os.system(f'rm {workdir}/{era5_info["short_name"]}_era5_{year}{month}.nc')
    os.system(
        f'ncatted -a units,{era5_info["short_name"]},m,c,"{era5_info["cmip_unit"]}" {workdir}/{era5_info["short_name"]}_era5_{year}{month}_divc.nc {tp_outfile}'
    )

    return tp_outfile


def convert_radiation(rad_outfile, workdir, era5_info, year, month):
    os.system(
        f'cdo -b F64 divc,86400 {rad_outfile} {workdir}/{era5_info["short_name"]}_era5_{year}{month}_divc.nc'
    )
    os.system(f'rm {workdir}/{era5_info["short_name"]}_era5_{year}{month}.nc')
    os.system(
        f'ncatted -a units,{era5_info["short_name"]},m,c,"{era5_info["cmip_unit"]}" {workdir}/{era5_info["short_name"]}_era5_{year}{month}_divc.nc {rad_outfile}'
    )

    return rad_outfile


def convert_era5_to_cmip(tmp_outfile, proc_archive, era5_info, year, month, time_chk, lon_chk, lat_chk):
    #tmpfile = f'{work_path}/{era5_info["short_name"]}_era5_{year}{month}'
    path_to_tmp = Path(tmp_outfile)
    tmpfile = f'{str(path_to_tmp.parent)}/{path_to_tmp.stem}'
    outfile = f'{proc_archive}/{era5_info["cmip_name"]}_day_era5_{year}{month}.nc'
    os.system(
        f"cdo remapcon,/net/atmos/data/era5_cds/gridfile_cds_025.txt {tmp_outfile} {tmpfile}_remapped.nc"
    )
    os.system(
        f"ncks -O -4 -D 4 --cnk_plc=g3d --cnk_dmn=time,{time_chk} --cnk_dmn=lat,{lat_chk} --cnk_dmn=lon,{lon_chk} {tmpfile}_remapped.nc {tmpfile}_chunked.nc"
    )
    os.system(
        f'ncrename -O -v {era5_info["short_name"]},{era5_info["cmip_name"]} {tmpfile}_chunked.nc {outfile}'
    )

    return outfile


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

    # variable to be processed
    var = config['variables']['varname']
    freq = config['variables']['freq']
    family = config['variables']['family']
    level = config['variables']['level']

    # configured paths
    origin = config['paths']['origin']
    download_path = config['paths']['download']
    work_path = config['paths']['work']
    proc_path = config['paths']['proc']
    grib_path = f"{download_path}/{var}/"

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


    # download and process for all years in configuration
    for year in range(config['time']['startyr'], config['time']['endyr'] + 1):
        logger.info(f"Processing year {year}.")
        logger.info(f"Copying variable {var}")

        if config['dataset']['origin'] == 'dkrz':
            typeid, vparam = download_data_dkrz(var, freq, family, level, year, era5_info, origin, grib_path)
            download_success = f"Data download successful!"
        else:
            download_success = f"Warning, download from origin {config['dataset']['origin']} not implemented."
        logger.info(download_success)

        proc_archive = f'{proc_path}/{era5_info["cmip_name"]}/day/native/{year}'
        os.makedirs(proc_archive, exist_ok=True)

        months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12",]

        for month in months:

            grib_file = f'{grib_path}{family}{level}{typeid}_{freq}_{year}-{month}_{vparam}.grb'

            tmp_outfile = convert_netcdf_add_era5_info(
                grib_file, work_path, era5_info, year, month
            )

            # check if unit needs to be changed from era5 variable to cmip variable
            if era5_info["unit"] != era5_info["cmip_unit"]:
                logger.info(
                    f'Unit for {era5_info["short_name"]} needs to be changed from {era5_info["unit"]} to {era5_info["cmip_unit"]}.'
                )
                if var == "tcc":
                    tmp_outfile = convert_tcc(
                        tmp_outfile, work_path, era5_info, year, month
                    )
                elif var == "tp":
                    tmp_outfile = convert_tp(
                        tmp_outfile, work_path, era5_info, year, month
                    )
                elif var == "ssrd" or var == "strd" or var == "str":
                    tmp_outfile = convert_radiation(
                        tmp_outfile, work_path, era5_info, year, month
                    )
                else:
                    logger.error(
                        f"Conversion of unit for variable {var} is not implemented!"
                    )
                    sys.exit(1)


            outfile_name = convert_era5_to_cmip(
                tmp_outfile, proc_archive, era5_info, year, month,
                config['chunking']['time_chk'], config['chunking']['lon_chk'], config['chunking']['lat_chk']
            )
            logger.info(f"File {outfile_name} written.")

            # calculate monthly mean
            proc_mon_archive = (
                f'{proc_path}/{era5_info["cmip_name"]}/mon/native/{year}'
            )
            os.makedirs(proc_mon_archive, exist_ok=True)
            outfile_mon = (
                f'{proc_mon_archive}/{era5_info["cmip_name"]}_mon_era5_{year}{month}.nc'
            )
            os.system(f"cdo monmean {outfile_name} {outfile_mon}")

        # -------------------------------------------------
        # Clean up
        # -------------------------------------------------
        os.system(f"rm {work_path}/{var}_*")
        os.system(f"rm {work_path}/tmp_{var}_*")
        os.system(f"rm {grib_path}/*")


if __name__ == "__main__":
    main()
