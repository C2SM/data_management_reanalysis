#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File Name : process_2D_analysis_multvars_from_cds_daily.py
Author: Ruth Lorenz (ruth.lorenz@c2sm.ethz.ch)
Created: 11/06/2026
Purpose: process ERA5, ERA5-Land data downloaded from cdsapi
        to cmip like format, with renaming of variables and dimensions,
        and conversion of units if necessary.
        Retrieve muliple variables at once from cds for efficiency, and process them in one go.
        The script is designed to be flexible and can be adapted
        to variable names and units as in cmip.
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
import zipfile
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
    f"logfiles/logging_ERA5-Land_cds_multvars_daily_stats"
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


def download_data_cds_multvar(
    dataname,
    varlist,
    download_path,
    year,
    month,
    statistic="daily_mean",
    overwrite=False):
    '''
    Download data from CDS for given variables, year and month, and save to target file.
    It will download all variables in varlist in one request, and save to one zip file by default.

    Input:
    dataname: name of the dataset, e.g. "ERA5-Land"
    varlist: list of variable long names to be downloaded, e.g. ["2m_temperature", "10m_u-component_of_wind"]
    download_path: path to save the downloaded file
    year: year to be downloaded, e.g. 2020
    month: month to be downloaded, e.g. "01" for January
    statistic: daily statistic to be downloaded, e.g. "daily_mean", "daily_maximum", "daily_minimum"
    overwrite: whether to overwrite existing files
    '''
    target = f'{download_path}/variables_{statistic}_{dataname}_{year}{month}.zip'

    if not os.path.isfile(f'{target}') or overwrite:
        logger.info(f"Downloading data for {dataname} from CDS for year {year} and month {month}.")
        dataset = f"derived-{dataname}-daily-statistics"
        request = {
            "variable": varlist
            ,
            "year": year,
            "month": month,
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
            "daily_statistic": statistic,
            "time_zone": "utc+00:00",
            "frequency": "1_hourly"
        }

        client = cdsapi.Client()
        client.retrieve(dataset, request, f'{target}')
        return target
    else:
        logger.info(f"File {target} already exists, skipping download.")
        return target


def process_multvar_era5_data(
    download_file_multvar,
    work_all_path,
    var_list_short,
    proc_path,
    era5_info,
    year,
    month,
    statistic_string,
    dataname,
    config,
    store,
    logger,
    overwrite=False,
):
    """Unzips, formats, and processes ERA5 multi-variable data for a given year and month.

    Parameters:
    -----------
    download_file_multvar : str
        Path to the downloaded multi-variable zip file.
    work_all_path : str
        Directory path where files are temporarily unzipped and processed.
    var_list_short : list
        List of short variable names to process.
    proc_path : str
        Base path for the final processed archive.
    era5_info : dict
        Metadata dictionary mapping short names to long names and CMIP names.
    year : int or str
        The year being processed.
    month : int or str
        The month being processed.
    statistic_string : str
        String pattern used in the unzipped file name (e.g., statistic type).
    dataname : str
        Name of the dataset.
    config : dict
        Configuration dictionary containing chunking sizes.
    store : object
        Storage target or handler used by the CMIP conversion tool.
    logger : logging.Logger
        Logger instance for tracking info and errors.
    overwrite : bool, optional
        If True, re-processes and overwrites existing files. Defaults to False.
    """
    # 1. unzip into single variable files
    with zipfile.ZipFile(download_file_multvar, "r") as zip_ref:
        zip_ref.extractall(work_all_path)

    for v, var in enumerate(var_list_short):
        proc_archive = (
            f"{proc_path}/{era5_info[var]['cmip_name']}/day/native/{year}"
        )
        os.makedirs(proc_archive, exist_ok=True)
        work_path = f"{work_all_path}/{var}/"
        os.makedirs(work_path, exist_ok=True)

        # Find file name for the variable, which should be the same as the long name in era5_info
        var_long = era5_info[var]["long_name"]
        file = f"{work_all_path}/{var_long}_0_{statistic_string}.nc"

        if not os.path.isfile(file):
            logger.error(
                f"Expected file {file} for variable {var} not found after unzipping."
            )
            logger.error(
                "Check if the variable long name in era5_info matches the file name in the zip file."
            )
            sys.exit(1)

        # 2. process each variable separately, with renaming and unit conversion if necessary
        outfile = f"{proc_archive}/{era5_info[var]['cmip_name']}_day_{dataname}_{year}{month}.nc"
        if os.path.isfile(outfile) and not overwrite:
            logger.info(
                f"File {outfile} already exists and overwrite is set to False."
            )
            logger.info(
                f"Skipping processing of variable {var} for month {month}."
            )
            continue

        tmp_outfile = convert_valid_time_latitude_longitude(
            file, work_path, era5_info[var], dataname, year, month
        )

        outfile_name = convert_era5_to_cmip(
            tmp_outfile,
            outfile,
            store,
            era5_info[var],
            config["chunking"]["time_chk"],
            config["chunking"]["lon_chk"],
            config["chunking"]["lat_chk"],
        )

        assert (
            outfile_name == outfile
        ), f"Output file name {outfile_name} does not match expected file name {outfile}."

        if not os.path.isfile(outfile_name) or os.path.getsize(outfile_name) == 0:
            logger.error(
                f"Output file {outfile_name} after conversion to cmip format was not created successfully."
            )
            sys.exit(1)
        else:
            logger.info(f"File {outfile_name} written.")
            os.remove(file)
            logger.info(f"Temporary file {file} removed.")
            os.remove(tmp_outfile)
            logger.info(f"Temporary file {tmp_outfile} removed.")

        # 3. calculate monthly mean
        outfile_mon = calc_mon_mean(proc_archive, outfile_name)
        if not os.path.isfile(outfile_mon) or os.path.getsize(outfile_mon) == 0:
            logger.error(f"Output file {outfile_mon} not created successfully.")
            sys.exit(1)
        else:
            logger.info(f"File {outfile_mon} written successfully.")


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
    statistic = config['variables']['statistic']

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

    statistic_string = statistic.replace("_", "-")

    # download and process for all years in configuration
    for year in range(startyr, endyr + 1):
        logger.info(f"Processing year {year}.")
        logger.info(f"Copying variables {var_list_short} from {store}.")

        varlist_long = [era5_info[var]['long_name'] for var in var_list_short]
        logger.info(f"Variable list for download: {varlist_long}")
        if store == 'cds':
            for month in months:
                # reteive data from cds, returns path to downloaded file, all variables in one file
                download_file_multvar = download_data_cds_multvar(
                    dataname,
                    varlist_long,
                    download_path,
                    year,
                    month,
                    statistic,
                    overwrite=overwrite)
                # process the downloaded file
                process_multvar_era5_data(
                    download_file_multvar,
                    work_all_path,
                    var_list_short,
                    proc_path,
                    era5_info,
                    year,
                    month,
                    statistic_string,
                    dataname,
                    config,
                    store,
                    logger,
                    overwrite=overwrite
                )

            download_success = f"Data download successful and processing completed!"
        else:
            download_success = f"Warning, download from store {store} not implemented."
        logger.info(download_success)

        # clean up work directory after processing all months for the year
        if os.path.isdir(work_all_path):
            logger.info(f"Cleaning up work directory {work_all_path} after processing year {year}.")
            shutil.rmtree(work_all_path)
            logger.info(f"Work directory {work_all_path} removed.")


if __name__ == "__main__":
    main()