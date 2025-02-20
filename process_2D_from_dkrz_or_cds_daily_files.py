#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File Name : process_2D_frm_dkrz_daily_files.py
Author: Ruth Lorenz (ruth.lorenz@c2sm.ethz.ch)
Created: 08/12/2023
Purpose: process ERA5 data downloaded from dkrz
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


def convert_month_list(config_month_list):
    """
    Convert comma separated month numbers from config file into list

    Input:
        01, 02, 03,

    Returns:
        list: ["01", "02", "03"]
    """

    try:
        if "," in config_month_list:
            config_month_list = config_month_list.replace('"', "").strip()
            month_list = [item.strip() for item in config_month_list.split(",")]
        else:
            month_list = [f"{config_month_list}"]
    except TypeError:
        month_list = ["{:02d}".format(config_month_list)]

    return month_list

def download_data_dkrz(freq, era5_info, origin, iac_path, year, months, all_months, family, level):
    """
    Download data from DKRZ

    Return:
    name of the grib file
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
    if all_months:
        files_to_copy = f'{dkrz_path}/*_{year}-??_{vparam}.*'
        os.system(
            f"rsync -av levante:{files_to_copy} {iac_path}"
        )
    else:
        for month in months:
            files_to_copy = f'{dkrz_path}/*_{year}-{month}_{vparam}.*'
            os.system(
                f"rsync -av levante:{files_to_copy} {iac_path}"
            )
    gribfile = f'{iac_path}{family}{level}{typeid}_{freq}_{year}-MM_{vparam}.grb'
    return gribfile


def download_data_cds(era5_info, origin, workdir, year, months, overwrite):
    """
    Download data from CDS

    Returns:
    Name of the netcdfs in general form (MM instead single months)

    """
    import cdsapi

    longname = era5_info["long_name"]

    target_allg = f'{workdir}/{era5_info["short_name"]}_era5_{year}MM.nc'

    for month in months:

        target = target_allg.replace("MM", month)
        logger.info(f'NetCDFfile to download is {target}')

        dataset = origin

        if not os.path.isfile(f'{target}') or overwrite:
            request = {
                "product_type": "reanalysis",
                "variable": [
                    f'{longname}'
                ],
                "year": f'{year}',
                "month": [f'{month}'],
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
                "daily_statistic": "daily_mean",
                "time_zone": "utc+00:00",
                "frequency": "1_hourly",
            }

            client = cdsapi.Client()
            client.retrieve(dataset, request, target)

    return target_allg


def convert_netcdf_add_era5_info(grib_file, workdir, era5_info, year, month):
    """
    Convert grib file to netcdf

    use grib_to_netcdf, adds meaningful variable name and time dimension
    incl. standard_name and long_name
    """

    tmpfile = f'{workdir}/tmp_var{era5_info["param"]}_era5_{year}{month}'
    tmp_outfile = f'{workdir}/tmp2_{era5_info["short_name"]}_era5_{year}{month}.nc'
    os.system(f"cdo -t ecmwf -setgridtype,regular {grib_file} {tmpfile}.grib")
    os.system(f"grib_to_netcdf -o  {tmpfile}.nc {tmpfile}.grib")
    os.system(f"ncrename -d latitude,lat -d longitude,lon -v latitude,lat -v longitude,lon {tmpfile}.nc {tmp_outfile}")

    return tmp_outfile


def convert_valid_time_latitude_longitude(ncfile, workdir, era5_info, year, month):
    """
    Convert netcdf to same structure as netcdf converted from grib using grib_to_netcdf

    time instead valid_time in units = "hours since ...."
    lon instead longitude
    lat instead latitude
    """

    ds = xr.open_dataset(ncfile)
    print(ds)
    # rename latitude,longitude to lat, lon if necessary
    if "lon" not in ds.dims:
        ds = ds.rename({"longitude": "lon", "latitude": "lat"})
    if "time" not in ds.dims:
        ds = ds.rename({"valid_time": "time"})

    print(time)
    startyear = f"{ds.time[0].dt.year.data}"
    startmonth = f"{ds.time[0].dt.month.data}"
    startday = f"{ds.time[0].dt.day.data}"

    encoding = {
        "time": {"_FillValue": None, "dtype": "d"},
        "lat": {"_FillValue": None},
        "lon": {"_FillValue": None},
    }

    tmp_outfile = f'{workdir}/tmp3_{era5_info["short_name"]}_era5_{year}{month}.nc'

    ds.to_netcdf(tmp_outfile, unlimited_dims="time", encoding=encoding, format='NETCDF4')

    return tmp_outfile


def convert_tcc(tcc_outfile, workdir, era5_info, year, month):
    os.system(
        f'cdo -b F64 mulc,100 {tcc_outfile} {workdir}/{era5_info["short_name"]}_era5_{year}{month}_mulc.nc'
    )
    os.system(f'rm {tcc_outfile}')
    os.system(
        f'ncatted -a units,{era5_info["short_name"]},m,c,"{era5_info["cmip_unit"]}" {workdir}/{era5_info["short_name"]}_era5_{year}{month}_mulc.nc {tcc_outfile}'
    )

    return tcc_outfile


def convert_tp(tp_outfile, workdir, era5_info, year, month):
    os.system(
        f'cdo -b F64 divc,86400 {tp_outfile} {workdir}/{era5_info["short_name"]}_era5_{year}{month}_divc.nc'
    )
    os.system(f'rm {tp_outfile}')
    os.system(
        f'ncatted -a units,{era5_info["short_name"]},m,c,"{era5_info["cmip_unit"]}" {workdir}/{era5_info["short_name"]}_era5_{year}{month}_divc.nc {tp_outfile}'
    )

    return tp_outfile


def convert_radiation(rad_outfile, workdir, era5_info, year, month):
    os.system(
        f'cdo -b F64 divc,86400 {rad_outfile} {workdir}/{era5_info["short_name"]}_era5_{year}{month}_divc.nc'
    )
    os.system(f'rm {rad_outfile}')
    os.system(
        f'ncatted -a units,{era5_info["short_name"]},m,c,"{era5_info["cmip_unit"]}" {workdir}/{era5_info["short_name"]}_era5_{year}{month}_divc.nc {rad_outfile}'
    )

    return rad_outfile


def convert_era5_to_cmip(tmp_outfile, store, proc_archive, era5_info, year, month, time_chk, lon_chk, lat_chk):
    #tmpfile = f'{work_path}/{era5_info["short_name"]}_era5_{year}{month}'
    path_to_tmp = Path(tmp_outfile)
    tmpfile = f'{str(path_to_tmp.parent)}/{path_to_tmp.stem}'
    outfile = f'{proc_archive}/{era5_info["cmip_name"]}_day_era5_{year}{month}.nc'

    if store == 'dkrz':
        os.system(
            f"cdo remapcon,/net/atmos/data/era5_cds/gridfile_cds_025.txt {tmp_outfile} {tmpfile}_remapped.nc"
        )
        os.system(
            f"ncks -O -4 -D 4 --cnk_plc=g3d --cnk_dmn=time,{time_chk} --cnk_dmn=lat,{lat_chk} --cnk_dmn=lon,{lon_chk} -L 1 {tmpfile}_remapped.nc {tmpfile}_chunked.nc"
        )
    else:
        os.system(
            f"ncks -O -4 -D 4 --cnk_plc=g3d --cnk_dmn=time,{time_chk} --cnk_dmn=lat,{lat_chk} --cnk_dmn=lon,{lon_chk} -L 1 {tmp_outfile} {tmpfile}_chunked.nc"
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

    store = config['dataset']['store']

    # variable to be processed
    var = config['variables']['varname']
    freq = config['variables']['freq']
    family = config['variables']['family']
    level = config['variables']['level']

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
    # read ERA5_variables.json
    # -------------------------------------------------
    logger.info("ERA5 variable info red from json file.")
    era5_info = read_era5_info(var)


    # download and process for all years in configuration
    for year in range(startyr, endyr + 1):
        logger.info(f"Processing year {year}.")
        logger.info(f"Copying variable {var}")

        print(store)
        if store == 'dkrz':
            download_file = download_data_dkrz(
                freq=freq, era5_info=era5_info, origin=origin, iac_path=grib_path, year=year, months=months, all_months=all_months, family=family, level=level)
            download_success = f"Data download successful!"
        elif store == 'cds':
            download_file = download_data_cds(
                era5_info=era5_info, origin=origin, workdir=work_path, year=year, months=months, overwrite=overwrite)
            download_success = f"Data download successful!"
        else:
            download_success = f"Warning, download from store {store} not implemented."
        logger.info(download_success)
        print(download_file)

        proc_archive = f'{proc_path}/{era5_info["cmip_name"]}/day/native/{year}'
        os.makedirs(proc_archive, exist_ok=True)
        proc_mon_archive = proc_archive.replace("day", "mon")
        os.makedirs(proc_mon_archive, exist_ok=True)

        for month in months:

            #grib_file = f'{grib_path}{family}{level}{typeid}_{freq}_{year}-{month}_{vparam}.grb'
            file = download_file.replace("MM", month)
            if file.endswith('.grb'):
                tmp_outfile = convert_netcdf_add_era5_info(
                    file, work_path, era5_info, year, month
                )
            else:
                tmp_outfile = convert_valid_time_latitude_longitude(
                    file, work_path, era5_info, year, month)
            print(tmp_outfile)

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
                tmp_outfile, store, proc_archive, era5_info, year, month,
                config['chunking']['time_chk'], config['chunking']['lon_chk'], config['chunking']['lat_chk']
            )
            logger.info(f"File {outfile_name} written.")

            # calculate monthly mean
            outfile_mon = (
                f'{proc_mon_archive}/{era5_info["cmip_name"]}_mon_era5_{year}{month}.nc'
            )
            os.system(f"cdo monmean {outfile_name} {outfile_mon}")

        # -------------------------------------------------
        # Clean up
        # -------------------------------------------------
        os.system(f"rm {work_path}/{var}_*")
        os.system(f"rm {work_path}/tmp*")
        os.system(f"rm {grib_path}/*")


if __name__ == "__main__":
    main()
