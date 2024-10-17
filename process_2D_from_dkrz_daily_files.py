#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File Name : process_2D_frm_dkrz_daily_files.py
Author: Ruth Lorenz (ruth.lorenz@c2sm.ethz.ch)
Created: 08/12/2023
Purpose: process ERA5 data downloaded from dkrz
        to variable names and units as in cmip
"""

import logging

# -------------------------------------------------
# Getting libraries and utilities
# -------------------------------------------------
import os
import sys
import time
from datetime import datetime

from functions.file_util import read_config, read_era5_info

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


def convert_era5_to_cmip(tmp_outfile, outfile, config, era5_info, year, month):
    tmpfile = f'{config.work_path}/{era5_info["short_name"]}_era5_{year}{month}'

    os.system(
        f"cdo remapcon,/net/atmos/data/era5_cds/gridfile_cds_025.txt {tmp_outfile} {tmpfile}_remapped.nc"
    )
    os.system(
        f"ncks -O -4 -D 4 --cnk_plc=g3d --cnk_dmn=time,1 --cnk_dmn=lat,{config.lat_chk} --cnk_dmn=lon,{config.lon_chk} {tmpfile}_remapped.nc {tmpfile}_chunked.nc"
    )
    os.system(
        f'ncrename -O -v {era5_info["short_name"]},{era5_info["cmip_name"]} {tmpfile}_chunked.nc {outfile}'
    )

    return outfile


# -------------------------------------------------


def main():
    # -------------------------------------------------
    # Read config
    # -------------------------------------------------
    cfg = read_config("configs", "era5_2D_dkrz_config.ini")
    logger.info(f"Read configuration is: {cfg}")
    print(f"Read configuration is: {cfg}")

    # -------------------------------------------------
    # Create directories if do not exist yet
    # -------------------------------------------------
    os.makedirs(cfg.work_path, exist_ok=True)
    os.makedirs(cfg.path_proc, exist_ok=True)

    for v, var in enumerate(cfg.variables):
        grib_path = f"{cfg.path}/{var}/"

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

        for year in range(cfg.startyr, cfg.endyr + 1):
            logger.info(f"Processing year {year}.")

            t0 = datetime.now()

            logger.info(f"Copying variable {var}")
            param = int(era5_info["param"])
            vparam = f"{param:03}"

            if int(era5_info["analysis"]) == 1:
                type = "an"
                typeid = "00"
            else:
                type = "fc"
                typeid = "12"

            dkrz_path = f"/pool/data/ERA5/E5/sf/{type}/{cfg.freq}/{vparam}"

            iac_path = f"{grib_path}"

            os.makedirs(iac_path, exist_ok=True)

            logger.info(f"rsync data from  {dkrz_path} to {iac_path}")
            os.system(
                f"rsync -av levante:{dkrz_path}/E5sf{typeid}_{cfg.freq}_{year}-??_{vparam}.* {iac_path}"
            )

            dt = datetime.now() - t0
            logger.info(f"Success! All data copied in {dt}")

            proc_archive = f'{cfg.path_proc}/{era5_info["cmip_name"]}/day/native/{year}'
            os.makedirs(proc_archive, exist_ok=True)

            for month in [
                "01",
                "02",
                "03",
                "04",
                "05",
                "06",
                "07",
                "08",
                "09",
                "10",
                "11",
                "12",
            ]:
                outfile = (
                    f'{proc_archive}/{era5_info["cmip_name"]}_day_era5_{year}{month}.nc'
                )

                grib_file = f'{grib_path}E5sf{typeid}_{cfg.freq}_{year}-{month}_{vparam}.grb'

                tmp_outfile = convert_netcdf_add_era5_info(
                    grib_file, cfg.work_path, era5_info, year, month
                )

                # check if unit needs to be changed from era5 variable to cmip variable
                if era5_info["unit"] != era5_info["cmip_unit"]:
                    logger.info(
                        f'Unit for {era5_info["short_name"]} needs to be changed from {era5_info["unit"]} to {era5_info["cmip_unit"]}.'
                    )
                    if var == "tcc":
                        tmp_outfile = convert_tcc(
                            tmp_outfile, cfg.work_path, era5_info, year, month
                        )
                    elif var == "tp":
                        tmp_outfile = convert_tp(
                            tmp_outfile, cfg.work_path, era5_info, year, month
                        )
                    elif var == "ssrd" or var == "strd" or var == "str":
                        tmp_outfile = convert_radiation(
                            tmp_outfile, cfg.work_path, era5_info, year, month
                        )
                    else:
                        logger.error(
                            f"Conversion of unit for variable {var} is not implemented!"
                        )
                        sys.exit(1)

                outfile_name = convert_era5_to_cmip(
                    tmp_outfile, outfile, cfg, era5_info, year, month
                )

                logger.info(f"File {outfile_name} written.")

                # calculate monthly mean
                proc_mon_archive = (
                    f'{cfg.path_proc}/{era5_info["cmip_name"]}/mon/native/{year}'
                )
                os.makedirs(proc_mon_archive, exist_ok=True)
                outfile_mon = (
                    f'{proc_mon_archive}/{era5_info["cmip_name"]}_mon_era5_{year}{month}.nc'
                )
                os.system(f"cdo monmean {outfile_name} {outfile_mon}")

            # -------------------------------------------------
            # Clean up
            # -------------------------------------------------
            os.system(f"rm {cfg.work_path}/{var}_*")
            os.system(f"rm {cfg.work_path}/tmp_{var}_*")
        os.system(f"rm {grib_path}/*")


if __name__ == "__main__":
    main()
