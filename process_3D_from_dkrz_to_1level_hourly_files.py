#!/usr/bin/env python

# -------------------------------------------------
# Getting libraries and utilities
# -------------------------------------------------
import os
import sys
import yaml
import logging
from datetime import datetime
import argparse
import calendar
import xarray as xr
from cdo import Cdo

from functions.file_util import read_era5_info
from functions.read_config import read_yaml_config
from functions.general_functions import *

cdo = Cdo()

# Define logfile and logger
seconds = time.time()
local_time = time.localtime(seconds)
# Name the logfile after first of all inputs
LOG_FILENAME = (
    f"logfiles/logging_ERA5_dkrz_pl_hourly"
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


def convert_era5_to_cds(tmp_outfile, store, proc_archive, era5_info, dataname, year, month, num_level, time_chk, lon_chk, lat_chk):
    path_to_tmp = Path(tmp_outfile)
    tmpfile = f'{str(path_to_tmp.parent)}/{path_to_tmp.stem}'
    outfile = f'{proc_archive}/{era5_info["cmip_name"]}_1hr_{dataname}_{year}{month}_p{num_level}.nc.nc'

    cdo.remapcon("/net/atmos/data/era5_cds/gridfile_cds_025.txt", input=tmp_outfile, output=f"{tmpfile}_remapped.nc")

    try:
        cmd = [
            "ncks",
            "-O", "-4", "-D", "4",
            "--cnk_plc=g3d",
            f"--cnk_dmn=time,{time_chk}",
            f"--cnk_dmn=lat,{lat_chk}",
            f"--cnk_dmn=lon,{lon_chk}",
            "-L", "1",
            f"{tmpfile}_remapped.nc",
            f"{tmpfile}_chunked.nc"
        ]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with return code {e.returncode}")
        print(f"Standard output:\n{e.stdout}")

    # check if outfile was created successfully
    if not os.path.isfile(f"{outfile}"):
        logger.error(f"Output file {outfile} was not created successfully.")
        sys.exit(1)

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
    num_level = config['variables']['num_level']
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

    for year in range(startyr, endyr + 1):
        logger.info(f"Processing year {year}.")

        print(store)
        if store == 'dkrz':
            download_file = download_data_dkrz(
                freq=freq, era5_info=era5_info, origin=origin, iac_path=grib_path, year=year, months=months, all_months=all_months, family=family, level=level)
            download_success = f"Data download successful!"
        else:
            download_success = f"Warning, download from store {store} not implemented."
        logger.info(download_success)
        print(download_file)

        proc_archive = f'{proc_path}/{var}/1hr/native/{year}'
        os.makedirs(proc_archive, exist_ok=True)

        for month in months:
            num_days = calendar.monthrange(year, int(month))[1]
            days = [*range(1, num_days + 1)]
            for day in days:
                day_str = f"{day:02d}"
                print(day_str)

                #grib_file = f'{grib_path}/E5pl00_{freq}_{year}-{month}-{day_str}_{era5_info["param"]}.grb'
                grib_file = download_file.replace("MM", month).replace("DD", day_str)

                tmp_outfile = convert_netcdf_add_era5_info(
                    grib_file, work_path, era5_info, dataname, year, month, day=day_str
                )
                print(tmp_outfile)
                # extract level, numeric value given in config by num_level
                cdo.sellevel(num_level, input=tmp_outfile, output=f"{work_path}/{var}_{year}-{month}-{day_str}_p{num_level}.nc")

                # check of output file exists and is not empty, if so remove the grib file and the temporary netcdf file
                if os.path.exists(f"{work_path}/{var}_{year}-{month}-{day_str}_p{num_level}.nc") and os.path.getsize(f"{work_path}/{var}_{year}-{month}-{day_str}_p{num_level}.nc") > 0:
                    logger.info(f"File {work_path}/{var}_{year}-{month}-{day_str}_p{num_level}.nc created successfully.")
                    os.system(f"rm {tmp_outfile}")
                    os.system(f"rm {grib_file}")
                else:
                    logger.warning(f"File {work_path}/{var}_{year}-{month}-{day_str}_p{num_level}.nc not created or empty.")

            tmp_outfile2 = cdo.mergetime("-b 64",input=f"{work_path}/{var}_{year}-{month}-*_p{num_level}.nc")
            outfile = convert_era5_to_cds(tmp_outfile2, store, proc_archive, era5_info, dataname, year, month, num_level, time_chk, lon_chk, lat_chk)

            if os.path.exists(outfile) and os.path.getsize(outfile) > 0:
                logger.info(f"File {outfile} created successfully.")
                # remove the single day files
                os.system(f"rm {work_path}/{var}_{year}-{month}-*_p{num_level}.nc")
            else:
                logger.warning(f"File {outfile} not created or empty.")


if __name__ == "__main__":
    main()