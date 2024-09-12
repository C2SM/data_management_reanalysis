#!/usr/bin/env python

# -------------------------------------------------
# Getting libraries and utilities
# -------------------------------------------------
import calendar
import numpy as np
import pandas as pd
import json
import logging
import os
from datetime import datetime

import xarray as xr

from functions.file_util import read_config, read_era5_info

# -------------------------------------------------
# Create a simple logger
# -------------------------------------------------

logging.basicConfig(
    format="%(asctime)s | %(levelname)s : %(message)s", level=logging.INFO
)
logger = logging.getLogger()

def copy_data(config, var, era5_info, year):
    """
    Copy data from DKRZ

    Parameters
    ----------

    Returns
    -------
    iac_path: string
        location of copyied data
    """
    t0 = datetime.now()

    vparam = era5_info["param"]

    dkrz_path = f"/pool/data/ERA5/E5/sf/an/{config.freq}/{vparam}"

    iac_path = f"{config.path}/{var}"

    os.makedirs(iac_path, exist_ok=True)

    logger.info(f"rsync data from  {dkrz_path} to {iac_path}")
    os.system(
        f"rsync -av levante:{dkrz_path}/E5sf00_{config.freq}_{year}-??-??_{vparam}.* {iac_path}"
    )

    dt = datetime.now() - t0
    logger.info(f"Success! All data copied in {dt}")

    return iac_path


def convert_netcdf_add_era5_info(grib_file, workdir, era5_info, year, month, day_str):
    """
    Convert grib file to netcdf

    use grib_to_netcdf, adds meaningful variable name and time dimension
    incl. standard_name and long_name
    """

    tmpfile = f'{workdir}/tmp_var{era5_info["param"]}_era5'
    tmp_outfile = f'{workdir}/{era5_info["short_name"]}_era5_{year}{month}{day_str}.nc'
    os.system(f"cdo -t ecmwf -setgridtype,regular {grib_file} {tmpfile}.grib")
    os.system(f"grib_to_netcdf -o  {tmp_outfile} {tmpfile}.grib")

    return tmp_outfile


def compute_wind_from_u_v(u_comp, v_comp):    
    '''
    Calculates Wind Speed from U and V components

    Input:
    u_comp: U Wind component
    v_comp: V Wind component

    Returns:
    Wind speed in m s-1 
    '''

    u_square = np.square(u_comp)
    v_square = np.square(v_comp)
    uv_sum = np.add(u_square, v_square)
                
    sfcW = np.sqrt(uv_sum)

    return sfcW

def calc_wind_daily(infile1, info1, infile2, info2, wind_file, year, month, day_str):
    ds_1 = xr.open_dataset(infile1, mask_and_scale=True)
    ds_2 = xr.open_dataset(infile2, mask_and_scale=True)

    logger.info('Calculating variable sfcWind')
    da_hourly = compute_wind_from_u_v(ds_1[info1["short_name"]], ds_2[info2["short_name"]])

    da_daily = da_hourly.mean(dim='time')

    return da_daily


def convert_era5_to_cmip(
    tmp_outfile, varout, outfile, config, year, month
):
    tmpfile = f'{config.work_path}/{varout}_era5_{year}{month}'

    os.system(
        f"cdo remapcon,/net/atmos/data/era5_cds/gridfile_cds_025.txt {tmp_outfile} {tmpfile}_remapped.nc"
    )
    os.system(
        f"ncks -O -4 -D 4 --cnk_plc=g3d --cnk_dmn=time,1 --cnk_dmn=lat,{config.lat_chk} --cnk_dmn=lon,{config.lon_chk} {tmpfile}_remapped.nc {outfile}"
    )

    return outfile


def calc_mon_mean(path_proc, infile, varout, year, month):
    proc_mon_archive = (
        f'{path_proc}/{varout}/mon/native/{year}'
    )
    os.makedirs(proc_mon_archive, exist_ok=True)
    outfile_mon = (
        f'{proc_mon_archive}/{varout}_mon_era5_{year}{month}.nc'
    )
    os.system(f"cdo monmean {outfile_name} {outfile_mon}")

# -------------------------------------------------


def main():
    # -------------------------------------------------
    # Read config
    # -------------------------------------------------
    # read config file
    cfg = read_config("configs", "era5_dkrz_config_sfcWind.ini")
    logger.info(f"Read configuration is: {cfg}")

    # -------------------------------------------------
    # Create directories if do not exist yet
    # -------------------------------------------------
    os.makedirs(cfg.work_path, exist_ok=True)
    os.makedirs(cfg.path_proc, exist_ok=True)

    standard_name="wind_speed"
    long_name="Near-Surface Wind Speed"
    unit="m s-1"

    dict_attr ={"standard_name": standard_name, "long_name": long_name, "units": unit,
				"_FillValue": 1.e+20}

    varout = cfg.varout
    var1 = cfg.variable1
    var2 = cfg.variable2

    logger.info(f'Processing variable {varout} from {var1} and {var2}:')
    # read ERA5_variables.json
    era5_info1 = read_era5_info(var1)

    logger.info("ERA5 variable info red from json file.")
    logger.info(f'longname: {era5_info1["long_name"]},')
    logger.info(f'unit: {era5_info1["unit"]},')
    logger.info(f'parameterid: {era5_info1["param"]},')

    era5_info2 = read_era5_info(var2)
    logger.info(f'longname: {era5_info2["long_name"]},')
    logger.info(f'unit: {era5_info2["unit"]},')
    logger.info(f'parameterid: {era5_info2["param"]},')

    for year in range(cfg.startyr, cfg.endyr + 1):
        logger.info(f"Processing year {year}.")

        logger.info(f"Copying variable {var1} and {var2}.")
        grib_path1 = copy_data(cfg, var1, era5_info1, year)
        grib_path2 = copy_data(cfg, var2, era5_info2, year)

        proc_archive = f'{cfg.path_proc}/{varout}/day/native/{year}'
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
                f'{proc_archive}/{varout}_day_era5_{year}{month}.nc'
            )

            num_days = calendar.monthrange(year, int(month))[1]
            days = [*range(1, num_days + 1)]

            wind_file = f'{cfg.work_path}/sfcWind_{cfg.freq}_era5_{year}{month}'

            daily_list=list()
            for day in days:
                day_str = f"{day:02d}"

                grib_file1 = f'{grib_path1}/E5sf00_{cfg.freq}_{year}-{month}-{day_str}_{era5_info1["param"]}.grb'

                tmp_outfile1 = convert_netcdf_add_era5_info(
                    grib_file1, cfg.work_path, era5_info1, year, month, day_str
                )

                grib_file2 = f'{grib_path2}/E5sf00_{cfg.freq}_{year}-{month}-{day_str}_{era5_info2["param"]}.grb'

                tmp_outfile2 = convert_netcdf_add_era5_info(
                    grib_file2, cfg.work_path, era5_info2, year, month, day_str
                )

                daily_wind = calc_wind_daily(tmp_outfile1, era5_info1, tmp_outfile2, era5_info2, wind_file, year, month, day_str)
                print(daily_wind)
                daily_list.append(daily_wind)

            # concatenate daily files
            daily_file = f"{cfg.work_path}/{varout}_day_era5_{year}{month}.nc"
            da_daily_wind = xr.concat(daily_list, dim='time')

            dates = pd.date_range(start=f'{year}-{month}-01', periods=num_days, freq='D')

            ds_out = da_daily_wind.assign_attrs(dict_attr).assign_coords(time=dates).to_dataset(name=varout)

            ds_out.to_netcdf(daily_file, encoding={varout: {"chunksizes": (cfg.time_chk, cfg.lat_chk, cfg.lon_chk)}})
            logger.info("Data written to %s", daily_file)

            outfile_name = convert_era5_to_cmip(
                daily_file, varout, outfile, cfg, year, month
            )

            logger.info(f"File {outfile_name} written.")

        # calculate monthly mean
        proc_mon_archive = (
            f'{cfg.path_proc}/{varout}/mon/native/{year}'
        )
        os.makedirs(proc_mon_archive, exist_ok=True)
        outfile_mon = (
            f'{proc_mon_archive}/{varout}_mon_era5_{year}{month}.nc'
        )
        os.system(f"cdo monmean {outfile_name} {outfile_mon}")

        logger.info(f"File {outfile_mon} written.")

        # -------------------------------------------------
        # Clean up
        # -------------------------------------------------
        #os.system(f"rm {cfg.work_path}/{varout}_*")
        #os.system(f"rm {cfg.work_path}/{var}_*")
        #os.system(f"rm {grib_path}/*")


if __name__ == "__main__":
    main()
