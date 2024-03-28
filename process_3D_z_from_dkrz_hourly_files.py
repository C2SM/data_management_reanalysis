#!/usr/bin/env python

# -------------------------------------------------
# Getting libraries and utilities
# -------------------------------------------------
import calendar
import json
import logging
import os
import sys
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


def convert_cc(tmp_outfile, workdir, era5_info, year, month, day_str):
    os.system(
        f'cdo mulc,100 {tmp_outfile} {workdir}/{era5_info["short_name"]}_era5_{year}{month}{day_str}_mulc.nc'
    )
    os.system(f'rm {workdir}/{era5_info["short_name"]}_era5_{year}{month}{day_str}.nc')
    os.system(
        f'ncatted -a units,{era5_info["short_name"]},m,c,"{era5_info["cmip_unit"]}" {workdir}/{era5_info["short_name"]}_era5_{year}{month}{day_str}_mulc.nc {tmp_outfile}'
    )

    return tmp_outfile


def convert_z(tmp_outfile, workdir, era5_info, year, month, day_str):
    # https://confluence.ecmwf.int/display/CKB/ERA5%3A+compute+pressure+and+geopotential+on+model+levels%2C+geopotential+height+and+geometric+height#ERA5:computepressureandgeopotentialonmodellevels,geopotentialheightandgeometricheight-Geopotentialheight
    # Earth's gravitational acceleration [m/s2]
    const = 9.80665
    os.system(
        f'cdo divc,{const} {tmp_outfile} {workdir}/{era5_info["short_name"]}_era5_{year}{month}{day_str}_divc.nc'
    )
    os.system(f"rm {tmp_outfile}")
    os.system(
        f'ncatted -a units,{era5_info["short_name"]},m,c,"{era5_info["cmip_unit"]}" {workdir}/{era5_info["short_name"]}_era5_{year}{month}{day_str}_divc.nc {tmp_outfile}'
    )

    return tmp_outfile


def convert_era5_to_cmip(
    tmp_outfile, outfile, config, era5_info, year, month
):
    tmpfile = f'{config.work_path}/{era5_info["short_name"]}_era5_{year}{month}'

    # extract number of p-levels for chunking
    with xr.open_dataset(f"{tmp_outfile}") as ds:
        plev = ds.sizes["level"]

    os.system(
        f"cdo remapcon,/net/atmos/data/era5_cds/gridfile_cds_025.txt {tmp_outfile} {tmpfile}_remapped.nc"
    )
    os.system(
        f"ncks -O -4 -D 4 --cnk_plc=g3d --cnk_dmn=time,1 --cnk_dmn=level,{plev} --cnk_dmn=lat,{config.lat_chk} --cnk_dmn=lon,{config.lon_chk} {tmpfile}_remapped.nc {tmpfile}_chunked.nc"
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
    # read config file
    cfg = read_config("configs", "era5_dkrz_config_z.ini")
    logger.info(f"Read configuration is: {cfg}")
    print(f"Read configuration is: {cfg}")

    # -------------------------------------------------
    # Create directories if do not exist yet
    # -------------------------------------------------
    os.makedirs(cfg.work_path, exist_ok=True)
    os.makedirs(cfg.path_proc, exist_ok=True)

    for v, var in enumerate(cfg.variables):
        grib_path = f"{cfg.path}/{var}"

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

        # read cmip standard_name and long_name from cmip6-cmor-tables
        cmip_info = read_cmip_info(era5_info["cmip_name"])

        for year in range(cfg.startyr, cfg.endyr + 1):
            logger.info(f"Processing year {year}.")

            t0 = datetime.now()

            logger.info(f"Copying variable {var}")
            vparam = era5_info["param"]

            dkrz_path = f"/pool/data/ERA5/E5/pl/an/{cfg.freq}/{vparam}"

            iac_path = f"{grib_path}"

            os.makedirs(iac_path, exist_ok=True)

            logger.info(f"rsync data from  {dkrz_path} to {iac_path}")
            os.system(
                f"rsync -av levante:{dkrz_path}/E5pl00_{cfg.freq}_{year}-??-??_{vparam}.* {iac_path}"
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
                print(month)
                num_days = calendar.monthrange(year, int(month))[1]
                days = [*range(1, num_days + 1)]
                print(days)
                for day in days:
                    day_str = f"{day:02d}"
                    print(day_str)

                    grib_file = f'{grib_path}/E5pl00_{cfg.freq}_{year}-{month}-{day_str}_{era5_info["param"]}.grb'

                    tmp_outfile = convert_netcdf_add_era5_info(
                        grib_file, cfg.work_path, era5_info, year, month, day_str
                    )

                    # check if unit needs to be changed from era5 variable to cmip variable
                    if era5_info["unit"] != era5_info["cmip_unit"]:
                        if var == "cc":
                            logger.info(
                                f'Unit for cc needs to be changed from {era5_info["unit"]} to {era5_info["cmip_unit"]}.'
                            )

                            tmp_outfile = convert_cc(
                                tmp_outfile,
                                cfg.work_path,
                                era5_info,
                                year,
                                month,
                                day_str,
                            )
                        elif var == "z":
                            logger.info(
                                f'Unit for z needs to be changed from {era5_info["unit"]} to {era5_info["cmip_unit"]}.'
                            )
                            tmp_outfile = convert_z(
                                tmp_outfile,
                                cfg.work_path,
                                era5_info,
                                year,
                                month,
                                day_str,
                            )
                        else:
                            logger.error(
                                f"Conversion of unit for variable {var} is not implemented!"
                            )
                            sys.exit(1)

                    # calculate daily means
                    os.system(
                        f"cdo daymean {tmp_outfile}  {cfg.work_path}/{var}_daymean_era5_{year}{month}{day_str}.nc"
                    )

                    if not os.path.isfile(
                        f"{cfg.work_path}/{var}_daymean_era5_{year}{month}{day_str}.nc"
                    ):
                        logger.warning(
                            f"{cfg.work_path}/{var}_daymean_era5_{year}{month}{day_str}.nc was not processed properly!"
                        )
                    else:
                        # clean up 1-hr data
                        os.system(f"rm {tmp_outfile}")

                # concatenate daily files
                daily_file = f"{cfg.work_path}/{var}_day_era5_{year}{month}.nc"
                os.system(
                    f"cdo mergetime {cfg.work_path}/{var}_daymean_era5_{year}{month}*.nc {daily_file}"
                )

                outfile_name = convert_era5_to_cmip(
                    daily_file, outfile, cfg, era5_info, year, month
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
            os.system(f"rm {grib_path}/*")


if __name__ == "__main__":
    main()
