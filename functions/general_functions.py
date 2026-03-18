import os
import sys
import xarray as xr
import numpy as np
import re
import time
import logging
import subprocess
from cdo import Cdo
from pathlib import Path

cdo = Cdo(debug=True)

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
        files_to_copy = f'{dkrz_path}/*_{year}-*_{vparam}.grb'
        try:
            cmd = [
                "rsync",
                "-av",
                f"levante:{files_to_copy}",
                f"{iac_path}"
            ]
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            print(f"Command failed with return code {e.returncode}")
            print(f"Standard output:\n{e.stdout}")
            print(f"Standard error:\n{e.stderr}")
    else:
        for month in months:
            files_to_copy = f'{dkrz_path}/*_{year}-{month}*_{vparam}.grb'
            try:
                cmd = [
                    "rsync",
                    "-av",
                    f"levante:{files_to_copy}",
                    f"{iac_path}"
                ]
                result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError as e:
                print(f"Command failed with return code {e.returncode}")
                print(f"Standard output:\n{e.stdout}")
                print(f"Standard error:\n{e.stderr}")
    if freq == "1D":
        gribfile = f'{iac_path}{family}{level}{typeid}_{freq}_{year}-MM_{vparam}.grb'
    elif freq == "1H":
        gribfile = f'{iac_path}{family}{level}{typeid}_{freq}_{year}-MM-DD_{vparam}.grb'
    return gribfile


def download_data_cds(dataname, era5_info, origin, workdir, year, months, overwrite, statistic="daily_mean"):
    """
    Download data from CDS

    Returns:
    Name of the netcdfs in general form (MM instead single months)

    """
    import cdsapi

    longname = era5_info["long_name"]

    target_allg = f'{workdir}/{era5_info["short_name"]}_{dataname}_{year}MM.nc'

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
                "daily_statistic": statistic,
                "time_zone": "utc+00:00",
                "frequency": "1_hourly",
            }

            client = cdsapi.Client()
            client.retrieve(dataset, request, target)

    return target_allg


def py_grib_to_netcdf(
    input_grib: str,
    output_nc: str,
    variable_name: str,
    calendar: str = "proleptic_gregorian",
    ):
    """
    Convert a GRIB file to NetCDF while preserving the exact time axis.
    Time is encoded as numeric hours since the GRIB reference time.
    """
    start = time.perf_counter()

    # --- Step 1: Load GRIB
    ds = xr.open_dataset(input_grib,
                         engine="cfgrib",
                         backend_kwargs={"filter_by_keys": {"shortName": variable_name}})

    # --- Step 2: Get GRIB time units
    if "GRIB_units" in ds.time.attrs:
        units = ds.time.attrs["GRIB_units"]
    else:
        # fallback
        t0 = np.datetime64(ds.time.values.min(), "s")
        units = f"hours since {str(t0).replace('T',' ')}"
        print(f"[INFO] Using inferred units: {units}")

    # --- Step 3: extract reference datetime from units
    m = re.search(r"(\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2})?)", units)
    if not m:
        raise ValueError(f"Could not parse reference time from units: {units}")

    ref_str = m.group(1)
    ref_dt64 = np.datetime64(ref_str.replace(" ", "T"))

    # --- Step 4: convert datetime64 → numeric hours
    hours = (ds["valid_time"] - ref_dt64) / np.timedelta64(1, "h")
    hours = hours.astype("float64")

    # --- Step 5: Replace time variable
    ds = ds.assign_coords(time=hours)

    if "valid_time" in ds:
        ds = ds.drop_vars("valid_time")

    # --- Step 6: Set units + calendar as **attributes**
    ds.attrs["Conventions"] = "CF-1.10"
    ds["time"].attrs["standard_name"] = "time"
    ds["time"].attrs["units"] = units
    ds["time"].attrs["calendar"] = calendar
    ds["time"].attrs["long_name"] = "valid time"

    # time, lat, lon encoding without FillValues
    encoding = {variable_name: {"zlib": False, "shuffle": False},
        "time": {"_FillValue": None, "dtype": "d"},
        "longitude": {"_FillValue": None},
        "latitude": {"_FillValue": None}
    }

    # --- Step 7: Save NetCDF
    ds.to_netcdf(output_nc, unlimited_dims='time', engine="h5netcdf", encoding=encoding)

    logger.info(f"Wrote NetCDF: {output_nc}")
    logger.debug(f"Time units: {units}")
    logger.debug(f"Calendar:   {calendar}")
    logger.debug(f"First times: {hours[:4]} ...")

    end = time.perf_counter()
    elapsed_time = end - start

    logger.debug(f"grib converted to netcdf in {elapsed_time:.6f} seconds.")
    return



def convert_tcc(tcc_outfile, workdir, data_info, dataname, year, month):
    #tmpfile_mulc = f'{workdir}/{data_info["short_name"]}_{dataname.lower()}_{year}{month}_mulc.nc'
    if (data_info["unit"]=='(0 - 1)' ) and data_info["cmip_unit"]=="%":
        logger.info(f"Converting from {data_info['unit']} to {data_info['cmip_unit']} for variable {data_info['short_name']}.")
        try:
            tmpfile_mulc = cdo.mulc("100", options="-b F64",
                    input=tcc_outfile)
            print(f"CDO mulc result: {tmpfile_mulc}")
        except RuntimeError as e:
            print(f"CDO execution failed!")
            print(f"Error details: {e}")
        # check if tmpfile_mulc was created successfully
        if not os.path.isfile(f"{tmpfile_mulc}"):
            logger.error(f"Output file {tmpfile_mulc} was not created successfully.")
            sys.exit(1)

        # remove incoming file to be overwritten with unit conversion file
        try:
            cmd = [
                "rm",
                f"{tcc_outfile}"
            ]
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed with return code {e.returncode}")
            logger.error(f"Standard output:\n{e.stdout}")

        # set units attribute to cmip unit
        try:
            cmd = [
                "ncatted",
                "-a",
                f"units,{data_info["short_name"]},m,c,{data_info["cmip_unit"]}",
                f"{tmpfile_mulc}",
                f"{tcc_outfile}"
            ]
            results = subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed with return code {e.returncode}")
            logger.error(f"Standard output:\n{e.stdout}")
    else:
        logger.error(f'Not implemented error. Conversion for conversion from {data_info["unit"]} to {data_info["cmip_unit"]} not available.')

    # check if tcc_outfile was created successfully
    if not os.path.isfile(f"{tcc_outfile}"):
        logger.error(f"Output file {tcc_outfile} was not created successfully.")
        sys.exit(1)

    return tcc_outfile


def convert_tp(tp_outfile, workdir, data_info, dataname, year, month):
    if (data_info["unit"]=='m' or data_info["unit"]=='kg m-2') and data_info["cmip_unit"]=="kg m-2 s-1":
        logger.info(f"Converting from {data_info['unit']} to {data_info['cmip_unit']} for variable {data_info['short_name']}.")
        try:
            tmpfile_divc = cdo.divc("86400", options="-b F64",
                    input=tp_outfile)
        except RuntimeError as e:
            logger.error(f"CDO execution failed!")
            logger.error(f"Error details: {e}")
        # check if output file was created successfully
        if not os.path.isfile(tmpfile_divc):
            logger.error(f"Output file {tmpfile_divc} was not created successfully.")
            sys.exit(1)

        # remove incoming file to be overwritten with unit conversion file
        try:
            cmd = [
                "rm",
                f"{tp_outfile}"
            ]
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed with return code {e.returncode}")
            logger.error(f"Standard output:\n{e.stdout}")

        # set units attribute to cmip unit
        try:
            cmd = [
                "ncatted",
                "-a",
                f"units,{data_info['short_name']},m,c,{data_info['cmip_unit']}",
                f"{tmpfile_divc}",
                f"{tp_outfile}"]
            results = subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed with return code {e.returncode}")
            logger.error(f"Standard output:\n{e.stdout}")

        # check if tp_outfile was created successfully
        if not os.path.isfile(f"{tp_outfile}"):
            logger.error(f"Output file {tp_outfile} was not created successfully.")
            sys.exit(1)
    else:
        logger.error(f'Not implemented error. Conversion for conversion from {data_info["unit"]} to {data_info["cmip_unit"]} not available.')

    return tp_outfile


def convert_radiation(rad_outfile, workdir, data_info, dataname, year, month):
    if (data_info["unit"]=='J m-2' ) and data_info["cmip_unit"]=="W m-2":
        logger.info(f"Converting from {data_info['unit']} to {data_info['cmip_unit']} for variable {data_info['short_name']}.")
        try:
            tmpfile_divc = cdo.divc("86400", options="-b F64", input=rad_outfile)
        except RuntimeError as e:
            logger.error(f"CDO execution failed!")
            logger.error(f"Error details: {e}")
        # check if output file was created successfully
        if not os.path.isfile(tmpfile_divc):
            logger.error(f"Output file {tmpfile_divc} was not created successfully.")
            sys.exit(1)

        # remove incoming file to be overwritten with unit conversion file
        try:
            cmd = [
                "rm",
                f"{rad_outfile}"
            ]
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed with return code {e.returncode}")
            logger.error(f"Standard output:\n{e.stdout}")

        # set units attribute to cmip unit
        try:
            cmd = [
                "ncatted",
                "-a",
                f"units,{data_info['short_name']},m,c,{data_info['cmip_unit']}",
                f"{tmpfile_divc}",
                f"{rad_outfile}"]
            results = subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed with return code {e.returncode}")
            logger.error(f"Standard output:\n{e.stdout}")
    else:
        logger.error(f'Not implemented error. Conversion for conversion from {data_info["unit"]} to {data_info["cmip_unit"]} not available.')
        sys.exit(1)
    # check if rad_outfile was created successfully
    if not os.path.isfile(f"{rad_outfile}"):
        logger.error(f"Output file {rad_outfile} was not created successfully.")
        sys.exit(1)

    return rad_outfile


def convert_valid_time_latitude_longitude(ncfile, workdir, era5_info, dataname, year, month):
    """
    Convert netcdf to same structure as netcdf converted from grib using grib_to_netcdf

    time instead valid_time in units = "hours since ...."
    lon instead longitude
    lat instead latitude
    """

    ds = xr.open_dataset(ncfile)

    # rename latitude,longitude to lat, lon if necessary
    if "lon" not in ds.dims:
        ds = ds.rename({"longitude": "lon", "latitude": "lat"})
    if "time" not in ds.dims:
        ds = ds.rename({"valid_time": "time"})

    startyear = f"{ds.time[0].dt.year.data}"
    startmonth = f"{ds.time[0].dt.month.data}"
    startday = f"{ds.time[0].dt.day.data}"

    encoding = {
        "time": {"_FillValue": None, "dtype": "d"},
        "lat": {"_FillValue": None},
        "lon": {"_FillValue": None},
    }

    tmp_outfile = f'{workdir}/tmp3_{era5_info["short_name"]}_{dataname}_{year}{month}.nc'

    ds.to_netcdf(tmp_outfile, unlimited_dims="time", encoding=encoding, format='NETCDF4')
    # check if tmp_outfile was created successfully
    if not os.path.isfile(f"{tmp_outfile}"):
         logger.error(f"Output file {tmp_outfile} was not created successfully.")
         sys.exit(1)
    else:
        logger.info('NetCDF file with converted time, lat, lon written.')

    return tmp_outfile


def convert_era5_to_cmip(tmp_outfile, store, proc_archive, era5_info, dataname, year, month, time_chk, lon_chk, lat_chk):
    path_to_tmp = Path(tmp_outfile)
    tmpfile = f'{str(path_to_tmp.parent)}/{path_to_tmp.stem}'
    outfile = f'{proc_archive}/{era5_info["cmip_name"]}_day_{dataname}_{year}{month}.nc'

    if store == 'dkrz':
        try:
            cmd = [
                "cdo",
                "remapcon,/net/atmos/data/era5_cds/gridfile_cds_025.txt",
                f"{tmp_outfile}",
                f"{tmpfile}_remapped.nc"
            ]
            result_remap = subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            print(f"Command failed with return code {e.returncode}")
            print(f"Standard output:\n{e.stdout}")

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
    else:

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
    try:
        cmd = [
            "ncrename",
            "-O",
            "-v",
            f'{era5_info["short_name"]},{era5_info["cmip_name"]}',
            f"{tmpfile}_chunked.nc",
            f"{outfile}"
        ]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with return code {e.returncode}")
        print(f"Standard output:\n{e.stdout}")

        if era5_info["short_name"]=='2t':
            print(f"Try with using t2m instead 2t.")
            try:
                cmd = [
                    "ncrename",
                    "-O",
                    "-v",
                    f't2m,{era5_info["cmip_name"]}',
                    f"{tmpfile}_chunked.nc",
                    f"{outfile}"
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
