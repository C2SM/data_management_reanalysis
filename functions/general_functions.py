import os

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


def convert_tcc(tcc_outfile, workdir, data_info, dataname, year, month):
    if (data_info["unit"]=='(0 - 1)' ) and data_info["cmip_unit"]=="%":
        os.system(
            f'cdo -b F64 mulc,100 {tcc_outfile} {workdir}/{data_info["short_name"]}_{dataname.lower()}_{year}{month}_mulc.nc'
        )
        os.system(f'rm {tcc_outfile}')
        os.system(
            f'ncatted -a units,{data_info["short_name"]},m,c,"{data_info["cmip_unit"]}" {workdir}/{data_info["short_name"]}_{dataname.lower()}_{year}{month}_mulc.nc {tcc_outfile}'
        )
    else:
        logger.error(f'Not implemented error. Conversion for conversion from {data_info["unit"]} to {data_info["cmip_unit"]} not available.')

    return tcc_outfile


def convert_tp(tp_outfile, workdir, data_info, dataname, year, month):
    if (data_info["unit"]=='m' or data_info["unit"]=='kg m-2') and data_info["cmip_unit"]=="kg m-2 s-1":
        os.system(
            f'cdo -b F64 divc,86400 {tp_outfile} {workdir}/{data_info["short_name"]}_{dataname.lower()}_{year}{month}_tmp.nc'
        )
        os.system(f'rm {tp_outfile}')
        os.system(
            f'ncatted -a units,{data_info["short_name"]},m,c,"{data_info["cmip_unit"]}" {workdir}/{data_info["short_name"]}_{dataname.lower()}_{year}{month}_tmp.nc {tp_outfile}'
        )
    else:
        logger.error(f'Not implemented error. Conversion for conversion from {data_info["unit"]} to {data_info["cmip_unit"]} not available.')

    return tp_outfile


def convert_radiation(rad_outfile, workdir, data_info, dataname, year, month):
    if (data_info["unit"]=='J m-2' ) and data_info["cmip_unit"]=="W m-2":
        os.system(
            f'cdo -b F64 divc,86400 {rad_outfile} {workdir}/{data_info["short_name"]}_{dataname.lower()}_{year}{month}_divc.nc'
        )
        os.system(f'rm {rad_outfile}')
        os.system(
            f'ncatted -a units,{data_info["short_name"]},m,c,"{data_info["cmip_unit"]}" {workdir}/{data_info["short_name"]}_{dataname.lower()}_{year}{month}_divc.nc {rad_outfile}'
        )
    else:
        logger.error(f'Not implemented error. Conversion for conversion from {data_info["unit"]} to {data_info["cmip_unit"]} not available.')

    return rad_outfile