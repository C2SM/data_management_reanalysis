#!/bin/bash
# File Name: update_era5.sh
# Author:  ruth.lorenz@c2sm.ethz.ch
# Created: 05/03/2026
# Modified: Thu Mar  5 12:28:32 2026
# Purpose : update era5 data,
#	either multiple months and years or
#	one month defined by offset_months X months ago

###-------------------------------------------------------

###-------------------------------------------------------
printf -v date '%(%Y-%m-%d_%H%M%S)T' -1
logfile="update_era5_$date.log"
mkdir -p logfiles
{
export PYTHONPATH=""
module load conda
source activate iacpy3_2025

# update daily 2D variables at surface available at DKRZ
#variable_list=(tp strd ssrd str cbh sst msl u10 v10 2t 2d skt sp)
variable_list=(tp)
for var in "${variable_list[@]}"; do
    python process_2D_from_dkrz_or_cds_daily_files.py -c configs/Config_era5_1day_sf_dkrz.yaml -v "$var"
    #echo "no processing"
done

# update daily 1D variables at surface available only on CDS
variable_list=(tcc)
for var in "${variable_list[@]}"; do
    #python process_2D_from_dkrz_or_cds_daily_files.py -c configs/Config_era5_1day_sf_cds.yaml -v "$var"
    echo "no processing"
done

# update daily 2D variables calculated from hourly files at DKRZ
# variable_list=(tasmax, tasmin)
python process_2D_maxmin_from_dkrz_hourly_files.py -c configs/Config_era5_2t_minmax.yaml

#variable_list=(sfcWind)
python process_2D_sfcWind_from_dkrz_hourly_files.py -c configs/Config_era5_sfcWind.yaml

# update daily 3D variables available from DKRZ
variable_list=(q r t u v)
for var in "${variable_list[@]}"; do
    #python process_3D_from_dkrz_daily_files.py -c configs/Config_era5_1day_pl_dkrz.yaml -v "$var"
    echo "no processing"
done

# update z from hourly files at dkrz
variable_list=(z)
for var in "${variable_list[@]}"; do
    python process_3D_z_from_dkrz_hourly_files.py -c configs/Config_era5_1hr_pl_z_dkrz.yaml -v "$var"
    #echo "no processing"
done

} 2>&1 | tee logfiles/"$logfile"