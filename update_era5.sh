#!/bin/bash
# File Name: update_era5.sh
# Author:  ruth.lorenz@c2sm.ethz.ch
# Created: 05/03/2026
# Modified: Thu Mar  5 12:28:32 2026
# Purpose : update era5 data,
#	either multiple months and years or
#	one month defined by offset_months X months ago
#   defined in the config file, e.g. if current month is 2026-03 and offset_months is 4,
#   then data from 2025-11 will be downloaded and processed

###-------------------------------------------------------
. /etc/profile.d/iac-path.sh
. /etc/profile.d/modules.sh
module load conda
###-------------------------------------------------------
printf -v date '%(%Y-%m-%d_%H%M%S)T' -1
logfile="update_era5_$date.log"
mkdir -p logfiles
{

PYTHON_EXE=/usr/local/Miniconda3-envs/envs/2025/envs/iacpy3_2025/bin/python
$PYTHON_EXE -c "import sys; print(sys.executable)"

# check if git repository is clean, i.e. no uncommitted changes, to avoid that changes are overwritten by the update process
if [[ -n $(git status --porcelain) ]]; then
  echo "There are uncommitted changes in the repository. Please commit and push them before running the update script."
  exit 1
fi


# update daily 2D variables at surface available at DKRZ
variable_list=("tp" "strd" "ssrd" "str" "sst" "msl" "u10" "v10" "2t" "2d" "skt" "sp")
#for var in "${variable_list[@]}"; do
#    echo $var
#    $PYTHON_EXE process_2D_from_dkrz_or_cds_daily_files.py -c configs/Config_era5_1day_sf_dkrz.yaml -v $var
#done
# Run in parallel
echo ${variable_list[@]}
printf "%s\n" "${variable_list[@]}" | parallel -j 64 nice $PYTHON_EXE process_2D_from_dkrz_or_cds_daily_files.py -c configs/Config_era5_1day_sf_dkrz.yaml -v $var {}


# update daily 2D variable tcc=clt at surface (available at DKRZ but v3 needs to be updated insterad v2, v2 contains not cmip standard unit fraction
var="tcc"
nice $PYTHON_EXE process_2D_from_dkrz_or_cds_daily_files.py -c configs/Config_era5_1day_sf_clt_dkrz.yaml -v $var

# update daily 1D variables at surface available only on CDS
variable_list=("cbh")
for var in "${variable_list[@]}"; do
    echo $var
    nice $PYTHON_EXE process_2D_from_dkrz_or_cds_daily_files.py -c configs/Config_era5_1day_sf_cds.yaml -v $var
done

# update daily 2D variables calculated from hourly files at DKRZ,
# e.g. max and min of 2m temperature, which are not available as daily files at DKRZ but can be calculated from hourly files
echo "2t min and max"
nice $PYTHON_EXE process_2D_maxmin_from_dkrz_hourly_files.py -c configs/Config_era5_2t_minmax_dkrz.yaml

# var=sfcWind
nice $PYTHON_EXE process_2D_sfcWind_from_dkrz_hourly_files.py -c configs/Config_era5_1day_sfcWind_dkrz.yaml

# update daily 3D variables available from DKRZ
variable_list=(q r t u v)
#for var in "${variable_list[@]}"; do
#    echo $var
#    $PYTHON_EXE process_3D_from_dkrz_daily_files.py -c configs/Config_era5_1day_pl_dkrz.yaml -v $var
#done
# Run in parallel
echo ${variable_list[@]}
printf "%s\n" "${variable_list[@]}" | parallel -j 64 nice $PYTHON_EXE process_3D_from_dkrz_daily_files.py -c configs/Config_era5_1day_pl_dkrz.yaml -v $var {}

# update z from hourly files at dkrz
var="z"
nice $PYTHON_EXE process_3D_z_from_dkrz_hourly_files.py -c configs/Config_era5_1hr_pl_z_dkrz.yaml -v $var


} 2>&1 | tee logfiles/${logfile}