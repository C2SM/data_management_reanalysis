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
variable_list=("sd", "swvl1", "swvl2", "swvl3", "swvl4", "tp")
# Run in parallel
echo ${variable_list[@]}
printf "%s\n" "${variable_list[@]}" | parallel -j 64 nice $PYTHON_EXE process_2D_from_dkrz_or_cds_daily_files.py -c configs/Config_era5-land_1day_sf_dkrz.yaml -v $var {}

# update daily 2D variables available as daily statistics from CDS
#variable_list=("2d", "2t", "10u", "10v", "sp")
nice $PYTHON_EXE process_2D_analysis_multvars_from_cds_daily.py -c configs/Config_era5-land_daily_multvar_cds.yaml

# update daily 2D variables only available as hourly files at CDS
variable_list=("e", "pev", "smlt", "ssrd", "strd")