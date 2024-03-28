#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File Name : .py
Author: Ruth Lorenz (ruth.lorenz@c2sm.ethz.ch)
Created: 29/02/2024
Purpose: copy data from dkrz ERA5 pool to IAC
"""

import json
import logging
import os
from datetime import datetime

# -------------------------------------------------
# Define some variables
# -------------------------------------------------

# variables = ['u', 'v', 'r', 'cc', 'z']
variables = ["cc"]
freq = "1D"
outpath = "/net/nitrogen/c2sm-scratch/rlorenz/era5_dkrz/"

# -------------------------------------------------
# Create a simple logger
# -------------------------------------------------

logging.basicConfig(
    format="%(asctime)s | %(levelname)s : %(message)s", level=logging.INFO
)
logger = logging.getLogger()

# -------------------------------------------------
# Define some functions
# -------------------------------------------------


def read_era5_info(vname):
    """
    Loading ERA5 variables's information as
    python Dictionary from JSON file

    Input:
    a string with the ERA5 variable short name to be processed

    Return:
    variable infos
    """
    era5_info = dict()

    with open("ERA5_variables.json", "r") as jf:
        era5 = json.load(jf)

        param = era5[vname][2]

    return param


def main():
    t0 = datetime.now()
    for v, var in enumerate(variables):
        logger.info(f"Copying variable {var}")
        vparam = read_era5_info(var)

        dkrz_path = f"/pool/data/ERA5/E5/pl/an/{freq}/{vparam}"

        iac_path = f"{outpath}/{var}"

        os.makedirs(iac_path, exist_ok=True)

        logger.info(f"rsync data from  {dkrz_path} to {iac_path}")
        os.system(f"rsync -av levante:{dkrz_path}/* {iac_path}")

    dt = datetime.now() - t0
    logger.info(f"Success! All data copied in {dt}")


if __name__ == "__main__":
    main()
