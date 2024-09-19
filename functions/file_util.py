#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File Name : file_util.py
Author: Ruth Lorenz (ruth.lorenz@c2sm.ethz.ch)
Created: 27/03/2024
Purpose: utility functions to process ERA5 data downloaded from dkrz
"""
import json
import os


class Config:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


def read_config(configfolder, configfile):
    config = {}
    type_mapping = {"True": True, "False": False}
    with open(os.path.join(os.getcwd(), configfolder, configfile), "r") as f:
        for line in f:
            if "=" in line:
                k, v = line.split("=", 1)
                v = v.replace('"', "").strip()
                if "," in v:
                    v = [item.strip() for item in v.split(",")]
                elif " " in v:
                    v = v.split(" ")
                else:
                    v = type_mapping.get(v, int(v) if v.isdigit() else v)
                config[k.strip()] = v

    return Config(**config)


# -------------------------------------------------
# Read ERA5 info from JSON file
# -------------------------------------------------


def read_era5_info(vname):
    """
    Loading ERA5 variables's information as
    python Dictionary from JSON file

    Input:
    a string with the ERA5 variable short name to be processed

    Return:
    dict with variable infos
    """
    era5_info = dict()

    with open("ERA5_variables.json", "r") as jf:
        era5 = json.load(jf)
        # Variable's long-name, param and unit
        vlong = era5[vname][0]
        vunit = era5[vname][1]
        vparam = era5[vname][2]
        analysis = era5[vname][4]
        vcmip = era5[vname][6]
        unitcmip = era5[vname][7]

        era5_info["short_name"] = vname
        era5_info["long_name"] = vlong
        era5_info["unit"] = vunit
        era5_info["param"] = vparam
        era5_info["analysis"] = analysis
        era5_info["cmip_name"] = vcmip
        era5_info["cmip_unit"] = unitcmip

    return era5_info


def get_checkpoint(work_dir):
    checkpoint_dir = os.path.join(work_dir, 'checkpoints')
    os.makedirs(checkpoint_dir, exist_ok=True)
    return checkpoint_dir