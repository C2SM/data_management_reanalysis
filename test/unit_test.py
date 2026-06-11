#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File Name : unit_test.py
Author: Ruth Lorenz (ruth.lorenz@c2sm.ethz.ch)
Created: 28/05/2026
Purpose: unit tests for functions in data_management_reanalysis repo
	testing individual components or functions of code to ensure they work correctly in isolation
"""

import json
from unittest.mock import MagicMock
import pytest

from functions.file_util import parse_args, read_era5_info, read_cerra_info, read_cmip_info
from functions.general_functions import convert_month_list
from functions.read_config import read_yaml_config


# test parse_args with valid input and missing required arguments
def test_parse_args_valid():
    args = parse_args(
        [
            "--configname",
            "config.yaml",
            "--varname",
            "2t",
        ]
    )

    assert args.configname == "config.yaml"
    assert args.varname == "2t"


def test_parse_args_missing_required_args():
    with pytest.raises(SystemExit):
        parse_args([])


# test convert_month_list with different input formats
def test_convert_month_list():
	assert convert_month_list(01, 02) == ["01", "02"]
	assert convert_month_list(10, 11, 12) == ["10", "11", "12"]
	assert convert_month_list("01", "02") == ["01", "02"]


# test read_yaml_config with valid config file and missing file
def test_read_yaml_config_valid():
	config = read_yaml_config("configs/test_config.yaml")
	assert config is not None
	assert "dataset" in config
	assert config["dataset"]["name"] == "ERA5"
	assert config["dataset"]["store"] == "dkrz"

	assert "variables" in config
	assert config["variables"]["freq"] == "1D"
	assert config["variables"]["family"] == "E5"
	assert config["variables"]["level"] == "sf"

	assert "paths" in config
	assert config["paths"]["origin"] == "/pool/data/ERA5/E5/sf/"
	assert config["paths"]["download"] == "/net/stratus/c2sm-data/rlorenz/era5_dkrz"
	assert config["paths"]["work"] == "/net/stratus/c2sm-data/rlorenz/era5_dkrz/work"
	assert config["paths"]["proc"] == "/net/atmos/data/era5_cds/processed/v2/"

	assert "time" in config
	assert config["time"]["months"] == 01
	assert config["time"]["startyr"] == 2025
	assert config["time"]["endyr"] == 2025

	assert "chunking" in config
	assert config["chunking"]["time_chk"] == 1
	assert config["chunking"]["lat_chk"] == 46
	assert config["chunking"]["lon_chk"] == 22

	assert "flags" in config
	assert config["flags"]["overwrite"] == False

def test_read_yaml_config_missing_file():
	config = read_yaml_config("configs/nonexistent_config.yaml")
	assert config is None


# test read_era5_info and read_cerra_info with valid variable and missing variable
def test_read_era5_info():
	info = read_era5_info("2t")
	assert info is not None
	assert "long_name" in info
	assert "unit" in info
	assert "param" in info
	assert info["short_name"] == "2t"
	assert info["long_name"] == "2m_temperature"
	assert info["unit"] == "K"
	assert info["param"] == 167
	assert info["cmip_name"] == "tas"
	assert info["cmip_unit"] == "K"


def test_read_era5_info_missing_variable():
	info = read_era5_info("nonexistent_variable")
	assert info is None


def test_read_cerra_info():
	info = read_cerra_info("2t")
	assert info is not None
	assert "long_name" in info
	assert "unit" in info
	assert "param" in info
	assert info["short_name"] == "2t"
	assert info["long_name"] == "2m_temperature"
	assert info["unit"] == "K"
	assert info["param"] == 167
	assert info["cmip_name"] == "tas"
	assert info["cmip_unit"] == "K"
	assert info["agg_method"] == "mean"

def test_read_cerra_info_missing_variable():
	info = read_cerra_info("nonexistent_variable")
	assert info is None

# test read_cmip_info with different scenarios of variable presence in the tables, using mocking to simulate the HTTP responses and logger behavior
def test_read_cmip_info_found_in_day_table(mocker):
    """Test that the variable is successfully found in the first (CMIP6_day) table."""
    # Arrange
    mock_get = mocker.patch('cmip_utils.requests.get')
    mock_logger = mocker.patch('cmip_utils.logger')

    mock_response = MagicMock()
    mock_response.text = json.dumps({
        "variable_entry": {
            "tas": {"standard_name": "air_temperature", "long_name": "Near-Surface Air Temperature"}
        }
    })
    mock_get.return_value = mock_response

    # Act
    standard_name, long_name = read_cmip_info("tas")

    # Assert
    assert standard_name == "air_temperature"
    assert long_name == "Near-Surface Air Temperature"

    mock_get.assert_called_once_with(
        'https://raw.githubusercontent.com/PCMDI/cmip6-cmor-tables/refs/heads/main/Tables/CMIP6_day.json'
    )
    mock_logger.error.assert_not_called()


def test_read_cmip_info_fallback_to_amon_table(mocker):
    """Test fallback to the second (CMIP6_Amon) table when missing from the first."""
    # Arrange
    mock_get = mocker.patch('cmip_utils.requests.get')
    mock_logger = mocker.patch('cmip_utils.logger')

    mock_day_resp = MagicMock()
    mock_day_resp.text = json.dumps({"variable_entry": {}})  # Empty, triggers KeyError

    mock_amon_resp = MagicMock()
    mock_amon_resp.text = json.dumps({
        "variable_entry": {
            "pr": {"standard_name": "precipitation_flux", "long_name": "Precipitation"}
        }
    })

    # side_effect returns these mocks sequentially on successive calls
    mock_get.side_effect = [mock_day_resp, mock_amon_resp]

    # Act
    standard_name, long_name = read_cmip_info("pr")

    # Assert
    assert standard_name == "precipitation_flux"
    assert long_name == "Precipitation"
    assert mock_get.call_count == 2
    mock_logger.error.assert_called_once_with("CMIP variable pr not found in CMIP6_day table.")


def test_read_cmip_info_fallback_to_omon_table(mocker):
    """Test fallback to the third (CMIP6_Omon) table when missing from the first two."""
    # Arrange
    mock_get = mocker.patch('cmip_utils.requests.get')
    mock_logger = mocker.patch('cmip_utils.logger')

    mock_empty_resp = MagicMock()
    mock_empty_resp.text = json.dumps({"variable_entry": {}})

    mock_omon_resp = MagicMock()
    mock_omon_resp.text = json.dumps({
        "variable_entry": {
            "tos": {"standard_name": "sea_surface_temperature", "long_name": "Sea Surface Temperature"}
        }
    })

    mock_get.side_effect = [mock_empty_resp, mock_empty_resp, mock_omon_resp]

    # Act
    standard_name, long_name = read_cmip_info("tos")

    # Assert
    assert standard_name == "sea_surface_temperature"
    assert long_name == "Sea Surface Temperature"
    assert mock_get.call_count == 3
    assert mock_logger.error.call_count == 2


def test_read_cmip_info_not_found_anywhere(mocker):
    """Test that (None, None) is returned when the variable isn't in any table."""
    # Arrange
    mock_get = mocker.patch('cmip_utils.requests.get')
    mock_logger = mocker.patch('cmip_utils.logger')

    mock_empty_resp = MagicMock()
    mock_empty_resp.text = json.dumps({"variable_entry": {}})
    mock_get.return_value = mock_empty_resp  # Will return empty for all calls

    # Act
    standard_name, long_name = read_cmip_info("fake_variable")

    # Assert
    assert standard_name is None
    assert long_name is None
    assert mock_get.call_count == 3
    assert mock_logger.error.call_count == 3