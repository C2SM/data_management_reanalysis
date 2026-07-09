#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File Name : unit_tests_general_functions.py
Author: Ruth Lorenz (ruth.lorenz@c2sm.ethz.ch)
Created: 28/05/2026
Purpose: unit tests for general_functions in data_management_reanalysis repo
	testing individual components or functions of code to ensure they work correctly in isolation
"""
import subprocess
from unittest.mock import MagicMock, patch
import pytest

from functions.general_functions import convert_month_list, download_data_dkrz

# test convert_month_list with different input formats
def test_convert_month_list():
	assert convert_month_list(01, 02) == ["01", "02"]
	assert convert_month_list(10, 11, 12) == ["10", "11", "12"]
	assert convert_month_list("01", "02") == ["01", "02"]


# test download_data_dkrz with mocked subprocess.run to avoid actual rsync calls

# Mock inputs for the tests
@pytest.fixture
def base_era5_info():
    return {
        "param": "167",
        "analysis": "1"  # 1 implies analysis ('an', '00')
    }

@pytest.fixture
def forecast_era5_info():
    return {
        "param": "228",
        "analysis": "0"  # 0 implies forecast ('fc', '12')
    }


# Patching all external system hooks globally for these tests
@patch("functions.general_functions.os.makedirs")
@patch("functions.general_functions.logger")
@patch("functions.general_functions.subprocess.run")
def test_download_all_months_analysis_daily(mock_run, mock_logger, mock_makedirs, base_era5_info):
    """Test downloading all months for daily analysis data (type='an', typeid='00')."""

    # Setup the mock process return value
    mock_process = MagicMock()
    mock_process.returncode = 0
    mock_run.return_value = mock_process

    gribfile, returncode = download_data_dkrz(
        freq="1D",
        era5_info=base_era5_info,
        origin="era5/data",
        iac_path="/local/path/",
        year=2023,
        months=[],
        all_months=True,
        family="e5",
        level="sfc"
    )

    # Verify directory creation was attempted
    mock_makedirs.assert_called_once_with("/local/path/", exist_ok=True)

    # Verify rsync command construction
    mock_run.assert_called_once_with(
        ["rsync", "-av", "levante:era5/data/an/1D/167/*_2023-*_167.grb", "/local/path/"],
        check=True,
        capture_output=True,
        text=True
    )

    # Verify return parameters
    assert returncode == 0
    assert gribfile == "/local/path/e5sfc00_1D_2023-MM_167.grb"


@patch("functions.general_functions.os.makedirs")
@patch("functions.general_functions.logger")
@patch("functions.general_functions.subprocess.run")
def test_download_specific_months_forecast_hourly(mock_run, mock_logger, mock_makedirs, forecast_era5_info):
    """Test looping over a specified list of months for hourly forecast data (type='fc', typeid='12')."""

    mock_process = MagicMock()
    mock_process.returncode = 0
    mock_run.return_value = mock_process

    gribfile, returncode = download_data_dkrz(
        freq="1H",
        era5_info=forecast_era5_info,
        origin="era5/data",
        iac_path="/local/path/",
        year=2023,
        months=["01", "02"],
        all_months=False,
        family="e5",
        level="sfc"
    )

    # Since all_months=False and 2 months are specified, rsync should be called twice
    assert mock_run.call_count == 2

    # Check the call signature of the first month ('01')
    first_call_args = mock_run.call_args_list[0][0][0]
    assert "levante:era5/data/fc/1H/228/*_2023-01*_228.grb" in first_call_args

    # Check the call signature of the second month ('02')
    second_call_args = mock_run.call_args_list[1][0][0]
    assert "levante:era5/data/fc/1H/228/*_2023-02*_228.grb" in second_call_args

    # Verify return parameters for hourly structure
    assert returncode == 0
    assert gribfile == "/local/path/e5sfc12_1H_2023-MM-DD_228.grb"


@patch("functions.general_functions.os.makedirs")
@patch("functions.general_functions.logger")
@patch("functions.general_functions.subprocess.run")
def test_download_subprocess_failure(mock_run, mock_logger, mock_makedirs, base_era5_info):
    """Test that rsync command failures are intercepted and logged without breaking execution."""
    from your_module import download_data_dkrz

    # Make the subprocess call throw an execution error
    mock_run.side_effect = subprocess.CalledProcessError(
        returncode=12,
        cmd="rsync",
        output="out string",
        stderr="err string"
    )

    gribfile, returncode = download_data_dkrz(
        freq="1D",
        era5_info=base_era5_info,
        origin="era5/data",
        iac_path="/local/path/",
        year=2023,
        months=[],
        all_months=True,
        family="e5",
        level="sfc"
    )

    # Function should smoothly absorb the exception and return the failure code
    assert returncode == 12
    # Verify the error logging mechanisms were invoked
    assert mock_logger.error.call_count == 3