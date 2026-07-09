import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Import the functions from module
from process_2D_analysis_multvars_from_cds_daily import download_data_cds_multvar, process_multvar_era5_data

class TestDownloadDataCdsMultvar:

    def _get_test_inputs(self):
        """Helper to generate clean inputs for each test case."""
        return {
            "dataname": "ERA5-Land",
            "varlist": ["2m_temperature", "10m_u-component_of_wind"],
            "download_path": "/mock/download/dir",
            "year": 2026,
            "month": "07",
            "statistic": "daily_mean",
        }

    @patch("process_2D_analysis_multvars_from_cds_daily.os.path.isfile")
    @patch("process_2D_analysis_multvars_from_cds_daily.logger")
    @patch("process_2D_analysis_multvars_from_cds_daily.cdsapi.Client")
    def test_download_when_file_does_not_exist(
        self, mock_cds_client, mock_logger, mock_isfile
    ):
        """Test that the API client is invoked correctly when the target file is missing."""
        inputs = self._get_test_inputs()
        mock_isfile.return_value = False

        # Set up the mock instance for the CDS API client
        mock_client_instance = MagicMock()
        mock_cds_client.return_value = mock_client_instance

        # Execute
        expected_target = "/mock/download/dir/variables_daily_mean_ERA5-Land_202607.zip"
        result = download_data_cds_multvar(**inputs, overwrite=False)

        # Assertions
        assert result == expected_target
        mock_logger.info.assert_any_call(
            "Downloading data for ERA5-Land from CDS for year 2026 and month 07."
        )

        # Verify the CDS client was instantiated and called with exact parameters
        mock_cds_client.assert_called_once()
        mock_client_instance.retrieve.assert_called_once_with(
            "derived-ERA5-Land-daily-statistics",
            {
                "variable": inputs["varlist"],
                "year": 2026,
                "month": "07",
                "day": [
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
                    "13",
                    "14",
                    "15",
                    "16",
                    "17",
                    "18",
                    "19",
                    "20",
                    "21",
                    "22",
                    "23",
                    "24",
                    "25",
                    "26",
                    "27",
                    "28",
                    "29",
                    "30",
                    "31",
                ],
                "daily_statistic": "daily_mean",
                "time_zone": "utc+00:00",
                "frequency": "1_hourly",
            },
            expected_target,
        )

    @patch("process_2D_analysis_multvars_from_cds_daily.os.path.isfile")
    @patch("process_2D_analysis_multvars_from_cds_daily.logger")
    @patch("process_2D_analysis_multvars_from_cds_daily.cdsapi.Client")
    def test_skip_download_when_file_exists_and_no_overwrite(
        self, mock_cds_client, mock_logger, mock_isfile
    ):
        """Test that the download is bypassed if the file exists and overwrite is False."""
        inputs = self._get_test_inputs()
        mock_isfile.return_value = True

        mock_client_instance = MagicMock()
        mock_cds_client.return_value = mock_client_instance

        # Execute
        expected_target = "/mock/download/dir/variables_daily_mean_ERA5-Land_202607.zip"
        result = download_data_cds_multvar(**inputs, overwrite=False)

        # Assertions
        assert result == expected_target
        mock_logger.info.assert_any_call(
            f"File {expected_target} already exists, skipping download."
        )

        # Client retrieval should NEVER be called
        mock_client_instance.retrieve.assert_not_called()

    @patch("process_2D_analysis_multvars_from_cds_daily.os.path.isfile")
    @patch("process_2D_analysis_multvars_from_cds_daily.logger")
    @patch("process_2D_analysis_multvars_from_cds_daily.cdsapi.Client")
    def test_force_download_when_file_exists_with_overwrite(
        self, mock_cds_client, mock_logger, mock_isfile
    ):
        """Test that the download proceeds even if the file exists when overwrite is True."""
        inputs = self._get_test_inputs()
        mock_isfile.return_value = True  # File exists!

        mock_client_instance = MagicMock()
        mock_cds_client.return_value = mock_client_instance

        # Execute with overwrite=True
        result = download_data_cds_multvar(**inputs, overwrite=True)

        # Assertions
        assert result == "/mock/download/dir/variables_daily_mean_ERA5-Land_202607.zip"
        mock_logger.info.assert_any_call(
            "Downloading data for ERA5-Land from CDS for year 2026 and month 07."
        )

        # Retrieval should still happen due to overwrite override
        mock_client_instance.retrieve.assert_called_once()


class TestProcessEra5MonthlyData:

    def _get_test_inputs(self):
        """Helper method to provide clean, isolated inputs for each test."""
        return {
            "download_file": "mock_download.zip",
            "work_all_path": "/mock/work/all",
            "var_list_short": ["tas"],
            "proc_path": "/mock/proc",
            "year": "2026",
            "month": "07",
            "statistic_string": "mean",
            "dataname": "ERA5",
            "era5_info": {
                "tas": {
                    "cmip_name": "tas",
                    "long_name": "2m_temperature",
                }
            },
            "config": {
                "chunking": {"time_chk": 10, "lon_chk": 10, "lat_chk": 10}
            },
            "store": MagicMock(),
            "logger": MagicMock(),
        }

    @patch("process_2D_analysis_multvars_from_cds_daily.zipfile.ZipFile")
    @patch("process_2D_analysis_multvars_from_cds_daily.os.makedirs")
    @patch("process_2D_analysis_multvars_from_cds_daily.os.path.isfile")
    @patch("process_2D_analysis_multvars_from_cds_daily.os.path.getsize")
    @patch("process_2D_analysis_multvars_from_cds_daily.os.remove")
    @patch("process_2D_analysis_multvars_from_cds_daily.convert_valid_time_latitude_longitude")
    @patch("process_2D_analysis_multvars_from_cds_daily.convert_era5_to_cmip")
    @patch("process_2D_analysis_multvars_from_cds_daily.calc_mon_mean")
    def test_successful_processing(
        self,
        mock_calc_mon_mean,
        mock_convert_era5,
        mock_convert_valid_time,
        mock_os_remove,
        mock_os_getsize,
        mock_os_isfile,
        mock_os_makedirs,
        mock_zipfile,
    ):
        """Test the standard happy path where everything processes successfully."""
        inputs = self._get_test_inputs()

        # Setup mock behaviors
        mock_os_isfile.return_value = True
        mock_os_getsize.return_value = 100  # Non-zero size

        expected_outfile = (
            "/mock/proc/tas/day/native/2026/tas_day_ERA5_202607.nc"
        )
        mock_convert_valid_time.return_value = "/mock/work/all/tas/tmp_out.nc"
        mock_convert_era5.return_value = expected_outfile
        mock_calc_mon_mean.return_value = (
            "/mock/proc/tas/day/native/2026/tas_mon_ERA5_202607.nc"
        )

        # Execute
        process_multvar_era5_data(
            download_file_multvar=inputs["download_file"],
            work_all_path=inputs["work_all_path"],
            var_list_short=inputs["var_list_short"],
            proc_path=inputs["proc_path"],
            era5_info=inputs["era5_info"],
            year=inputs["year"],
            month=inputs["month"],
            statistic_string=inputs["statistic_string"],
            dataname=inputs["dataname"],
            config=inputs["config"],
            store=inputs["store"],
            logger=inputs["logger"],
            overwrite=False,
        )

        # Pytest uses standard Python assert statements
        mock_zipfile.assert_called_once_with(inputs["download_file"], "r")
        mock_convert_valid_time.assert_called_once()
        mock_convert_era5.assert_called_once()
        mock_calc_mon_mean.assert_called_once_with(
            "/mock/proc/tas/day/native/2026", expected_outfile
        )

        assert mock_os_remove.call_count == 2
        inputs["logger"].info.assert_any_call(
            f"File {expected_outfile} written."
        )

    @patch("process_2D_analysis_multvars_from_cds_daily.zipfile.ZipFile")
    @patch("process_2D_analysis_multvars_from_cds_daily.os.makedirs")
    @patch("process_2D_analysis_multvars_from_cds_daily.os.path.isfile")
    @patch("process_2D_analysis_multvars_from_cds_daily.convert_valid_time_latitude_longitude")
    def test_skip_when_file_exists_and_no_overwrite(
        self,
        mock_convert_valid_time,
        mock_os_isfile,
        mock_os_makedirs,
        mock_zipfile,
    ):
        """Test that the function skips processing if the outfile already exists and overwrite is False."""
        inputs = self._get_test_inputs()
        mock_os_isfile.side_effect = lambda path: True

        # Execute
        process_era5_monthly_data(
            download_file_multvar=inputs["download_file"],
            work_all_path=inputs["work_all_path"],
            var_list_short=inputs["var_list_short"],
            proc_path=inputs["proc_path"],
            era5_info=inputs["era5_info"],
            year=inputs["year"],
            month=inputs["month"],
            statistic_string=inputs["statistic_string"],
            dataname=inputs["dataname"],
            config=inputs["config"],
            store=inputs["store"],
            logger=inputs["logger"],
            overwrite=False,
        )

        mock_convert_valid_time.assert_not_called()
        inputs["logger"].info.assert_any_call(
            "Skipping processing of variable tas for month 07."
        )

    @patch("process_2D_analysis_multvars_from_cds_daily.zipfile.ZipFile")
    @patch("process_2D_analysis_multvars_from_cds_daily.os.makedirs")
    @patch("process_2D_analysis_multvars_from_cds_daily.os.path.isfile")
    def test_exit_when_unzipped_file_missing(
        self, mock_os_isfile, mock_os_makedirs, mock_zipfile
    ):
        """Test that the program exits if the expected unzipped file isn't found."""
        inputs = self._get_test_inputs()
        mock_os_isfile.return_value = False

        # Use pytest.raises to intercept system exits cleanly
        with pytest.raises(SystemExit) as exc_info:
            process_era5_monthly_data(
                download_file_multvar=inputs["download_file"],
                work_all_path=inputs["work_all_path"],
                var_list_short=inputs["var_list_short"],
                proc_path=inputs["proc_path"],
                era5_info=inputs["era5_info"],
                year=inputs["year"],
                month=inputs["month"],
                statistic_string=inputs["statistic_string"],
                dataname=inputs["dataname"],
                config=inputs["config"],
                store=inputs["store"],
                logger=inputs["logger"],
                overwrite=False,
            )

        assert exc_info.value.code == 1
        inputs["logger"].error.assert_any_call(
            "Expected file /mock/work/all/2m_temperature_0_mean.nc for variable tas not found after unzipping."
        )