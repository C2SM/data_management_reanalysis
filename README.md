# Data Management reanalysis

Repository to download and process reanalysis data (ERA5, CERRA, etc.).
CERRA and CERRA-Land data are downloaded from Climate Data Store (CDS).
ERA5 data are downloaded from DKRZ if possible, variables not available from DKRZ are downloaded from CDS.
Data is downloaded as grib (smaller, CDS netcdf download experimental) and then converted to netcdf.
Then, variable names and units are changed according to cmip. Some cmip variables have to be calculated.

## Description

The ERA5 reanalysis gives a state of the art representation of the past conditions of the atmosphere. The data is 
useful in many aspects and can be downloaded directly from ECMWF via Climate Data Store (CDS). However, the CDS
tends to be too slow if large amounts of data need to be downloaded, in particular if 3D fields on pressure levels are needed.
The data can only be downoaded as 1-hourly (or some variables as monthly aggregates) data from CDS but many of our users need daily resolutions.

The DKRZ also has a ERA5 data pool with part of the data in 1-hourly, daily and monthly resolutions.
This repository is used to download data directly from DKRZ as daily data where possible
(e.g. geopotential we download as 1-hourly data because we need daily geopotential height in the end which needs to be calculated from the 1-hourly data).

The data is then converted to netcdf (using grib_to_netcdf) and variable names are changed according to CMIP.
For some variables we also change the units according to CMIP.

## Getting Started

### Dependencies

* needs python, xarray
* needs eccodes grib_to_netcdf


### Installing

* 
* Adjust config_example.ini
  
### Executing program

* How to run the program
* Step-by-step bullets
```
code blocks for commands
```

## Help

Any advise for common problems or issues.
```
command to run if program contains helper info
```

## Authors

Ruth Lorenz
ruth.lorenz at c2sm.ethz.ch

## Version History
* 0.1
    * Initial Release

## License

This project is licensed under the MIT-License - see the LICENSE.md file for details
