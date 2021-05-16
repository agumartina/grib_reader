
# Grib Reader

A few scripts to read, reproject and rasterize GRIB, GRIB2 or NetCDF data, from [Copernicus](https://cds.climate.copernicus.eu/#!/home) ERA5 model, to a data serie in csv.


## Usage/Examples

Examples how to use

### Reproject and save as Geotiff
This script takes a directory full of grib files, reprojects them to WGS84 and save it as a raster in GeoTiff format. 
```Python
    python gen_geotiff.py --path /path/to/grib/files 
```

### Extract data
This scripts use the points declared in a shapefile, get the data from de rasters and generate a serie in a csv file. 
```Python
    python rasterize.py --path /path/to/geotiff --shapefile /path/to/shapefile 
```

### Calculate wind direction and speed
Because the model has U and V components of wind, and wind speed (wspd) and wind direction (wdir) are desired.
```Python
    python products.py --path /path/to/U_V_csv/file
```

  
## Installation 

Required packages
| Package      |  Version     |
| :------------- | :----------: |
| gdal | 3.1.14 | 
| xarray | >= 0.17 |
| rasterio | 1.2.0 | 
| rasterstats | >= 0.14.0 |
| pandas | 1.2.2 | 
| netcdf4 | 1.5.6 |
    