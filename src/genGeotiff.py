import glob
import argparse
import rasterio
import numpy as np
import xarray as xr
import pandas as pd
import ray
import os
from config.constants import EXTENT, KM_PER_DEGREE, RESOLUTION, DICT_VAR
from osgeo import osr, gdal, gdal_array
from affine import Affine


def getList(path: str):
    return glob.glob(path, recursive=True)


def getGeoT(extent, nlines, ncols):
    # Compute resolution based on data dimension
    resx = (extent[2] - extent[0]) / ncols
    resy = (extent[3] - extent[1]) / nlines
    return [extent[0], resx, 0, extent[3], 0, -resy]


@ray.remote
def transformGrib(filename: str):

    # ORIGIN DATASET
    grib = gdal.Open(filename)
    origin_proj = grib.GetProjection()
    origin_transform = grib.GetGeoTransform()
    # origin_xsize = grib.RasterXSize
    # origin_ysize = grib.RasterYSize

    ds = xr.open_dataset(filename, engine="pynio")

    for var in ds.variables:
        if var in DICT_VAR:
            for arr_in in ds[var]:
                # Origen
                origin = gdal_array.OpenArray(np.flipud(arr_in.values))
                origin.SetProjection(origin_proj)
                origin.SetGeoTransform(origin_transform)

                # Destination grid
                # Lat/lon WSG84 Spatial Reference System
                targetPrj = osr.SpatialReference()
                targetPrj.ImportFromProj4('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')

                sizex = int((EXTENT[2] - EXTENT[0]) * KM_PER_DEGREE/RESOLUTION)
                sizey = int((EXTENT[3] - EXTENT[1]) * KM_PER_DEGREE/RESOLUTION)

                memDriver = gdal.GetDriverByName('MEM')

                # Create grid
                grid = memDriver.Create('grid', sizex, sizey, 1, gdal.GDT_Float64)

                # Setup projection and geo-transformation
                grid.SetProjection(targetPrj.ExportToWkt())
                grid.SetGeoTransform(getGeoT(EXTENT, grid.RasterYSize, grid.RasterXSize))

                # Perform the projection/resampling
                gdal.ReprojectImage(
                    origin,
                    grid,
                    grib.GetProjection(),
                    targetPrj.ExportToWkt(),
                    gdal.GRA_NearestNeighbour,
                    options=['NUM_THREADS=ALL_CPUS']
                    )

                # Read grid data
                array1 = grid.ReadAsArray()

                # Get transform in Affine format
                geotransform = grid.GetGeoTransform()
                transform = Affine.from_gdal(*geotransform)

                # Build filename
                time = pd.to_datetime(arr_in.initial_time0_hours.values)
                date = f"{time.strftime('%Y-%m-%dZ%H:%M')}"
                path_dir = f"../geotiff/{var}"
                try:
                    os.makedirs(path_dir)
                except OSError:
                    continue

                tiffname = f"{var}_{date}.tiff"

                pathfile = f'{path_dir}/{tiffname}'

                # WRITE GIFF
                nw_ds = rasterio.open(pathfile, 'w', driver='GTiff',
                                      height=grid.RasterYSize,
                                      width=grid.RasterXSize,
                                      count=1,
                                      dtype=gdal.GetDataTypeName(gdal.GDT_Float64).lower(),
                                      crs=grid.GetProjection(),
                                      transform=transform)
                nw_ds.write(array1, 1)
                nw_ds.close()


def main():
    parser = argparse.ArgumentParser(
                description='genGeotiff.py --path=to_grib_files',
                epilog="Convert  all grib files stored in path folder \
                        to a raster in geoTiff format")

    parser.add_argument("--path", type=str, dest="path",
                        help="folder with grib2", required=True)

    args = parser.parse_args()

    # define options
    parser.print_help()

    # 'data/GFS/*.grib2'
    filelist = getList(args.path)
    filelist.sort()

    it = ray.util.iter.from_items(filelist, num_shards=4)
    proc = [transformGrib.remote(filename) for filename in it.gather_async()]
    ray.get(proc)


if __name__ == "__main__":
    main()
