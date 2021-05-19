import glob
import ray
import argparse
import os
import numpy as np
import geopandas as gpd
import pandas as pd
from rasterstats import point_query
from datetime import datetime


ray.init(address='localhost:6381', _redis_password='5241590000000000')


def getList(regex: str):
    regex = regex + '/*'
    return glob.glob(regex, recursive=True)


def getInfo(filename: str):
    """Retorna la parametrizacion y el timestamp a partir del
    nombre del archivo wrfout
    ../geotiff/10V_GDS0_SFC/10V_GDS0_SFC_1991-07-18Z22:00.tiff"""
    filename = filename.split('/')[-1]
    var, temp = filename.split('_', 1)
    temp1, temp1, timestamp = temp.split('_', 2)
    timestamp, extension = timestamp.split('.', 1)
    date = datetime.strptime(timestamp, "%Y-%m-%dZ%H:%M")

    return var, date


def integrate_shapes(filename: str, shapefile: str) -> gpd.GeoDataFrame:
    """
    This functions opens a geotiff with desired data, converts to a raster,
    integrate the data into polygons and returns a GeoDataFrame object.

    Parameters:
        cuencas_shp: Path to shapefile
    Returns:
        cuencas_gdf_ppn (GeoDataFrame): a geodataframe with cuerncas and ppn
    """

    cuencas_gdf: gpd.GeoDataFrame = gpd.read_file(shapefile, encoding='utf-8')
    df_zs = pd.DataFrame(point_query(shapefile, filename))

    cuencas_gdf_ppn = pd.concat([cuencas_gdf, df_zs], axis=1)

    COLUM_REPLACE = {0: 'data'}

    cuencas_gdf_ppn = cuencas_gdf_ppn.rename(columns=COLUM_REPLACE)

    return cuencas_gdf_ppn[['NAME', 'geometry', 'data']]


@ray.remote
def getSeries(filename: str, shapefile: str):
    var, date = getInfo(filename)

    zonas = pd.DataFrame()

    zonas_gdf = integrate_shapes(filename, shapefile)
    zonas_gdf = zonas_gdf[['NAME', 'data']]
    zonas_gdf['date'] = datetime.strptime(filename[-21:-5], "%Y-%m-%dZ%H:%M")
    zonas = zonas.append(zonas_gdf, ignore_index=True)
    filename = f"csv/{var}.csv"
    print(f"Saving in {filename}")
    zonas.to_csv(filename, mode='a', header=False, encoding='utf-8')


def rasterize(regex: str, shapefile: str):
    filelist = getList(regex)
    if not filelist:
        print("ERROR: No geotiff files matched")
        return

    filelist.sort()
    it = ray.util.iter.from_items(filelist, num_shards=4)

    proc = [getSeries.remote(filename, shapefile) for filename in it.gather_async()]
    ray.get(proc)


def main():
    parser = argparse.ArgumentParser(
                description=('rasterize.py --path=path/to/geotiff '
                             '--shapefile=path/to/shapefiles'),
                epilog="Extract info from rasters")

    parser.add_argument("--path", type=str, dest="path",
                        help="folder with gtiff files", required=True)

    parser.add_argument("--shapefile", type=str, dest="shapefile",
                        help="if it's gfs or gefs", required=True)

    args = parser.parse_args()

    # define options
    parser.print_help()

    rasterize(args.path, args.shapefile)


if __name__ == "__main__":
    main()
