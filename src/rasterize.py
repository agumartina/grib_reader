import glob
import ray
import argparse
import os
import numpy as np
import geopandas as gpd
import pandas as pd
from rasterstats import point_query
from datetime import datetime


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


def genWind():
    # Open generated CSV
    run_dir = os.getenv('RUN_DIR')
    file_spd = f'{run_dir}/csv/wspd.csv'
    file_wdir = f'{run_dir}/csv/wdir.csv'
    data_U_file = f'{run_dir}/csv/10U_GDS0_SFC.csv'
    data_V_file = f'{run_dir}/csv/10V_GDS0_SFC.csv'
    data_U = pd.read_csv(data_U_file, header=None)
    data_V = pd.read_csv(data_V_file, header=None)

    data_U["NAME"] = data_U[1]
    data_U["data"] = data_U[2]
    data_U["date"] = pd.to_datetime(data_U[3])
    data_U = data_U[['NAME', 'data', 'date']]
    data_V["NAME"] = data_V[1]
    data_V["data"] = data_V[2]
    data_V["date"] = pd.to_datetime(data_V[3])
    data_V = data_V[['name', 'data', 'date']]

    # Get unique values of zones
    zonas = data_U.name.unique()

    for zona in zonas:
        zona_U = data_U.loc[data_U['NAME'] == zona]
        zona_V = data_V.loc[data_V['NAME'] == zona]

        WDIR = (270-np.rad2deg(np.arctan2(zona_V['data'], zona_U['data']))) % 360
        WSPD = np.sqrt(np.square(zona_V['data']) + np.square(zona_U['data']))

        zona_wspd = zona_V[['NAME', 'date']]
        zona_wdir = zona_V[['NAME', 'date']]
        zona_wdir.loc[:, 'wdir'] = WDIR.values
        zona_wspd.loc[:, 'wspd'] = WSPD.values

        zona_wdir.sort_index(inplace=True)
        zona_wspd.sort_index(inplace=True)

        zona_wspd.to_csv(file_spd, mode='a', header=None, encoding='utf-8')
        zona_wdir.to_csv(file_wdir, mode='a', header=None, encoding='utf-8')


def rasterize(regex: str, shapefile: str):
    filelist = getList(regex)
    if not filelist:
        print("ERROR: No geotiff files matched")
        return

    filelist.sort()
    it = ray.util.iter.from_items(filelist, num_shards=4)

    proc = [getSeries.remote(filename, shapefile) for filename in it.gather_async()]
    ray.get(proc)

    # genWind()


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
