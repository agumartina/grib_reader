import rasterio
import argparse
import ray
import glob
import numpy as np
from affine import Affine
from datetime import datetime
from pathlib import Path


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


@ray.remote
def flipRaster(filename: str):
    # new filename
    var, date = getInfo(filename)
    path_dir = f"/home/datos/geotiff/{var}_flip"
    tiff_name = f"{var}_{date}.tiff"
    pathfile = f'{path_dir}/{tiff_name}'

    # do not process if the file exist
    if Path(pathfile).is_file():
        print("File exist")
        return

    # open raster and flip
    src = rasterio.open(filename)
    transform = src.get_transform()
    x = src.read(1)

    pathfolder = Path(path_dir)
    pathfolder.mkdir(parents=True, exist_ok=True)

    with rasterio.open(
        pathfile,
        'w',
        driver='GTiff',
        height=x.shape[0],
        width=x.shape[1],
        count=1,
        dtype=x.dtype,
        crs=src.crs.to_proj4(),
        transform=Affine.from_gdal(*transform),
    ) as dst:
        dst.write(np.flipud(x), 1)


def main():
    parser = argparse.ArgumentParser(
                description='flip_raster.py --path=to_geotiff_files',
                epilog="flip all rasters in a folder")

    parser.add_argument("--path", type=str, dest="path",
                        help="folder with geoTiff to flip", required=True)

    args = parser.parse_args()

    # define options
    parser.print_help()

    # 'data/GFS/*.grib2'
    filelist = getList(args.path)
    filelist.sort()

    it = ray.util.iter.from_items(filelist, num_shards=32)
    proc = [flipRaster.remote(filename) for filename in it.gather_async()]
    ray.get(proc)


if __name__ == "__main__":
    main()