import argparse
import numpy as np
import pandas as pd


def genWind():
    # Open generated CSV
    file_spd = 'csv/wspd.csv'
    file_wdir = 'csv/wdir.csv'
    data_U_file = 'csv/10U.csv'
    data_V_file = 'csv/10V.csv'
    data_U = pd.read_csv(data_U_file, header=None)
    data_V = pd.read_csv(data_V_file, header=None)

    data_U["NAME"] = data_U[1]
    data_U["data"] = data_U[2]
    data_U["date"] = pd.to_datetime(data_U[3])
    data_U = data_U[['NAME', 'data', 'date']]
    data_U = data_U.sort_values(by='date')
    data_V["NAME"] = data_V[1]
    data_V["data"] = data_V[2]
    data_V["date"] = pd.to_datetime(data_V[3])
    data_V = data_V[['NAME', 'data', 'date']]
    data_V = data_V.sort_values(by='date')

    # Get unique values of zones
    zonas = data_U.NAME.unique()

    for zona in zonas:
        zona_U = data_U.loc[data_U['NAME'] == zona]
        zona_V = data_V.loc[data_V['NAME'] == zona]

        WDIR = (270-np.rad2deg(np.arctan2(zona_V['data'], zona_U['data']))) % 360
        WSPD = np.sqrt(np.square(zona_V['data']) + np.square(zona_U['data']))

        zona_wspd = zona_V[['NAME', 'date']]
        zona_wdir = zona_V[['NAME', 'date']]
        zona_wdir.loc[:, 'wdir'] = WDIR.values
        zona_wspd.loc[:, 'wspd'] = WSPD.values

        # zona_wdir.sort_index(inplace=True)
        # zona_wspd.sort_index(inplace=True)

        zona_wspd.to_csv(file_spd, mode='a', header=None, encoding='utf-8')
        zona_wdir.to_csv(file_wdir, mode='a', header=None, encoding='utf-8')


def genProducts(regex: str):
    genWind()


def main():
    parser = argparse.ArgumentParser(
                description=('products.py --path=path/to/csv'),
                epilog="Gen products from csv")

    parser.add_argument("--path", type=str, dest="path",
                        help="folder with gtiff files", required=True)

    args = parser.parse_args()

    # define options
    parser.print_help()

    genProducts(args.path)


if __name__ == "__main__":
    main()
