import matplotlib.pyplot as plt
import numpy as np
import xarray as xr


class Plotter:
    def __init__(self, ncfile):
        self.ds = xr.open_dataset(ncfile)

    def plot_temp_depth(self):
        temp = self.ds['psal'].values
        depth = self.ds['depth'].values
        # print(type(temp))
        # i, = np.where(temp > 90)
        # print(i)
        # print(temp[i])
        # print(depth[i])

        plt.plot(temp, depth, '.')
        plt.ylabel('depth')
        plt.xlabel('psal')
        plt.gca().invert_yaxis()
        plt.show()

        def plot_lon_lat(self):
            lon = self.ds['lon'].values
            lat = self.ds['lat'].values
            plt.plot(lon, lat)
            plt.xlabel('lon')
            plt.ylabel('lat')
            plt.show()
