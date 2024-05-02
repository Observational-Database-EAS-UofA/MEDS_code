import os

import matplotlib.pyplot as plt
import xarray as xr
import numpy as np


class MEDSPlotter:
    def __init__(self, ncfile, save_path):
        self.ds = xr.open_dataset(ncfile)
        self.save_path = save_path

    def clean_variable(self, var_name, var_data):
        cleaned_var = var_data
        if f"{var_name}_flag" in self.ds:
            flagged_var = self.ds[f"{var_name}_flag"].values
            for i in range(len(var_data)):
                if flagged_var[i] not in [1, 5, 8]:
                    cleaned_var[i] = np.nan
        return cleaned_var

    def save_figure(self, var1_name, var2_name, is_clean):
        plots_directory = f"{self.save_path}plots"
        if not os.path.isdir(plots_directory):
            os.mkdir(plots_directory)
        os.chdir(plots_directory)
        if is_clean:
            plt.savefig(f"{var1_name}_{var2_name}_clean.png")
        else:
            plt.savefig(f"{var1_name}_{var2_name}.png")

    def plot_variables(self, var1_name, var2_name):
        var1_data = self.ds[var1_name].values
        var2_data = self.ds[var2_name].values

        plt.plot(var1_data, var2_data, '.')
        plt.xlabel(var1_name)
        plt.ylabel(var2_name)
        plt.gca().invert_yaxis()
        plt.title(f"{var1_name} x {var2_name}")
        self.save_figure(var1_name, var2_name, False)
        plt.clf()

    def plot_variables_clean(self, var1_name, var2_name):
        var1_data = self.ds[var1_name].values
        var2_data = self.ds[var2_name].values
        cleaned_v1 = self.clean_variable(var1_name, var1_data)
        cleaned_v2 = self.clean_variable(var2_name, var2_data)

        plt.plot(cleaned_v1, cleaned_v2, '.')
        plt.xlabel(var1_name)
        plt.ylabel(var2_name)
        plt.gca().invert_yaxis()
        plt.title(f"{var1_name} x {var2_name} clean")
        self.save_figure(var1_name, var2_name, True)
        plt.clf()


def main():
    netcdf_file = '../ncfiles_standard/MEDS_1916_2000_raw_id_standard.nc'
    save_path = os.getcwd()[:os.getcwd().rfind("/") + 1]
    meds_plotter = MEDSPlotter(netcdf_file, save_path)
    meds_plotter.plot_variables('temp', 'depth')
    meds_plotter.plot_variables_clean('temp', 'depth')
    meds_plotter.plot_variables('psal', 'depth')
    meds_plotter.plot_variables_clean('psal', 'depth')


if __name__ == '__main__':
    main()
