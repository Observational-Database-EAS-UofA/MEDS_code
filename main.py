from functions.read_MEDS_CAA_Baffin_1920_2021 import read_MEDS
from functions.plot_data import Plotter


def plot_data():
    # data = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/MEDS_2021/ncfiles_raw/MEDS2015_2021_meds_raw.nc'
    # data = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/MEDS_2021/ncfiles_raw/MEDS2009_2014_meds_raw.nc'
    data = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/MEDS_2021/ncfiles_raw/MEDS1916_2008_meds_raw.nc'
    plotter = Plotter(data)
    # plotter.plot_lon_lat()
    plotter.plot_temp_depth()


def get_data():
    # data_path = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/MEDS_2021/original_data/a_MEDS_profile_prof_2015_2021.csv'
    # data_path = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/MEDS_2021/original_data/a_MEDS_profile_prof_2009_2014.csv'
    data_path = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/MEDS_2021/original_data/a_MEDS_profile_prof_1916_2008.csv'
    # data_path = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/MEDS_2021/original_data/test_data.csv'
    save_path = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/MEDS_2021/ncfiles_raw'
    read_MEDS(data_path, save_path)


if __name__ == '__main__':
    get_data()
    # plot_data()
