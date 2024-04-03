from read_MEDS_CAA_Baffin_1920_2021 import read_MEDS


def get_data():
    meds_2015_2021 = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/MEDS_2021/original_data/a_MEDS_profile_prof_2015_2021.csv'
    meds_2009_2014 = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/MEDS_2021/original_data/a_MEDS_profile_prof_2009_2014.csv'
    meds_1916_2008 = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/MEDS_2021/original_data/a_MEDS_profile_prof_1916_2008.csv'

    data_path_list = [meds_2015_2021, ]

    for data_path in data_path_list:
        save_path = '/home/novaisc/workspace/obs_database/AW_CAA/CTD_DATA/MEDS_2021/ncfiles_raw'
        print(f"reading {data_path}...")
        read_MEDS(data_path, save_path)


if __name__ == '__main__':
    get_data()
