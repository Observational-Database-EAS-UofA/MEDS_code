"""
This script reads MEDS (Marine Environmental Data Services) profile data from CSV files,
processes it in chunks due to its large size, and saves it into NetCDF files. It categorizes
the data into three time ranges: 1916-2000, 2001-2010, and 2011-2021.

Key Functions:
1. MEDSReader.initialize_variables(): Initializes variables and lists for storing data.
2. MEDSReader.get_dict_index(): Retrieves the appropriate dictionary index based on the year.
3. MEDSReader.create_dataset(): Creates or appends to NetCDF files depending on whether the files exist.
4. MEDSReader.process_chunks(): Processes data in chunks, extracts relevant information, and stores it.
5. MEDSReader.run(): Executes the data reading, processing, and saving workflow.
6. main(): Main function to run the MEDSReader for multiple data files.

"""

import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime
import os
from tqdm import tqdm


class MEDSReader:
    def __init__(self):
        """Initializes the MEDSReader class."""
        pass

    def initialize_variables(self):
        """
        Initializes variables and lists for storing data.

        Returns:
        - string_attrs: List of string attributes.
        - measurements_attrs: List of measurement attributes.
        - data_lists: Dictionary to store data categorized by year ranges.
        """
        string_attrs = [
            "orig_cruise_id",
            "instrument_type",
            "station_no",
            "lat",
            "lonlat_flag",
            "lon",
            "datestr",
            "timestamp",
            "shallowest_depth",
            "deepest_depth",
            "datestr_flag",
            "depth_row_size",
            "press_row_size",
            "temp_row_size",
            "psal_row_size",
        ]
        measurements_attrs = [
            "depth",
            "press",
            "temp",
            "psal",
            "depth_flag",
            "press_flag",
            "temp_flag",
            "psal_flag",
        ]
        data_lists = {
            year_range: {attr: [] for attr in string_attrs + measurements_attrs}
            for year_range in ["1916_2000", "2001_2010", "2011_2021"]
        }
        return string_attrs, measurements_attrs, data_lists

    def get_dict_index(self, data_lists, year):
        """
        Retrieves the appropriate dictionary index based on the year.

        Parameters:
        - data_lists: Dictionary containing data categorized by year ranges.
        - year: Year for which the index is to be retrieved.

        Returns:
        - Dictionary key representing the appropriate year range.
        """
        for k in data_lists.keys():
            if int(k[:4]) <= int(year) <= int(k[5:]):
                return k

    def create_dataset(self, data_lists, string_attrs, data_path, save_path):
        """
        Creates or appends to NetCDF files depending on whether the files exist.

        Parameters:
        - data_lists: Dictionary containing data categorized by year ranges.
        - string_attrs: List of string attributes.
        - data_path: Path to the original data file.
        - save_path: Path where the processed NetCDF files will be saved.
        """
        if not os.path.isdir(save_path):
            os.mkdir(save_path)
        os.chdir(save_path)
        current_files = [f[5:14] for f in os.listdir() if f.endswith(".nc")]
        for key, data_list in data_lists.items():
            ### check if the file with this year range was already created
            if key not in current_files and len(data_list["timestamp"]) != 0:
                ds = xr.Dataset(
                    coords=dict(
                        timestamp=(["profile"], data_list["timestamp"]),
                        lat=(
                            [
                                "profile",
                            ],
                            data_list["lat"],
                        ),
                        lon=(
                            [
                                "profile",
                            ],
                            data_list["lon"],
                        ),
                    ),
                    data_vars=dict(
                        **{
                            attr: xr.DataArray(data_list[attr], dims=["profile"])
                            for attr in string_attrs
                            if attr not in ["lat", "lon", "timestamp", "datestr"]
                        },
                        datestr=xr.DataArray(data_list["datestr"], dims=["profile"], attrs={"timezone": "UTC"}),
                        # measurements
                        depth=xr.DataArray(data_list["depth"], dims=["depth_obs"]),
                        depth_flag=xr.DataArray(data_list["depth_flag"], dims=["depth_obs"]),
                        press=xr.DataArray(data_list["press"], dims=["press_obs"]),
                        press_flag=xr.DataArray(data_list["press_flag"], dims=["press_obs"]),
                        temp=xr.DataArray(data_list["temp"], dims=["temp_obs"]),
                        temp_flag=xr.DataArray(data_list["temp_flag"], dims=["temp_obs"]),
                        psal=xr.DataArray(data_list["psal"], dims=["psal_obs"]),
                        psal_flag=xr.DataArray(data_list["psal_flag"], dims=["psal_obs"]),
                    ),
                    attrs=dict(
                        dataset_name="MEDS_2021",
                        creation_date=str(datetime.now().strftime("%Y-%m-%d %H:%M")),
                    ),
                )

                ds.to_netcdf(f"MEDS_{key}_raw.nc")
            ### if yes, append the data instead of create the data
            elif key in current_files and len(data_list["timestamp"]) != 0:
                os.chdir(save_path)
                ds = xr.open_dataset(f"MEDS_{key}_raw.nc")
                ds = xr.Dataset(
                    coords=dict(
                        timestamp=(["profile"], np.append(ds["timestamp"], data_list["timestamp"])),
                        lat=(
                            [
                                "profile",
                            ],
                            np.append(ds["lat"], data_list["lat"]),
                        ),
                        lon=(
                            [
                                "profile",
                            ],
                            np.append(ds["lon"], data_list["lon"]),
                        ),
                    ),
                    data_vars=dict(
                        **{
                            attr: xr.DataArray(np.append(ds[attr], data_list[attr]), dims=["profile"])
                            for attr in string_attrs
                            if attr not in ["lat", "lon", "timestamp", "parent_index"]
                        },
                        # measurements
                        depth=xr.DataArray(np.append(ds["depth"], data_list["depth"]), dims=["depth_obs"]),
                        depth_flag=xr.DataArray(
                            np.append(ds["depth_flag"], data_list["depth_flag"]), dims=["depth_obs"]
                        ),
                        press=xr.DataArray(np.append(ds["press"], data_list["press"]), dims=["press_obs"]),
                        press_flag=xr.DataArray(
                            np.append(ds["press_flag"], data_list["press_flag"]), dims=["press_obs"]
                        ),
                        temp=xr.DataArray(np.append(ds["temp"], data_list["temp"]), dims=["temp_obs"]),
                        temp_flag=xr.DataArray(np.append(ds["temp_flag"], data_list["temp_flag"]), dims=["temp_obs"]),
                        psal=xr.DataArray(np.append(ds["psal"], data_list["psal"]), dims=["psal_obs"]),
                        psal_flag=xr.DataArray(np.append(ds["psal_flag"], data_list["psal_flag"]), dims=["psal_obs"]),
                    ),
                    attrs=dict(
                        dataset_name="MEDS_2021",
                        creation_date=str(datetime.now().strftime("%Y-%m-%d %H:%M")),
                    ),
                )

                ds.to_netcdf(f"MEDS_{key}_raw.nc")

    def process_chunks(self, reader, data_lists):
        """
        Processes data in chunks, extracts relevant information, and stores it.

        Parameters:
        - reader: Pandas reader object to read chunks of the file.
        - data_lists: Dictionary to store data categorized by year ranges.
        """
        for chunk in reader:
            grouped_df = chunk.groupby(
                [
                    "DATA_TYPE",
                    "CR_NUMBER",
                    "STN_NUMBER",
                    "SOURCE_ID",
                    "OBS_YEAR",
                    "OBS_MONTH",
                    "OBS_DAY",
                    "OBS_TIME",
                    "Q_DATE_TIME",
                    "LONGITUDE (+E)",
                    "LATITUDE (+N)",
                    "Q_POS",
                ]
            )
            for group, data in tqdm(grouped_df, colour="GREEN"):
                (
                    data_type,
                    cr_number,
                    stn_number,
                    source_id,
                    obs_year,
                    obs_month,
                    obs_day,
                    obs_time,
                    datestr_flag,
                    lon,
                    lat,
                    lonlat_flag,
                ) = group
                index = self.get_dict_index(data_lists, obs_year)
                data_lists[index]["orig_cruise_id"].append(cr_number)
                data_lists[index]["instrument_type"].append(data_type)
                data_lists[index]["station_no"].append(stn_number)
                data_lists[index]["lon"].append(lon)
                data_lists[index]["lat"].append(lat)
                data_lists[index]["lonlat_flag"].append(lonlat_flag)
                datestr = datetime(obs_year, obs_month, obs_day, obs_time // 100, obs_time % 100)
                data_lists[index]["timestamp"].append(datestr.timestamp())
                data_lists[index]["datestr"].append(datetime.strftime(datestr, "%Y/%m/%d %H:%M:%S"))
                data_lists[index]["datestr_flag"].append(datestr_flag)
                if len(data["DEPTH_PRESS"].values) > 1:
                    data_lists[index]["shallowest_depth"].append(min(data["DEPTH_PRESS"][data["DEPTH_PRESS"] != 0]))
                else:
                    data_lists[index]["shallowest_depth"].append(min(data["DEPTH_PRESS"]))
                data_lists[index]["deepest_depth"].append(max(data["DEPTH_PRESS"]))

                data_lists[index]["temp"].extend(data["TEMP"])
                data_lists[index]["temp_row_size"].append(len(data["TEMP"]))
                data_lists[index]["temp_flag"].extend(data["Q_TEMP"])
                data_lists[index]["psal"].extend(data["PSAL"])
                data_lists[index]["psal_row_size"].append(len(data["PSAL"]))
                data_lists[index]["psal_flag"].extend(data["Q_PSAL"])

                ### check if DEPTH_PRESS is depth of press
                if data["D_P_CODE"].values.all() == "D":
                    data_lists[index]["depth"].extend(data["DEPTH_PRESS"])
                    data_lists[index]["depth_row_size"].append(len(data["DEPTH_PRESS"]))
                    data_lists[index]["depth_flag"].extend(data["DP_FLAG"])
                    data_lists[index]["press_row_size"].append(0)
                elif data["D_P_CODE"].values.all() == "P":
                    data_lists[index]["press"].extend(data["DEPTH_PRESS"])
                    data_lists[index]["press_row_size"].append(len(data["DEPTH_PRESS"]))
                    data_lists[index]["press_flag"].extend(data["DP_FLAG"])
                    data_lists[index]["depth_row_size"].append(0)

    # this function reads the MEDS profile data. It reads the csv files in chunks because of the loads of data
    def run(self, data_path, save_path):
        """
        Executes the data reading, processing, and saving workflow.

        Parameters:
        - data_path: Path to the data file.
        - save_path: Path where the processed NetCDF files will be saved.
        """
        string_attrs, measurements_attrs, data_lists = self.initialize_variables()

        with pd.read_csv(
            data_path,
            chunksize=10**6,
            low_memory=False,
        ) as reader:
            self.process_chunks(reader, data_lists)
            self.create_dataset(data_lists, string_attrs, data_path, save_path)


def main():
    """
    Main function to run the MEDSReader for multiple data files.

    It defines the paths to the data files, initializes a MEDSReader object, and processes each data file.
    """
    meds_2015_2021 = "/mnt/storage6/caio/AW_CAA/CTD_DATA/MEDS_2021/original_data/a_MEDS_profile_prof_2015_2021.csv"
    meds_2009_2014 = "/mnt/storage6/caio/AW_CAA/CTD_DATA/MEDS_2021/original_data/a_MEDS_profile_prof_2009_2014.csv"
    meds_1916_2008 = "/mnt/storage6/caio/AW_CAA/CTD_DATA/MEDS_2021/original_data/a_MEDS_profile_prof_1916_2008.csv"

    data_path_list = [meds_1916_2008, meds_2009_2014, meds_2015_2021]
    meds_reader = MEDSReader()

    for data_path in data_path_list:
        save_path = "/mnt/storage6/caio/AW_CAA/CTD_DATA/MEDS_2021/ncfiles_raw"
        print(f"reading {data_path}...")
        meds_reader.run(data_path, save_path)


if __name__ == "__main__":
    main()
