import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime
import os


def read_MEDS(data_path, save_path):
    string_attrs = ['orig_cruise_id', 'instrument_type', 'station_no', 'lat', 'lon', 'datestr', 'timestamp',
                    'shallowest_depth', 'deepest_depth']
    measurements_attrs = ['depth', 'press', 'temp', 'psal']
    data_lists = {attr: [] for attr in string_attrs + measurements_attrs}
    parent_index = []
    i = 0

    with pd.read_csv(data_path, chunksize=10 ** 6, low_memory=False, ) as reader:
        for chunk in reader:
            print(chunk['TEMP'][chunk['TEMP'] > 90])

            # grouped_df = chunk.groupby(
            #     ['DATA_TYPE', 'CR_NUMBER', 'STN_NUMBER', 'SOURCE_ID', 'OBS_YEAR', 'OBS_MONTH', 'OBS_DAY', 'OBS_TIME',
            #      'LONGITUDE (+E)', 'LATITUDE (+N)'])
            # for group, data in grouped_df:
            #     data_type, cr_number, stn_number, source_id, obs_year, obs_month, obs_day, obs_time, lon, lat = group
                # data_lists['orig_cruise_id'].append(cr_number)
                # data_lists['instrument_type'].append(data_type)
                # data_lists['station_no'].append(stn_number)
                # data_lists['lon'].append(lon)
                # data_lists['lat'].append(lat)
                # datestr = datetime(obs_year, obs_month, obs_day, obs_time // 100, obs_time % 100)
                # timestamp = datestr.timestamp()
                # datestr = datetime.strftime(datestr, "%Y/%m/%d %H:%M:%S")
                # data_lists['datestr'].append(datestr)
                # data_lists['timestamp'].append(timestamp)
                # data_lists['temp'].extend(data['TEMP'])
                # if data['TEMP'].values.any() > 90:
                #     print(data['TEMP'].values)
                # data_lists['psal'].extend(data['PSAL'])
                # if data['D_P_CODE'].values.all() == 'D':
                #     data_lists['depth'].extend(data['DEPTH_PRESS'])
                #     data_lists['press'].extend([np.nan] * len(data['DEPTH_PRESS']))
                # elif data['D_P_CODE'].values.all() == 'P':
                #     data_lists['press'].extend(data['DEPTH_PRESS'])
                #     data_lists['depth'].extend([np.nan] * len(data['DEPTH_PRESS']))
                # if len(data['DEPTH_PRESS'].values) > 1:
                #     data_lists['shallowest_depth'].append(min(data['DEPTH_PRESS'][data['DEPTH_PRESS'] != 0]))
                # else:
                #     data_lists['shallowest_depth'].append(min(data['DEPTH_PRESS']))
                # data_lists['deepest_depth'].append(max(data['DEPTH_PRESS']))
                # parent_index.extend([i] * len(data['DEPTH_PRESS']))
                # i += 1
        # create_dataset(data_lists, parent_index, string_attrs, data_path, save_path)


def create_dataset(data_lists, parent_index, string_attrs, data_path, save_path):
    ds = xr.Dataset(
        coords=dict(
            timestamp=(['profile'], data_lists['timestamp']),
            lat=(['profile', ], data_lists['lat']),
            lon=(['profile', ], data_lists['lon']),
        ),
        data_vars=dict(
            **{attr: xr.DataArray(data_lists[attr], dims=['profile']) for attr in string_attrs if
               attr not in ['lat', 'lon', 'timestamp']},
            parent_index=xr.DataArray(parent_index, dims=['obs']),

            # measurements
            depth=xr.DataArray(data_lists['depth'], dims=['obs']),
            press=xr.DataArray(data_lists['press'], dims=['obs']),
            temp=xr.DataArray(data_lists['temp'], dims=['obs']),
            psal=xr.DataArray(data_lists['psal'], dims=['obs']),
        ),
        attrs=dict(),
    )
    netcdf_filename = 'myfile.nc'
    match data_path[data_path.rfind("/") + 1:]:
        case "a_MEDS_profile_prof_2015_2021.csv":
            netcdf_filename = "MEDS2015_2021_meds_raw.nc"
        case "a_MEDS_profile_prof_2009_2014.csv":
            netcdf_filename = "MEDS2009_2014_meds_raw.nc"
        case "a_MEDS_profile_prof_1916_2008.csv":
            netcdf_filename = "MEDS1916_2008_meds_raw.nc"
        case _:
            print("wrong file type")

    if not os.path.isdir(save_path):
        os.mkdir(save_path)
    os.chdir(save_path)

    ds.to_netcdf(netcdf_filename, unlimited_dims={'obs': True})
