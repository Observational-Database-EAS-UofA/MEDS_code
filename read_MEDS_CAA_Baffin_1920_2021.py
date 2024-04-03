import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime
import os


def initialize_variables():
    string_attrs = ['orig_cruise_id', 'instrument_type', 'station_no', 'lat', 'lonlat_flag', 'lon', 'datestr',
                    'timestamp', 'shallowest_depth', 'deepest_depth', 'datestr_flag', 'parent_index']
    measurements_attrs = ['depth', 'press', 'temp', 'psal', 'depth_flag', 'temp_flag', 'psal_flag']
    data_lists = {year_range: {attr: [] for attr in string_attrs + measurements_attrs} for year_range in
                  ['1916_2000', '2001_2010', '2011_2021']}
    return string_attrs, measurements_attrs, data_lists


def get_dict_index(data_lists, year):
    for k in data_lists.keys():
        if int(k[:4]) <= int(year) <= int(k[5:]):
            return k


# these functions create the .nc file. If the file already exist, it will append the data into this existent file
def create_dataset(data_lists, string_attrs, data_path, save_path):
    dataset = data_path.split("/")[-3]

    if not os.path.isdir(save_path):
        os.mkdir(save_path)
    os.chdir(save_path)
    current_files = [f[5:14] for f in os.listdir() if f.endswith('.nc')]
    for key, data_list in data_lists.items():
        if key not in current_files and len(data_list['timestamp']) != 0:
            ds = xr.Dataset(
                coords=dict(
                    timestamp=(['profile'], data_list['timestamp']),
                    lat=(['profile', ], data_list['lat']),
                    lon=(['profile', ], data_list['lon']),
                ),
                data_vars=dict(
                    **{attr: xr.DataArray(data_list[attr], dims=['profile']) for attr in string_attrs if
                       attr not in ['lat', 'lon', 'timestamp', 'parent_index', 'datestr']},
                    datestr=xr.DataArray(data_list['datestr'], dims=['profile'], attrs={'timezone': 'UTC'}),
                    # measurements
                    parent_index=xr.DataArray(data_list['parent_index'], dims=['obs']),
                    depth=xr.DataArray(data_list['depth'], dims=['obs']),
                    depth_flag=xr.DataArray(data_list['depth_flag'], dims=['obs']),
                    press=xr.DataArray(data_list['press'], dims=['obs']),
                    temp=xr.DataArray(data_list['temp'], dims=['obs']),
                    temp_flag=xr.DataArray(data_list['temp_flag'], dims=['obs']),
                    psal=xr.DataArray(data_list['psal'], dims=['obs']),
                    psal_flag=xr.DataArray(data_list['psal_flag'], dims=['obs']),
                ),
                attrs=dict(
                    dataset_name=dataset,
                    creation_date=str(datetime.now().strftime("%Y-%m-%d %H:%M")),
                ),
            )

            ds.to_netcdf(f"MEDS_{key}_raw.nc")
        elif key in current_files and len(data_list['timestamp']) != 0:
            os.chdir(save_path)
            ds = xr.open_dataset(f"MEDS_{key}_raw.nc")
            ds = xr.Dataset(
                coords=dict(
                    timestamp=(['profile'], np.append(ds['timestamp'], data_list['timestamp'])),
                    lat=(['profile', ], np.append(ds['lat'], data_list['lat'])),
                    lon=(['profile', ], np.append(ds['lat'], data_list['lon'])),
                ),
                data_vars=dict(
                    **{attr: xr.DataArray(np.append(ds[attr], data_list[attr]), dims=['profile']) for attr in
                       string_attrs if
                       attr not in ['lat', 'lon', 'timestamp', 'parent_index']},
                    parent_index=xr.DataArray(
                        np.append(ds['parent_index'],
                                  np.array(data_list['parent_index']) + ds['parent_index'].values[-1] + 1),
                        dims=['obs']),
                    # measurements
                    depth=xr.DataArray(np.append(ds['depth'], data_list['depth']), dims=['obs']),
                    depth_flag=xr.DataArray(np.append(ds['depth_flag'], data_list['depth_flag']), dims=['obs']),
                    press=xr.DataArray(np.append(ds['press'], data_list['press']), dims=['obs']),
                    temp=xr.DataArray(np.append(ds['temp'], data_list['temp']), dims=['obs']),
                    temp_flag=xr.DataArray(np.append(ds['temp_flag'], data_list['temp_flag']), dims=['obs']),
                    psal=xr.DataArray(np.append(ds['psal'], data_list['psal']), dims=['obs']),
                    psal_flag=xr.DataArray(np.append(ds['psal_flag'], data_list['psal_flag']), dims=['obs']),
                ),
                attrs=dict(
                    dataset_name=dataset,
                    creation_date=str(datetime.now().strftime("%Y-%m-%d %H:%M")),
                ),
            )

            ds.to_netcdf(f"MEDS_{key}_raw.nc")


def process_chunks(reader, data_lists):
    for chunk in reader:
        grouped_df = chunk.groupby(
            ['DATA_TYPE', 'CR_NUMBER', 'STN_NUMBER', 'SOURCE_ID', 'OBS_YEAR', 'OBS_MONTH', 'OBS_DAY', 'OBS_TIME',
             'Q_DATE_TIME', 'LONGITUDE (+E)', 'LATITUDE (+N)', 'Q_POS'])
        for group, data in grouped_df:
            data_type, cr_number, stn_number, source_id, obs_year, obs_month, obs_day, obs_time, datestr_flag, lon, lat, lonlat_flag = group
            index = get_dict_index(data_lists, obs_year)
            data_lists[index]['orig_cruise_id'].append(cr_number)
            data_lists[index]['instrument_type'].append(data_type)
            data_lists[index]['station_no'].append(stn_number)
            data_lists[index]['lon'].append(lon)
            data_lists[index]['lat'].append(lat)
            data_lists[index]['lonlat_flag'].append(lonlat_flag)
            datestr = datetime(obs_year, obs_month, obs_day, obs_time // 100, obs_time % 100)
            data_lists[index]['timestamp'].append(datestr.timestamp())
            data_lists[index]['datestr'].append(datetime.strftime(datestr, "%Y/%m/%d %H:%M:%S"))
            data_lists[index]['datestr_flag'].append(datestr_flag)
            if len(data['DEPTH_PRESS'].values) > 1:
                data_lists[index]['shallowest_depth'].append(min(data['DEPTH_PRESS'][data['DEPTH_PRESS'] != 0]))
            else:
                data_lists[index]['shallowest_depth'].append(min(data['DEPTH_PRESS']))
            data_lists[index]['deepest_depth'].append(max(data['DEPTH_PRESS']))

            data_lists[index]['temp'].extend(data['TEMP'])
            data_lists[index]['temp_flag'].extend(data['Q_TEMP'])
            data_lists[index]['psal'].extend(data['PSAL'])
            data_lists[index]['psal_flag'].extend(data['Q_PSAL'])
            if data['D_P_CODE'].values.all() == 'D':
                data_lists[index]['depth'].extend(data['DEPTH_PRESS'])
                data_lists[index]['press'].extend([np.nan] * len(data['DEPTH_PRESS']))
            elif data['D_P_CODE'].values.all() == 'P':
                data_lists[index]['press'].extend(data['DEPTH_PRESS'])
                data_lists[index]['depth'].extend([np.nan] * len(data['DEPTH_PRESS']))
            data_lists[index]['depth_flag'].extend(data['DP_FLAG'])
            if len(data_lists[index]['parent_index']) > 0:
                last_parent_index = data_lists[index]['parent_index'][-1]
                data_lists[index]['parent_index'].extend([(last_parent_index + 1)] * len(data['DEPTH_PRESS']))
            else:
                data_lists[index]['parent_index'].extend([0] * len(data['DEPTH_PRESS']))


# this function reads the MEDS profile data. It reads the csv files in chunks because of the loads of data
def read_MEDS(data_path, save_path):
    string_attrs, measurements_attrs, data_lists = initialize_variables()

    with pd.read_csv(data_path, chunksize=10 ** 6, low_memory=False, ) as reader:
        process_chunks(reader, data_lists)
        create_dataset(data_lists, string_attrs, data_path, save_path)
