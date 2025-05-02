import pandas as pd

df_storm = pd.read_csv(r'data\NOAA_StormEvents\StormEvents_details-ftp_v1.0_d2015_c20240716.csv')

def change_date_storm(df_storm):
    df_storm['BEGIN_DATE_TIME'] = pd.to_datetime(df_storm['BEGIN_DATE_TIME']).dt.date
    df_storm['END_DATE_TIME'] = pd.to_datetime(df_storm['END_DATE_TIME']).dt.date
    return df_storm

def keep_c_storm(df_storm):
    df_storm = df_storm[df_storm['CZ_TYPE'] == 'C']
    return df_storm

def make_fips_storm(storms):
    storms['STATE_FIPS'] = storms['STATE_FIPS'].astype(str).str.zfill(2)
    storms['CZ_FIPS'] = storms['CZ_FIPS'].astype(str).str.zfill(3)
    storms['FIPS'] = storms['STATE_FIPS'] + storms['CZ_FIPS']
    return storms

def change_date_outage(outage):
    outage['run_start_time'] = pd.to_datetime(outage['run_start_time'])
    outage['run_start_time'] = outage['run_start_time'].dt.date
    return outage

def combine_customers(df_outage):
    df_outage_grouped = df_outage.groupby(['run_start_time', 'fips_code'], as_index=False)['customers_out'].sum()
    return df_outage_grouped



df_outage = pd.read_csv(r'data\eaglei_data\eaglei_outages_2015.csv')


df_storm = change_date_storm(df_storm)
df_storm = keep_c_storm(df_storm)
df_storm = make_fips_storm(df_storm)

df_outage = change_date_outage(df_outage)
print("Length before group:", len(df_outage))
print("Customers before group:", df_outage['customers_out'].sum())
df_outage = combine_customers(df_outage)
print("Length after group:", len(df_outage))
print("Customers after group:", df_outage['customers_out'].sum())

df_outage.to_csv('df_outage_grouped.csv', index=False)


df_outage['fips_code'] = df_outage['fips_code'].astype(str).str.zfill(5)
df_storm['FIPS'] = df_storm['FIPS'].astype(str).str.zfill(5)



