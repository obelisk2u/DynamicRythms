import pandas as pd
import modelling_utils as mu

def create_merged_data():
    outages = mu.parse_outage()
    storms = mu.parse_storm()
    df = mu.merge(outages, storms)
    df.to_csv('merged_data.csv', index=False)

create_merged_data()

