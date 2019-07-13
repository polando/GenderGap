import pandas as pd
from pandas import read_excel
import xlsxwriter


file_name_source = "crawl_data.csv"
file_name_dest = "crawl_data_lifspan.csv"
file_address = 'data/Cleaned Data/'


df = pd.read_csv(file_address + file_name_source,sep='\t' ,error_bad_lines=False, lineterminator='\n',encoding='ISO-8859-1')
df['max_release_year'] = df.groupby(['artist_name'])['year'].transform(max)
df['min_release_year'] = df.groupby(['artist_name'])['year'].transform(min)
df['life_span'] = df['max_release_year'] - df['min_release_year']

with open(file_address + file_name_dest, 'w', encoding='UTF-8') as output_file:
    df.to_csv(output_file, sep='\t', index=None, header=True)