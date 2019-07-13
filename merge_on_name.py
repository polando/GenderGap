import pandas as pd
from pandas import read_excel
import xlsxwriter


file_name_source_gigs = "RA_all_gigs_extended_version.tsv"
file_name_source_artist = "crawl_data_lifspan.csv"
file_name_dest = "crawl_data_gig.csv"
file_address = 'data/Cleaned Data/'


df1 = pd.read_csv(file_address + file_name_source_artist,sep='\t' ,error_bad_lines=False, lineterminator='\n',encoding='ISO-8859-1')
df1 = df1.drop(df1.columns.difference(['artist_name','gender']), axis=1)
df2 = pd.read_csv("data/"+file_name_source_gigs,sep='\t' ,error_bad_lines=False, lineterminator='\n',encoding='ISO-8859-1',na_values='None',index_col=False)
merged = pd.merge(df1, df2, on='artist_name')
with open(file_address + file_name_dest, 'w', encoding='UTF-8') as output_file:
    merged.to_csv(output_file, sep='\t', index=None, header=True)
merged

