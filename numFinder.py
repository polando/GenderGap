import pandas as pd
from pandas import read_excel

file_name_source = "Combine_data.xlsx"
file_name_dest = "releases_merged_ready.csv"
file_address = 'data/Cleaned Data/'
my_sheet = 'Sheet1'



#df = pd.read_csv(file_address + file_name_source,sep='\t' ,error_bad_lines=False, lineterminator='\n',encoding='ISO-8859-1')
df = read_excel(file_address+file_name_source, sheet_name = my_sheet)
#df
df['num_of_releases'] = df.groupby('artist_name')['artist_name'].transform('count')
df['sum_of_price'] = df.groupby('artist_name')['price'].transform('sum')
df = df.drop_duplicates(subset='artist_name', keep='first')
df = df.round({"sum_of_price":4})
with open(file_address+file_name_dest, 'w', encoding='UTF-8') as output_file:
    df.to_csv(output_file, sep='\t', index=None, header=True)
