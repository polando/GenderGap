import pandas as pd
from pandas import read_excel
import xlsxwriter

file_name_source = "Combine_data.xlsx"
file_name_dest = "releases_merged_ready.csv"
file_address = 'data/Cleaned Data/'
my_sheet = 'Sheet1'



#df = pd.read_csv(file_address + file_name_source,sep='\t' ,error_bad_lines=False, lineterminator='\n',encoding='ISO-8859-1')
df = read_excel(file_address+file_name_source, sheet_name = my_sheet , dtype=str)
df
#df['num_of_releases'] = df.groupby('label_name')['artist_name'].transform('count')
#df['sum_of_price'] = df.groupby('artist_name')['price'].transform('sum')

group_data  = df.groupby(['label_name','gender']).size()
#print(group_data)
df2 = group_data.to_frame().reset_index()
df2 = df2.sort_values('label_name')
df2.columns = ['label_name','gender','num_of_release']
dfpivot = df2.pivot_table('num_of_release', ['label_name'], 'gender')
dfpivot.fillna(0, inplace=True)
dfpivot.insert(0, 'label_name', dfpivot.index)
#medals['label_name'] = medals.index
#medals
#df = df.drop_duplicates(subset='artist_name', keep='first')
#df = df.round({"sum_of_price":4})
#with open(file_address+file_name_dest, 'w', encoding='UTF-8') as output_file:
#    df.to_csv(output_file, sep='\t', index=None, header=True)

writer = pd.ExcelWriter(file_address+'releases_label.xlsx', engine='xlsxwriter')
dfpivot.to_excel(writer, sheet_name='Sheet1', index=False)
writer.save()