import os
import glob
import pandas as pd
currentDir = os.getcwd()
print(currentDir)
files_directory = '/data/PF/'
os.chdir(currentDir+files_directory)
extension = 'tsv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
df = pd.DataFrame(columns= ['artist_name', 'URL', 'gender', 'born in', 'position', 'genres', 'site','booking','external_links'])
for f in all_filenames:
    newFile = pd.read_csv(f, sep='\t', encoding='UTF-8', header=None ,names=['artist_name', 'URL', 'gender', 'born in', 'position', 'genres', 'site','booking','external_links'])
    df = df.append(newFile)
df.to_csv( "PF_artists_combined.csv", index=False, encoding='UTF-8',header=True)