from translate import Translator
import pandas as pd
from tqdm import tqdm

tqdm.pandas()
file_address = 'data/PF/'
file_name_source = 'PF_artists_combined.csv'
file_name_dest = 'PF_artists_combined_translated.csv'
df = pd.read_csv(file_address+file_name_source,encoding='UTF-8')
translator = Translator(from_lang="nl",to_lang="en")
df['gender'] = df['gender'].replace('vrouw' ,'woman');
df['born in'] = df['born in'].progress_apply(lambda x: translator.translate(str(x)) if x is not None else x)
df.to_csv(file_name_dest, index=False, encoding='UTF-8',header=True)