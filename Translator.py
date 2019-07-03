from translate import Translator
import pandas as pd
import json

def makeDic(df):
    countries = set(df['born in'])
    dictionary = dict()
    for country in countries:
        if country is not None:
            dictionary[country] = translator.translate(str(country))
    return dictionary

def saveDic(dictionary,file_address,dictName):
    json.dump(dictionary, open(file_address + dictName , 'w', encoding="UTF-8"), ensure_ascii=False)

def loadDic(file_address,dictName):
    with open(file_address+dictName, encoding='utf-8') as fh:
        dictionary = json.load(fh)
    return dictionary


file_address = 'data/PF/'
file_name_source = 'PF_artists_combined.csv'
file_name_dest = 'PF_artists_combined_translated.csv'
dictName = "dict.json"
translator = Translator(to_lang="en",from_lang="nl")
df = pd.read_csv(file_address+file_name_source,encoding='UTF-8')
#dictionary = makeDic(df)
#saveDic(dictionary,file_address)
dictionary = loadDic(file_address,dictName)
df['born in'] = df['born in'].fillna("not given")
df['born in'] = df['born in'].apply(lambda x: dictionary[x] if x is not None else x)
df['gender'] = df['gender'].replace('vrouw','woman')
df.to_csv(file_address+file_name_dest, sep='\t', index=None, header=True)



