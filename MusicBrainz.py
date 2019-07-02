import requests
import json
import csv
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from bs4 import BeautifulSoup

def get_soup(url):
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    page = session.get(url)
    contents = page.content
    soup = BeautifulSoup(contents, 'html.parser')
    return soup

def write_list_to_csv(headers,rows,file_name):
    with open(file_name, 'w',encoding="utf-8") as output_file:
        wr = csv.writer(output_file, delimiter='\t')
        wr.writerow(headers)
        wr.writerows(rows)

def read_RA_artist_names_from_file():
    artist_names = []

    top_profiles = json.load(open('data/RA/top_profiles_info.json', 'r', encoding="ISO-8859-1"))
    others_profiles = json.load(open('data/RA/others_profiles_info.json', 'r' , encoding="ISO-8859-1"))

    artist_names.extend(top_profiles.keys())
    artist_names.extend(others_profiles.keys())

    return artist_names

def save_MB_artist_urls_to_file(artist_names,file_name):
    #E.g. Url : https://musicbrainz.org/search?query=Jon+Hopkins&type=artist&method=indexed
    url = "https://musicbrainz.org/search?query={0}&type=artist&method=indexed"
    artist_url_list = list()

    i = 1
    for artist_name in artist_names:
        soup_url = url.format(artist_name.replace(" ","+"))
        soup = get_soup(soup_url)

        print(i,soup_url)
        i = i + 1
        table = soup.find('table', {'class': 'tbl'})
        if table == None:
            artist_url_list.append([artist_name,None])
        else:
            artist_page_url = table.find("a").get('href')
            artist_url_list.append([artist_name,artist_page_url])



    headers = ['artist_name', 'artist_page_url']
    write_list_to_csv(headers,artist_url_list,file_name)

def get_MB_gender(soup):
    gender = soup.find('dd', {'class': 'gender'})
    if gender != None:
        return gender.getText()
    return ''

def get_MB_born(soup):
    born = soup.find('dd', {'class': 'begin-date'})
    if born != None:
        born = born.getText()
        return born[0:born.find('(')]
    return ''

def get_MB_born_in(soup):
    retVal = ''
    born_in = soup.find('dd', {'class': 'begin_area'})
    if born_in != None:
        born_in_list = born_in.findAll('bdi')
        if born_in_list != None:
            for b in born_in_list:
                retVal = retVal + b.getText() + ','
            retVal = retVal[:-1]
    return retVal

def get_MB_area(soup):
    retVal = ''
    area = soup.find('dd', {'class': 'area'})
    if area != None:
        areas = area.findAll('bdi')
        if areas != None:
            for a in areas:
                retVal = retVal + a.getText() + ','
            retVal = retVal[:-1]
    return retVal

def add_columns():
    df = pd.read_csv(file_name, sep='\t')

    df['born_in'] = ''

    df.to_csv(file_name, sep='\t', index=None, header=True)

def save_MB_artist_info_to_file(file_name, start_index):
    df = pd.read_csv(file_name, sep='\t')
    end_index = len(df)

    for i in range(start_index, end_index):
        df.iloc[i] = crawl_MB_artist_info(df.iloc[i])

        if i % 1000 == 0:
            df.to_csv(file_name, sep='\t', index=None, header=True)
            df = pd.read_csv(file_name, sep='\t')

        print(i, df.iloc[i]['artist_name'])

def crawl_MB_artist_info(df_row):

    url = 'https://musicbrainz.org{0}'

    artist_url = df_row['artist_page_url']
    if pd.isnull(artist_url) == False:
        soup_url = url.format(artist_url)
        soup = get_soup(soup_url)

        df_row['gender'] = get_MB_gender(soup)
        df_row['born'] = get_MB_born(soup)
        df_row['born_in'] = get_MB_born_in(soup)
        df_row['area'] = get_MB_area(soup)

        #Get External Links
        url_relationships = 'https://musicbrainz.org{0}/relationships'
        soup_url = url_relationships.format(artist_url)
        soup = get_soup(soup_url)

        df_row['external_links'] = get_relationships(soup)

    return df_row

def get_relationships(soup):
    relationships = {}
    tables = soup.findAll('table', {'class': 'details'})
    if tables != None:
        for table in tables:
            rows = table.findAll('tr')
            if rows != None:
                for r in rows:
                    rel_type = r.find('th').getText()[:-1]
                    rel_urls = ''

                    rel_url = r.findAll('bdi')
                    if rel_url != None:
                        for u in rel_url:
                            rel_urls = rel_urls + u.getText() + ' '
                    relationships[rel_type] = rel_urls

    return relationships

def crawl_MB_artist_page_to_test(artist_page_url):
    url = 'https://musicbrainz.org/artist{0}'
    url_relationships = 'https://musicbrainz.org/artist{0}/relationships'
    if pd.isnull(artist_page_url) == False:
        soup_url = url.format(artist_page_url)
        print(soup_url)
        soup = get_soup(soup_url)

        artist_info = {}
        artist_info['gender'] = get_MB_gender(soup)
        artist_info['born'] = get_MB_born(soup)
        artist_info['born_in'] = get_MB_born_in(soup)
        artist_info['area'] = get_MB_area(soup)



        soup_url = url_relationships.format(artist_page_url)
        print(soup_url)
        soup = get_soup(soup_url)

        artist_info['relationships'] = get_relationships(soup)

        print(artist_info)

if __name__ == '__main__':
    #RA : https://www.residentadvisor.net/
    #MB : https://musicbrainz.org/

    crawl_artist_urls_from_MB = False
    crawl_artists_from_MB = True
    crawl_test = False
    add_column = False

    artist_url_to_test = '/0b0c25f4-f31c-46a5-a4fb-ccbf53d663bd'
    file_name = 'data/MB/MB_artist_page_urls.tsv'

    if crawl_artist_urls_from_MB == True:
        artist_names = read_RA_artist_names_from_file()
        #comment to crawl all artists
        #artist_names = artist_names[0:3000]

        save_MB_artist_urls_to_file(artist_names,file_name)

    if crawl_artists_from_MB == True:
        save_MB_artist_info_to_file(file_name, 0)

    if crawl_test == True:
        crawl_MB_artist_page_to_test(artist_url_to_test)

    if add_column == True:
        add_columns()


