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

    for artist_name in artist_names:
        soup_url = url.format(artist_name.replace(" ","+"))
        soup = get_soup(soup_url)

        print(soup_url)

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

def get_MB_area(soup):
    area = soup.find('dd', {'class': 'area'})
    if area != None:
        return area.find('bdi').getText()
    return ''

def get_external_links(soup):
    external_links = soup.find('ul', {'class': 'external_links'})
    if external_links == None:
        return {}


    links = {}
    wikidata = external_links.find('li', {'class': 'wikidata-favicon'})
    songkick = external_links.find('li', {'class': 'songkick-favicon'})
    residentadvisor = external_links.find('li', {'class': 'residentadvisor-favicon'})

    if wikidata != None:
        links['wikidata'] = wikidata.find('a').get('href')

    if songkick != None:
        links['songkick'] = songkick.find('a').get('href')

    if residentadvisor != None:
        links['residentadvisor'] = residentadvisor.find('a').get('href')

    return links

def save_MB_artist_info_to_file(file_name):
    #df = pd.read_csv('data/MB/c_MB_artist_page_urls.tsv', sep='\t')
    df = pd.read_csv(file_name, sep='\t')

    df['gender'] = ''
    df['born'] = ''
    df['area'] = ''
    df['external_links'] = ''

    i=1
    url = 'https://musicbrainz.org{0}'
    for index, row in df.iterrows():
        artist_url = row['artist_page_url']
        if pd.isnull(artist_url) == False:
            soup_url = url.format(artist_url)
            soup = get_soup(soup_url)

            row['gender'] = get_MB_gender(soup)
            row['born'] = get_MB_born(soup)
            row['area'] = get_MB_area(soup)
            row['external_links'] = get_external_links(soup)

            print(i,row['artist_name'],row['gender'],row['born'],row['area'],row['external_links'])

        i = i+1

    #df.to_csv('data/MB/c_MB_artist_page_urls.tsv', sep='\t', index=None, header=True)
    df.to_csv(file_name, sep='\t', index=None, header=True)

if __name__ == '__main__':
    #RA : https://www.residentadvisor.net/
    #MB : https://musicbrainz.org/

    crawl_artist_urls_from_MB = True
    crawl_artists_from_MB = True

    file_name = 'data/MB/MB_artist_page_urls.tsv'

    if crawl_artist_urls_from_MB == True:
        artist_names = read_RA_artist_names_from_file()
        #comment to crawl all artists
        artist_names = artist_names[0:3000]

        save_MB_artist_urls_to_file(artist_names,file_name)

    if crawl_artists_from_MB == True:
        save_MB_artist_info_to_file(file_name)