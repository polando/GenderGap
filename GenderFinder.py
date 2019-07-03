import requests
import json
import csv
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import pandas as pd

from bs4 import BeautifulSoup


def get_soup(url):
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    page = session.get(url)

    # page = requests.get(url)
    contents = page.content
    soup = BeautifulSoup(contents, 'html.parser')
    return soup


def get_artists_names(df):
    matrix = df[df.columns[0]].as_matrix()
    artist_names = matrix.tolist()
    return artist_names


def read_file_save_to_df(fileName):
    df = pd.read_csv(fileName, sep=';', error_bad_lines=False)
    return df


def prepare_and_save_data(artist_names):
    # E.g. Url : https://musicbrainz.org/search?query=Jon+Hopkins&type=artist&method=indexed
    url = "https://musicbrainz.org/search?query={0}&type=artist&method=indexed"
    artist_url_list = list()

    for i in range(31219, len(artist_names)):
        artist_names[i] = str(artist_names[i])
        soup_url = url.format(str(artist_names[i]).replace(" ", "+"))
        soup = get_soup(soup_url)

        print(soup_url)
        print(i)
        table = soup.find('table', {'class': 'tbl'})
        if table == None:
            artist_url_list.append([artist_names[i], None])
        else:
            artist_page_url = table.find("a").get('href')
            artist_url_list.append([artist_names[i], artist_page_url])

        artist_url_and_name = pd.DataFrame(artist_url_list, columns=['artist_name', 'artist_page_url'])
        artist_info = find_gender_by_URL(artist_url_and_name)
        merged_data = pd.merge(df_original, artist_info, on='artist_name')
        if i > 0:
            merged_data.to_csv(file_name_result, sep='\t', encoding='utf-8-sig', header=False, mode="a")
        else:
            merged_data.to_csv(file_name_result, sep='\t', encoding='utf-8-sig')
        artist_url_list = []


def get_MB_gender(soup):
    gender = soup.find('dd', {'class': 'gender'})
    if gender != None:
        return gender.getText()
    return ''


def find_gender_by_URL(df):
    df['gender'] = ''

    i = 1
    url = 'https://musicbrainz.org{0}'
    for index, row in df.iterrows():
        artist_url = row['artist_page_url']
        if pd.isnull(artist_url) == False:
            soup_url = url.format(artist_url)
            soup = get_soup(soup_url)

            row['gender'] = get_MB_gender(soup)

        i = i + 1

    return df


if __name__ == '__main__':
    # RA : https://www.residentadvisor.net/
    # MB : https://musicbrainz.org/

    crawl_artist_urls_from_MB = True
    crawl_artists_from_MB = True

    file_name_result = 'data/releases_merged_gender.tsv'
    file_name_source = 'data/releases_merged.tsv'

    df_original = read_file_save_to_df(file_name_source)

    df_no_dup = df_original.drop_duplicates(subset=['artist_name'], keep='first')

    print("dup"+str(df_no_dup.count()))

    artist_names = get_artists_names(df_no_dup)
    prepare_and_save_data(artist_names)
