import requests
import json
import csv
import pandas as pd
import re
import socks
import socket
from lxml.html import fromstring
import requests
from itertools import cycle
import random
from retrying import retry
from torrequest import TorRequest


from bs4 import BeautifulSoup

# def get_proxies():
#     url = 'https://free-proxy-list.net/'
#     response = requests.get(url)
#     parser = fromstring(response.text)
#     proxies = set()
#     for i in parser.xpath('//tbody/tr')[:10]:
#         if i.xpath('.//td[7][contains(text(),"yes")]'):
#             proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
#             proxies.add(proxy)
#     return proxies


def get_soup(url):
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36',"Accept-Language": "en-US"}
    # proxies = {"http": "http://127.0.0.1:18275"}
    # proxies = {'http': 'socks5://127.0.0.1:9050',
    #                    'https': 'socks5://127.0.0.1:9050'}
    # socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, '127.0.0.1', 9150)
    # socket.socket = socks.socksocket
    with TorRequest(proxy_port=9050, ctrl_port=9051, password=None) as tr:
        page = tr.get(url)
    contents = page.content
    soup = BeautifulSoup(contents, 'html.parser')
    return soup


def write_list_to_csv(headers,rows,file_name):
    with open(file_name, 'w') as output_file:
        wr = csv.writer(output_file, delimiter='\t')
        wr.writerow(headers)
        wr.writerows(rows)

def read_RA_artist_names_from_file():
    artist_names = []

    top_profiles = json.load(open('data/RA/top_profiles_info.json', 'r',encoding="ISO-8859-1"))
    others_profiles = json.load(open('data/RA/others_profiles_info.json', 'r'))

    artist_names.extend(top_profiles.keys())
    artist_names.extend(others_profiles.keys())

    return artist_names

def save_MB_artist_urls_to_file(artist_names,file_name):


    #E.g. Url : https://musicbrainz.org/search?query=Jon+Hopkins&type=artist&method=indexed
    url = "https://partyflock.nl/search?enc=%E2%98%A0&TERMS={0}&ELEMENT=artist"
    artist_url_list = list()


    for artist_name in artist_names:
        soup_url = url.format(artist_name.replace(" ","+"))
        soup = get_soup(soup_url)

        print(soup_url)

        table = soup.find('div', {'class': 'search'})
        if table == None:
            artist_url_list.append([artist_name,None])
        else:
            artist_page_url = table.find("a").get('href')
            artist_url_list.append([artist_name,artist_page_url])



    headers = ['artist_name', 'artist_page_url']
    write_list_to_csv(headers,artist_url_list,file_name)

def get_MB_gender(soup):

    gender = soup.find("td", itemprop="gender")
    if gender != None:
        return gender.getText()
    return ''

def get_MB_born(soup):
    born = soup.find("span", itemprop="nationality")
    if born != None:
        return born.find("a").getText()
    return ''

def get_MB_pos(soup):
    pos = soup.find("td", itemprop="jobTitle")
    if pos != None:
        return pos.getText()
    return ''

def get_external_links(soup,baseURL):
    external_links = soup.find('tr', {'class': 'presencerow'})
    if external_links == None:
        return {}


    links = {}
    soundcloud = external_links.find('a', title= re.compile('soundcloud'))
    spotify = external_links.find('a', title= re.compile('spotify'))
    facebook = external_links.find('a', title= re.compile('facebook'))

    if soundcloud != None:
        links['soundcloud'] = baseURL+soundcloud.get('href')

    if spotify != None:
        links['spotify'] = baseURL+spotify.get('href')

    if facebook != None:
        links['facebook'] = baseURL+facebook.get('href')

    return links



def save_MB_artist_info_to_file(file_name):
    #df = pd.read_csv('data/MB/c_MB_artist_page_urls.tsv', sep='\t')
    df = pd.read_csv(file_name, sep='\t')

    df['gender'] = ''
    df['born'] = ''
    df['position'] = ''
    df['external_links'] = ''

    i=1
   # url = 'https://partyflock.nl{0}'
    baseURL = 'https://partyflock.nl'
    url = baseURL+'{0}'
    for index, row in df.iterrows():
        artist_url = row['artist_page_url']
        if pd.isnull(artist_url) == False:
            soup_url = url.format(artist_url)
            soup = get_soup(soup_url)
            row['gender'] = get_MB_gender(soup)
            row['born'] = get_MB_born(soup)
            row['position'] = get_MB_pos(soup)
            row['external_links'] = get_external_links(soup, baseURL)

            print(i,row['artist_name'],row['gender'],row['born'],row['position'],row['external_links'])

        i = i+1

    #df.to_csv('data/MB/c_MB_artist_page_urls.tsv', sep='\t', index=None, header=True)
    df.to_csv(file_name, sep='\t', index=None, header=True)

if __name__ == '__main__':
    #RA : https://www.residentadvisor.net/

    crawl_artist_urls_from_MB = True
    crawl_artists_from_MB = True

    file_name = 'data/PF/PF_artist_page_urls.tsv'


    if crawl_artist_urls_from_MB == True:
        artist_names = read_RA_artist_names_from_file()
        #comment to crawl all artists
        artist_names = artist_names[0:3]

        save_MB_artist_urls_to_file(artist_names,file_name)

    if crawl_artists_from_MB == True:
        save_MB_artist_info_to_file(file_name)