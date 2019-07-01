import requests
import json
import csv
import pandas as pd
import re
import requests
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
    twitter = external_links.find('a', title=re.compile('twitter'))
    itunes = external_links.find('a', title=re.compile('itunes'))
    instagram = external_links.find('a', title=re.compile('instagram'))
    youtube = external_links.find('a', title=re.compile('youtube'))

    if soundcloud != None:
        links['soundcloud'] = baseURL+soundcloud.get('href')

    if spotify != None:
        links['spotify'] = baseURL+spotify.get('href')

    if facebook != None:
        links['facebook'] = baseURL+facebook.get('href')

    if twitter != None:
        links['twitter'] = baseURL + twitter.get('href')

    if itunes != None:
        links['itunes'] = baseURL + itunes.get('href')

    if instagram != None:
        links['instagram'] = baseURL + instagram.get('href')

    if youtube != None:
        links['youtube'] = baseURL + youtube.get('href')

    return links

def get_genres(soup):
    genre = soup.find("td", text="Genres")
    if genre is not None:
        return genre.nextSibling.text
    return ''

def get_webSite(soup):
    site = soup.find("td", text="Site")
    if site is not None:
        return (site.nextSibling.find('a')['href'])
    return ''

def get_bookingWebsite(soup):
    booking = soup.find("td", text="Boekingen")
    if booking is not None:
        return (booking.nextSibling.find('a')['href'])
    return ''


def save_MB_artist_info_to_file(file_name):
    #df = pd.read_csv('data/MB/c_MB_artist_page_urls.tsv', sep='\t')
    df = pd.read_csv(file_name, sep='\t')

    df['gender'] = ''
    df['born'] = ''
    df['position'] = ''
    df['genres'] = ''
    df['site'] = ''
    df['booking'] = ''
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
            row['genres'] = get_genres(soup)
            row['site'] = get_webSite(soup)
            row['booking'] = get_bookingWebsite(soup)


            print(i,row['artist_name'],row['gender'],row['born'],row['position'], row['genres'],row['site'],row['booking'],row['external_links'])

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
        artist_names = artist_names[0:5]

        save_MB_artist_urls_to_file(artist_names,file_name)

    if crawl_artists_from_MB == True:
        save_MB_artist_info_to_file(file_name)