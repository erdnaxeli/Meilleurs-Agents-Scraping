# -*- coding: utf-8 -*-
"""
Created on Friday, 11 January 2019

@author: Arthur Journe

Scraping data from the website "www.meilleuragents.com" using rotating proxies
"""
# Imports
import time
from lxml import html
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import random

# Auxiliary functions
def test_house_and_flat(soup):
    maison = soup.find_all("p", {'class':"text--sm text--small"})
    appart = soup.find_all("p", {'class':"text--small text--sm"})
    for i in range(len(appart)):
        if appart[i].text == 'Loyer':
            del appart[i]
    if len(maison) > 0 and len(appart) == 0:
        return('House')
    elif len(maison) == 0 and len(appart) > 0:
        return('Flat')
    elif len(maison) > 0 and len(appart) > 0:
        return('Both')

backlinks_data = pd.read_csv('meilleursagents.com-backlinks.csv')
backlinks_url = list(backlinks_data['Source url'])
def get_random_referer():
    return random.choice(backlinks_url)

def get_random_ua():
    random_ua =''
    ua_file = 'user_agents.txt'
    try:
        with open(ua_file) as f:
            lines = f.readlines()
        if len(lines) > 0:
            prng = np.random.RandomState()
            index = prng.permutation(len(lines) - 1)
            idx = np.asarray(index, dtype=np.integer)[0]
            random_ua = lines[int(idx)].replace("\n", "")
    except Exception as ex:
        print('[!] Exception in random_ua')
        print(str(ex))
    finally:
        return random_ua

def get_random_headers():
    user_agent = get_random_ua()
    referer = get_random_referer()
    headers = {
        'user-agent': user_agent,
        'referer': referer,
    }
    return headers

def get_proxies_from_website():
    url = 'https://free-proxy-list.net'
    headers_base = get_random_headers()
    html = requests.get(url, headers=headers_base)
    soup = BeautifulSoup(html.text, 'lxml')
    available_proxies = []
    for tr in soup.find_all('tr')[2:]:
        tds = tr.find_all('td')
        if len(tds) > 0:
            dico_proxy = {}
            adress = tds[0].text
            port = tds[1].text
            s = ':'
            proxy = s.join((adress, port))
            dico_proxy['http'] = proxy
            dico_proxy['https'] = proxy
            available_proxies.append(dico_proxy)
    return available_proxies[:-11]

def test_proxy(proxy):
    url = 'https://httpbin.org/ip'
    try:
        response = requests.get(url,proxies=proxy, timeout = 3)
    except:
        return False
    if response.json()['origin'] == proxy['http'].split(':')[0]:
        return True
    else:
        return False

def get_random_proxy():
    available_proxies = get_proxies_from_website()
    proxy = random.choice(available_proxies)
    if test_proxy(proxy):
        return proxy
    else:
        get_random_proxy()

# Load the data
imdata=pd.read_csv('meilleuragentcsv.csv', encoding='utf8', engine='python')

# Main
n = len(imdata)
errors = []
print("[+] Finding 1st proxy...")
proxy = get_random_proxy()
while proxy == None:
    proxy = get_random_proxy()
print("[+] Done ! Starting...")
number_of_use_of_proxy = 0
number_rows = 10
for i in range(number_rows):

    time.sleep(0.8)
    print("[+] Row:", i)
    headers = get_random_headers()
    print("[+] Proxy:", proxy)
    try:
        url = 'http://www.meilleursagents.com/prix-immobilier/'+ imdata.iloc[i,5]
    except:
        print("[!] Error: url table entry is empty.")
        continue


    # Create the BeautifulSoup object
    tries = 10
    for j in range(tries):
        try:
            html = requests.get(url, headers=headers, proxies=proxy)
        except:
            print("[!] Error: the proxy died. Finding another proxy...")
            proxy = get_random_proxy()
            print("[+] Found a new proxy.")
            continue
        soup = BeautifulSoup(html.text, 'lxml')
        if len(soup.find_all("div", {'class':"alert alert-danger"})) > 0:
            print("[!] Maximum number of connexions exceeded. Switching proxies...")
            proxy = get_random_proxy()
            print("[+] New proxy found !")
            continue
        break
    else:
        print("[!] All connexions attempts failed.")
        errors.append(i)
        continue

    if len(soup.select("html.error-html")) > 0: # The url adress is wrong
        errors.append(i)
        print("[!] Error: wrong url adress")
        continue

    # Make sure we are getting the data for the right city
    try:
        city = soup.select("h1.prices-summary__title ")[0].text.split(" ")[3]
        print("[+] Target:", imdata.iloc[(i, 1)], ", Retrieved:", city)
    except:
        print("[!] Error while retrieving the target.")
        continue

    # Parse the html file
    avg = soup.select("div.prices-source-graph__average-value")
    stats = soup.find_all("div", {'class':"prices-source-graph__number-value"})
    # Retrive the value we need
    try:
        if test_house_and_flat(soup) == 'Both':
            # Appartements
            average_appartements = avg[0].text.strip().split("€")[0].strip()
            mini_appartements = stats[0].text.strip().split("€")[0].strip()
            maxi_appartement = stats[1].text.strip().split("€")[0].strip()

            # Houses
            average_houses = avg[1].text.strip().split("€")[0].strip()
            mini_houses = stats[2].text.strip().split("€")[0].strip()
            maxi_houses = stats[3].text.strip().split("€")[0].strip()

            # Put in the dataframe
            imdata.iloc[(i, 7)] = mini_houses
            imdata.iloc[(i, 8)] = average_houses
            imdata.iloc[(i, 9)] = maxi_houses
            imdata.iloc[(i, 11)] = mini_appartements
            imdata.iloc[(i, 12)] = average_appartements
            imdata.iloc[(i, 13)] = maxi_appartement

        elif test_house_and_flat(soup) == 'House':
            average = avg[0].text.strip().split("€")[0].strip()
            mini = stats[0].text.strip().split("€")[0].strip()
            maxi = stats[1].text.strip().split("€")[0].strip()

            # Put in the dataframe
            imdata.iloc[(i, 7)] = mini
            imdata.iloc[(i, 8)] = average
            imdata.iloc[(i, 9)] = maxi
        elif test_house_and_flat(soup) == 'Flat':
            average = avg[0].text.strip().split("€")[0].strip()
            mini = stats[0].text.strip().split("€")[0].strip()
            maxi = stats[1].text.strip().split("€")[0].strip()

            # Put in the dataframe
            imdata.iloc[(i, 11)] = mini
            imdata.iloc[(i, 12)] = average
            imdata.iloc[(i, 13)] = maxi

    except Exception as e:
        print("[!] Error while parsing the file.")
        print(e)
        errors.append(i)
        continue
    # Save the periodically the datafram

    if i % 100 == 0:
        print("[+] Saving the dataframe to csv...")
        imdata.to_csv('meilleuragentcsv.csv', index=False)
        print("[+] Done !")
print("[+] Writing to xlsx...")
writer = pd.ExcelWriter('meilleuragent.xlsx')
imdata.to_excel(writer,'Sheet1')
writer.save()
print("[+] Done ! Exiting.")
