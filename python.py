# -*- coding: utf-8 -*-
import numpy as np
import requests
import lxml
from bs4 import BeautifulSoup as bs
import re
import pymongo
import time

client = pymongo.MongoClient('192.168.220.130', 27017)
db = client.MovieDB # 或者 db = client['Test'] ...

def getMovie(in_url):
    mongo_collection2 = db.Movie_list
    html = requests.get(in_url)
    Soup = bs(html.content, features="lxml")
    soup_title = Soup.select('h1[class]')
    soup_revenue = Soup.find_all('span',attrs={"itemprop":"ratingValue"})
    soup_info = Soup.select('div[class="subtext"]')
    soup_post = Soup.select('div[class="poster"]')
    info = soup_info[0].get_text(strip=True).split("|")

    title = soup_title[0].get_text(strip=True)
    revenue = float(soup_revenue[0].get_text(strip=True))
    length = info[1]
    genre = info[2]
    release_date_country = info[3]
    poster = soup_post[0].find('img').get('src')

    data_test2 ={
        'title': title,
        'revenue': revenue,
        'genre': genre,
        'release & country': release_date_country,
        'poster': poster
    }
    result = mongo_collection2.insert(data_test2)
    print(result)
    return(result)


def getCastFromMovie(in_url,movie):
    pre_Html = "https://www.imdb.com"
    html = requests.get(in_url)
    Soup = bs(html.content, features="lxml")
    mongo_collection1 = db.Cast_list

    #get cast-list
    href_re2 = re.compile(pattern="/name/nm[0-9]{7}")
    soup2 = Soup.find_all('a', attrs={"href":href_re2})

    #get director 
    href_re = re.compile(pattern="simpleTable simpleCreditsTable")
    soup = Soup.find('table', attrs={"class":href_re})
    soup1 = soup.find_all('a', attrs={"href":href_re2})
    director =  [var.get_text().strip() for var in soup1]
    name = [var.get_text().strip() for var in soup2]

    #get information page URL
    URL = ["1"]
    for all_url in Soup.find_all('a', attrs={"href":href_re2},href=True): 
        if(all_url['href'][0:5] == '/name'):
            if URL[-1] != all_url['href']:
                URL.append(all_url['href'])
    URL.remove("1")
    print(director)

    # get cast info
    # 还需添加防止人员重复添加
    i=0
    for new_URL in URL:
        new_html = pre_Html + new_URL
        stuff_html = requests.get(new_html)
        print(new_html)
        new_Soup = bs(stuff_html.content, features="lxml")
        soup_name = new_Soup.select('h1[class="header"]')
        soup_born = new_Soup.find('time')
        soup_poster = new_Soup.select('img[id="name-poster"]',src=True)
        name = soup_name[0].get_text(strip=True).replace("(I)","")

        if (soup_born != None):
            date = soup_born.get_text(strip=True)
        else:
            date = 'Unknown'
        if (soup_poster != []):
            foto = soup_poster[0].get('src')
        else:
            foto = 'None'

#包裹一层数据库重复查询
        movie_list = []
        movie_list.append(movie)
        myquery = { "name": name }
        sample = mongo_collection1.find(myquery)
        m=0
        for x in sample:
            m=x
        if (m == 0):

            if (director[0] == name):
                data_test1 ={
                    'name': name,
                    'status': 'Director',
                    'born': date,
                    'foto': foto,
                    'participate': movie_list
                }
            else:
                data_test1 ={
                    'name': name,
                    'status': 'actor&stuff',
                    'born': date,
                    'foto': foto,
                    'participate': movie_list
                }
            i = i+1
            time.sleep(0.1)
            print(i)
            result = mongo_collection1.insert(data_test1)
        else:
            m['participate'].append(movie)          
            newvalues = { "$set": { "participate": m['participate'] } }
            mongo_collection1.update(myquery,newvalues)



if __name__ == "__main__":
    movie_url = "https://www.imdb.com/title/tt1375666/?ref_=nm_knf_i4"
    Cast_url = movie_url[:37]+"fullcredits?ref_=tt_cl_sm#cast"
    movie_id = getMovie(movie_url)
    getCastFromMovie(Cast_url,movie_id)


