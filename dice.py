from pymongo import MongoClient, ASCENDING
import pymongo, random, requests, math, re
from requests.auth import HTTPProxyAuth
from bs4 import BeautifulSoup

import datetime
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()


########## START configuration ##########
useProxy = False
input_proxy_file = "proxy.txt"
proxy_username = "US219524"
proxy_password = "EhYksL7ttc"
input_keyword_file = "input.txt"
########### END configuration ###########


########## START database configuration ##########
try:
    client = MongoClient("localhost", 27017)
    db = client["test"]
    collection = db["dice_jobs"]
    collection.create_index([("job_unique_id", ASCENDING)], unique=True)
except:
    print "database setup error: please check your database service"
    exit()
########### END database configuration ###########


def randProxy(input_proxy_file):
    """ return a random proxy """
    with open(input_proxy_file) as f:
        proxyIP = random.choice(list(f)).strip()
    return proxyIP


def getPageProxy(index_url, useProxy, input_proxy_file, proxy_username, proxy_password):
    """ return a requests w/ or w/o proxy """
    if useProxy:
        proxyIP = randProxy(input_proxy_file)
        proxyDict = {
            "http": proxyIP,
            "https": proxyIP
        }
        auth = HTTPProxyAuth(proxy_username, proxy_password)
        str1 = requests.get(index_url, proxies=proxyDict, auth=auth)
    if not useProxy:
        str1 = requests.get(index_url)
    return str1


def insert_document(keyword, job_title, job_url, company, post_date, job_unique_id, job_description):
    """ return true if insertion is successful
        return false if there is a duplicate """
    try:
        collection.insert_one({
            "keyword": keyword,
            "job_title": job_title,
            "job_url": job_url,
            "company": company,
            "post_date": post_date,
            "job_unique_id": job_unique_id,
            "job_description": job_description
        })
        return True
    except pymongo.errors.DuplicateKeyError:
        return False


def convertStrDate(dateStr):
    date = datetime.datetime.now() - datetime.timedelta(0)
    date = date.strftime("%m/%d/%Y")
    try:
        if (re.search("Just|hour|minute|second|Today", dateStr)):
            try:
                date = datetime.datetime.now() - datetime.timedelta(0)
                date = date.strftime("%m/%d/%Y")
            except ValueError:
               ""
        else:
            try:
                dateStr2 = re.sub("\D", "", dateStr)
                dateStr2 = int(dateStr2)
                date = datetime.datetime.now() - datetime.timedelta(dateStr2)
                date = date.strftime("%m/%d/%Y")
            except ValueError:
                ""
    except ValueError:
        ""
    return date



########## START SCRAPING ##########

keyword_dict = {}

f = open(input_keyword_file)

for keywords in f.readlines():
    keyword = keywords.strip()
    print "keyword = ", keyword
    keyword_dict[keyword] = (0,0)

    dup_count = 0
    total_found = 0
    fetched_count = 0

    if collection.count({"keyword": keyword}) == 0:
        print "========================================"
        print "get all data for: ", keyword
        print "========================================"

        index_url = "http://service.dice.com/api/rest/jobsearch/v1/simple.json?text="+keyword
        try:
            str1 = getPageProxy(index_url, useProxy, input_proxy_file, proxy_username, proxy_password)
            count_result = str1.json()["count"]
            count_page = int(math.ceil(count_result/50.0))

            total_found = count_result
        except:
            print "connection error, please check internet connection or check server status"
            exit()

        for i in range(count_page):
            index_url_nav = index_url+"&page="+str(i+1)
            str1 = getPageProxy(index_url_nav, useProxy, input_proxy_file, proxy_username, proxy_password)
            resultItemList = str1.json()["resultItemList"]
            for item in resultItemList:
                job_title = item["jobTitle"]
                job_url = item["detailUrl"]
                company = item["company"]
                post_date = item["date"]

                try:
                    detail_url = getPageProxy(job_url, useProxy, input_proxy_file, proxy_username, proxy_password)
                    detail_html = BeautifulSoup(detail_url.content, "html5lib")
                    job_unique_id = detail_html.select("meta[name=jobId]")[0]["content"]
                    job_description = detail_html.select("div[id=jobdescSec]")[0].getText(separator=u" "").encode("ascii", "ignore").decode("ascii")

                    keyword = re.sub("\'", "\\\'", keyword).strip()
                    insert_document(keyword, job_title, job_url, company, post_date, job_unique_id, job_description)
                    fetched_count = fetched_count + 1
                except:
                    ""
    else:
        index_url = "http://service.dice.com/api/rest/jobsearch/v1/simple.json?text="+keyword+"&age=1&sort=1"

        try:
            str1 = getPageProxy(index_url, useProxy, input_proxy_file, proxy_username, proxy_password)
            count_result = str1.json()["count"]
            count_page = int(math.ceil(count_result/50.0))

            total_found = count_result
        except:
            print "connection error, please check internet connection or server status"
            exit()

        for i in range(count_page):
            index_url_nav = index_url+"&page="+str(i+1)
            str1 = getPageProxy(index_url, useProxy, input_proxy_file, proxy_username, proxy_password)
            resultItemList = str1.json()["resultItemList"]
            for item in resultItemList:
                job_title = item["jobTitle"]
                job_url = item["detailUrl"]
                company = item["company"]
                post_date = item["date"]

                try:
                    detail_url = getPageProxy(job_url, useProxy, input_proxy_file, proxy_username, proxy_password)
                    detail_html = BeautifulSoup(detail_url.content, "html5lib")
                    job_unique_id = detail_html.select("meta[name=jobId]")[0]["content"]
                    job_description = detail_html.select("div[id=jobdescSec]")[0].getText(separator=u" ").encode("ascii", "ignore").decode("ascii")

                    keyword = re.sub("\'", "\\\'", keyword).strip()
                    insert = insert_document(keyword, job_title, job_url, company, post_date, job_unique_id, job_description)
                    if insert:
                        fetched_count = fetched_count + 1
                    else:
                        dup_count = dup_count + 1
                        if dup_count > 5:
                            break_for = True
                            print "########## found duplicate ##########"
                            break
                except:
                    ""
            if break_for:
                print "########## found duplicate ##########"
                break
    keyword_dict[keyword] = (total_found, fetched_count)
