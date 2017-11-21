from pymongo import MongoClient, ASCENDING
import pymongo, random, requests, math, re
from requests.auth import HTTPProxyAuth
from bs4 import BeautifulSoup

import datetime, urllib
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
    collection = db["indeed_jobs"]
    collection.create_index([("job_unique_id", ASCENDING)], unique=True)
except:
    print "database setup error: please check your database service"
    exit()
########### END database configuration ###########

def randProxy(input_proxy_file):
    with open(input_proxy_file) as f:
        proxyIP = random.choice(list(f)).strip()
    return proxyIP

def getPageProxy(index_url, useProxy, input_proxy_file, proxy_username, proxy_password):
    if useProxy:
        proxyIP = randProxy(input_proxy_file)
        print "proxyIP = ", proxyIP
        proxyDict = {
            "http": proxyIP,
            "https": proxyIP
        }
        auth = HTTPProxyAuth(proxy_username,  proxy_password)
        str1 = requests.get(index_url, proxies=proxyDict, auth=auth)

    if not useProxy:
        str1 = requests.get(index_url)
    return str1


def insert_document(keyword, job_title, job_url, company, post_date, job_unique_id, job_description):
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
        if (re.search('Just|hour|minute|second|Today', dateStr)):
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

    keyword = ''
    keyword = keywords.strip()
    print 'keyword = ', keyword
    # tuple (found, fetched)
    keyword_dict[keyword] = (0,0)


    dup_count = 0
    total_found = 0
    fetched_count = 0
    if collection.count({"keyword": keyword}) == 0:
        print "========================================"
        print 'get all data for:', keyword
        print "========================================"

        index_url = 'http://www.indeed.com/jobs?q='+urllib.quote_plus(keyword)+'&limit=50'
        #print 'index_url = ', index_url

        str1 = getPageProxy(index_url, useProxy, input_proxy_file, proxy_username, proxy_password)
        html1 = BeautifulSoup(str1.content, "html5lib")

        count_result = 0
        count_page = 0
        rest_count_page = 0
        class1 = 'div[id=searchCount]'
        # get total count for all result
        for d1 in html1.select(class1):
            try:
                count_result = re.sub('.* of', '', d1.text).strip()
                count_result = re.sub('[^\d+]', '', count_result).strip()

                total_found = count_result

                count_page = math.ceil(int(count_result)/50)
                rest_count_page = int(count_result)%50
                if(rest_count_page > 0):
                    count_page = count_page + 1

                if(count_page>20):
                    count_page = 20

            except ValueError:
                ""

        print 'count_result = ', count_result
        # print 'count_page = ', count_page
        # print "========================================"

        index_url_nav = ''
        # iterate pages (50 results per page)
        for i in range(0, int(count_page)):
            index_url_nav = index_url+'&start='+str(i*50)

            # print 'page = ', (i + 1)
            # print 'index_url_nav = ', index_url_nav
            # print "========================================"

            str1 = getPageProxy(index_url_nav, useProxy, input_proxy_file, proxy_username, proxy_password)
            html1 = BeautifulSoup(str1.content, "html5lib")

            job_title = ''
            job_url = ''
            company = ''
            post_date = ''
            job_unique_id = ''
            job_description = ''

            job_tk_id = ''
            class1 = 'input[name=tk]'
            for d1 in html1.select(class1):
                try:
                    job_tk_id = d1.get('value').strip().encode('ascii', 'ignore').decode('ascii')
                    job_tk_id = re.sub('\'', '\\\'', job_tk_id).strip()
                except ValueError:
                    ""

            class1 = 'td[id=resultsCol] div[id*=p_]'
            # iterate 50 results in one page
            for d1 in html1.select(class1):
                try:

                    try:
                        job_unique_id = re.sub('\s+', ' ', d1.get('data-jk')).strip().encode('ascii', 'ignore').decode(
                            'ascii')
                        job_unique_id = re.sub('\'', '\\\'', job_unique_id).strip()
                    except ValueError:
                        ""

                    class2 = 'h2[class=jobtitle] a'
                    for d2 in d1.select(class2):
                        try:
                            job_title = d2.text.strip().encode('ascii', 'ignore').decode('ascii')
                            job_title = re.sub('\'', '\\\'', job_title).strip()
                        except ValueError:
                            ""

                    class2 = 'span[class=company] span[itemprop=name]'
                    for d2 in d1.select(class2):
                        try:
                            company = re.sub('\s+', ' ', d2.text).strip().encode('ascii', 'ignore').decode('ascii')
                            company = re.sub('\'', '\\\'', company).strip()
                        except ValueError:
                            ""

                    class2 = 'span[class=date]'
                    for d2 in d1.select(class2):
                        try:
                            post_date = d2.text.strip().encode('ascii', 'ignore').decode('ascii')
                            post_date = re.sub('\'', '\\\'', post_date).strip()
                            post_date = convertStrDate(post_date)
                        except ValueError:
                            ""

                    try:
                        job_url = "http://www.indeed.com/viewjob?jk="+job_unique_id+"&q="+keyword+"&tk="+job_tk_id+"&from=web"
                    except ValueError:
                      ""

                    str3 = getPageProxy(job_url, useProxy, input_proxy_file, proxy_username, proxy_password)
                    html3 = BeautifulSoup(str3.content, "html5lib")

                    job_description = ''
                    class3 = 'span[id=job_summary]'
                    for d3 in html3.select(class3):
                        try:
                            job_description = d3.getText(separator=u' ').encode('ascii', 'ignore').decode('ascii')
                            job_description = re.sub('\'', '\\\'', job_description).strip()
                        except ValueError:
                            ""

                    # print 'job_title = ', job_title
                    # print 'job_url = ', job_url
                    # print 'company = ', company
                    # print 'post_date = ', post_date
                    # print 'job_unique_id = ', job_unique_id
                    # print 'Job_description = ', Job_description
                    # print "========================================"

                    keyword = re.sub('\'', '\\\'', keyword).strip()
                    insert_document(keyword, job_title, job_url, company, post_date, job_unique_id, job_description)
                    fetched_count = fetched_count + 1

                except ValueError:
                    ""

    else:

        # print "========================================"
        # print 'get filtered data'
        # print "========================================"

        index_url = 'http://www.indeed.com/jobs?as_and='+urllib.quote_plus(keyword)+'&as_phr=&as_any=&as_not=&as_ttl=&as_cmp=&jt=all&st=&salary=&radius=25&l=&fromage=1&limit=50&sort=date&psf=advsrch'
        # print 'index_url = ', index_url

        str1 = getPageProxy(index_url, useProxy, input_proxy_file, proxy_username, proxy_password)
        html1 = BeautifulSoup(str1.content, "html5lib")

        count_result = 0
        count_page = 0
        rest_count_page = 0
        class1 = 'div[id=searchCount]'
        for d1 in html1.select(class1):
            try:
                count_result = re.sub('.* of', '', d1.text).strip()
                count_result = re.sub('[^\d+]', '', count_result).strip()

                total_found = count_result

                count_page = math.ceil(int(count_result)/50)
                rest_count_page = int(count_result)%50
                if(rest_count_page > 0):
                    count_page = count_page + 1

                if(count_page>20):
                    count_page = 20

            except ValueError:
                ""

        print 'count_result = ', count_result
        print 'count_page = ', count_page
        # print "========================================"

        index_url_nav = ''
        for i in range(0, int(count_page)):
            index_url_nav = index_url+'&start='+str(i*50)

            # print 'page = ', (i + 1)
            # print 'index_url_nav = ', index_url_nav
            # print "========================================"

            str1 = getPageProxy(index_url_nav, useProxy, input_proxy_file, proxy_username, proxy_password)
            html1 = BeautifulSoup(str1.content, "html5lib")

            job_title = ''
            job_url = ''
            company = ''
            post_date = ''
            job_unique_id = ''
            job_description = ''

            job_tk_id = ''
            class1 = 'input[name=tk]'
            for d1 in html1.select(class1):
                try:
                    job_tk_id = d1.get('value').strip().encode('ascii', 'ignore').decode('ascii')
                    job_tk_id = re.sub('\'', '\\\'', job_tk_id).strip()
                except ValueError:
                    ""

            break_for = "false"

            class1 = 'td[id=resultsCol] div[id*=p_]'
            for d1 in html1.select(class1):
                try:

                    try:
                        job_unique_id = re.sub('\s+', ' ', d1.get('data-jk')).strip().encode('ascii', 'ignore').decode(
                            'ascii')
                        job_unique_id = re.sub('\'', '\\\'', job_unique_id).strip()
                    except ValueError:
                        ""

                    class2 = 'h2[class=jobtitle] a'
                    for d2 in d1.select(class2):
                        try:
                            job_title = d2.text.strip().encode('ascii', 'ignore').decode('ascii')
                            job_title = re.sub('\'', '\\\'', job_title).strip()
                        except ValueError:
                            ""

                    class2 = 'span[class=company] span[itemprop=name]'
                    for d2 in d1.select(class2):
                        try:
                            company = re.sub('\s+', ' ', d2.text).strip().encode('ascii', 'ignore').decode('ascii')
                            company = re.sub('\'', '\\\'', company).strip()
                        except ValueError:
                            ""

                    class2 = 'span[class=date]'
                    for d2 in d1.select(class2):
                        try:
                            post_date = d2.text.strip().encode('ascii', 'ignore').decode('ascii')
                            post_date = re.sub('\'', '\\\'', post_date).strip()
                            post_date = convertStrDate(post_date)
                        except ValueError:
                            ""

                    try:
                        job_url = "http://www.indeed.com/viewjob?jk="+job_unique_id+"&q="+keyword+"&tk="+job_tk_id+"&from=web"
                    except ValueError:
                      ""



                    str3 = getPageProxy(job_url, useProxy, input_proxy_file, proxy_username, proxy_password)
                    html3 = BeautifulSoup(str3.content, "html5lib")

                    job_description = ''
                    class3 = 'span[id=job_summary]'
                    for d3 in html3.select(class3):
                        try:
                            job_description = d3.text.strip().encode('ascii', 'ignore').decode('ascii')
                            job_description = re.sub('\'', '\\\'', job_description).strip()
                        except ValueError:
                            ""

                    # print 'job_title = ', job_title
                    # print 'job_url = ', job_url
                    # print 'company = ', company
                    # print 'post_date = ', post_date
                    # print 'job_unique_id = ', job_unique_id
                    # print 'Job_description = ', Job_description
                    # print "========================================"

                    keyword = re.sub('\'', '\\\'', keyword).strip()
                    dup = insert_document(keyword, job_title, job_url, company, post_date, job_unique_id, job_description)
                    if not dup:
                        fetched_count = fetched_count + 1
                    else:
                        dup_count = dup_count + 1
                        if dup_count > 5:
                            break_for = "true"
                            print '################found duplicate###################'
                            break

                except ValueError:
                    ""

            if(break_for == "true"):
                print '################found duplicate###################'
                break

    keyword_dict[keyword] = (total_found, fetched_count)
