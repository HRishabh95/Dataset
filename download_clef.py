import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import argparse
import time
import json
import gzip
import pickle
import os
#import boto3
from io import StringIO
from bs4 import BeautifulSoup
from itertools import islice, chain
from multiprocessing import Process
from multiprocessing import Pool
import sys
import xml.etree.ElementTree as ET
import numpy as np
domains = np.loadtxt('/home/rupadhyay/Cred2vec_trec/clef_domain.lst',delimiter='\t',dtype=object)[:,1]
output_folder = '/data/ricky/dataset/CLEF2020/'
parallel_threads = 1

### -----------------------
### Does HTTP session management to handle retries and problems.
### see https://www.peterbe.com/plog/best-practice-with-retries-with-requests
###
def requests_retry_session(retries=8, backoff_factor=0.3, status_forcelist=(500, 502, 504), session=None):
    session = session or requests.Session()
    retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    return session

### -----------------------
### Searches the Common Crawl Index for a domain.
### -----------------------
def search_domain(domain):
    record_list = []
    #print "[*] Target domain: %s" % domain
    index = "2018-09"

    #print "[*] Trying index %s" % index
    cc_url  = "http://index.commoncrawl.org/CC-MAIN-%s-index?" % index
    cc_url += "url=%s&matchType=domain&output=json" % domain.strip() # this allows the crawl of the whole domain from which the URL comes from
    #cc_url += "url=%s&matchType=exact&output=json" % domain #this allows *exact* match of URL
    try:
        response = requests_retry_session().get(cc_url)
    except Exception as x:
        print('It failed :(', x.__class__.__name__)
    else:
        if response.status_code == 200:
            records = response.content.splitlines()
            for record in records:
                record_list.append(json.loads(record))

    print("[*] Target domain: {} with a total of {} hits.".format(domain.strip(), len(record_list)))
    return record_list


def get_url(warc):
    thiswarc = warc
    matched_lines = [line for line in thiswarc.split('\n') if "WARC-Target-URI: " in line]
    url = matched_lines[0].replace('WARC-Target-URI: ','')
    url = url.replace("\r", "")
    #print url
    return url

def get_uri(warc):
    matched_lines = [line for line in warc.split('\n') if "WARC-Record-ID: <urn:uuid:" in line]
    uri = matched_lines[0].replace('WARC-Record-ID: <urn:uuid:','')
    uri = uri.replace('>','')
    uri = uri.replace("\r", "")
    return uri

def get_name(url):
    name = os.path.basename(url)
    if len(name) < 1:
        name = url
    return name

def batch(iterable, size):
    sourceiter = iter(iterable)
    while True:
        batchiter = islice(sourceiter, size)
        yield chain([batchiter.next()], batchiter)

#
# Downloads full page
#
def download_page(record, directory):
    #print "Downloading " + record['filename']
    offset, length = int(record['offset']), int(record['length'])
    offset_end = offset + length - 1

    # We'll get the file via HTTPS so we don't need to worry about S3 credentials
    # Getting the file on S3 is equivalent however - you can request a Range
    prefix = 'https://data.commoncrawl.org/'
    url=""
    # We can then use the Range header to ask for just this set of bytes
    try:
        resp = requests_retry_session().get(prefix + record['filename'], headers={'Range': 'bytes={}-{}'.format(offset, offset_end)})
    except Exception as x:
        print('It failed :(', x.__class__.__name__)
    else:
        # The page is stored compressed (gzip) to save space
        # We can extract it using the GZIP library
        import io
        raw_data = io.BytesIO(resp.content)
        try:
            #data=gzip.decompress(raw_data).decode('utf-8')
            f = gzip.GzipFile(fileobj=raw_data)
            data = f.read().decode('utf-8')
        except:     # except OSError because IOError was merged to OSError in Python 3.3
            print("Exception for directory: %s" % directory.strip())
            data = ""

        response = ""
        if len(data):
            try:
                #print data
                try:
                    warc,header,response = data.strip().split('\r\n\r\n', 2)
                except:
                    warc, header = data.strip().split('\r\n\r\n', 2)


                #response_code = header.
                http_res_line = header.strip().split('\n')[0]
                http_res_code_array = http_res_line.split(' ')
                http_res_code = http_res_code_array[0] + ' ' + http_res_code_array[2]
                url = get_url(warc)
                #print url
                name = get_name(url)
                if name.lower().endswith(('.pdf','.png', '.jpg', '.jpeg', '.mp3', '.avi', '.zip', '.tar', '.gz')): #or name == 'robots.txt' or len(response)==0:
                    print(url + '\tnull\tfie not allowed')
                elif len(response)==0:
                    print(url + '\tnull' + '\t' + http_res_code)
                elif name == 'robots.txt':
                    print(url + '\tnull' + '\trobots')
                else:
                    uri = get_uri(warc)
                    #print uri
                    filepath = directory + '/' + uri
                    file = open(filepath, 'w')
                    file.write(response)
                    file.close()
                    print(url + '\t' + uri + '\t' + http_res_code)
                #if name.lower().endswith('.pdf','.png', '.jpg', '.jpeg'):
                #    print("Skipping " + name)
                #else:
                #    uri = get_uri(warc)
                #    filepath = output_folder + '/' + uri
                #    file = open(filepath, 'w')
                #    file.write(response)
                #    file.close()

            except Exception as e:
                print(e)
                sys.exit(1)

    return url

def process_domain(domain):
    directory = output_folder + '/' + domain.strip()
    if not os.path.exists(directory):
        os.makedirs(directory)
    record_list = search_domain(domain)
    for record in record_list:
        url = download_page(record, directory)

    return


def runInParallel(batchiter, fn):
    proc = []
    for item in batchiter:
        p = Process(target=fn(item))
        p.start()
        proc.append(p)
        for p in proc:
            p.join()

### -----------------------
###     Main Function
### -----------------------
def main():
    #print("Starting CommonCrawl Search")
    #Finds all relevant domains

    if parallel_threads==0:
        #the input is a domain, not a file with a list of domains
        process_domain(domains[6])
    else:
        #domain is a pointer to a file containing the list of domains to source, one per line
        pool = Pool(processes=int(parallel_threads))
        pool.map(process_domain, domains)

