# -*- coding: utf-8 -*-
"""
Created on Wed Oct 07 14:08:03 2015

@author:    Mark Berry
            
            markberry2010 at gmail.com
"""

from datetime import *
from cStringIO import StringIO
import sys
import hashlib
import requests


from bs4 import BeautifulSoup as bs
import pandas as pd
from pandas.tseries.offsets import MonthBegin

class WikiDownloader():
    """Class to Download Wikipedia pageveiw data
    """
    def __init__(self, start, end, names=None,
                 hit_threshold=None, projects=None):
                     
        frb = True
        if frb:
            self.proxies = {'http':"http://webproxy.frb.gov:8080",
               'https':"http://webproxy.frb.gov:8080"}
            self.headers = {'User-agent' : 'Lynx'}
        else:
            self.proxies = None
            self.headers = None
            
        self.links = self.find_links_from_range(start, end)
        
        #Setup filters
        self.names = names
        self.hit_threshold = hit_threshold
        self.projects = projects

        
    
    def find_links_from_range(self, start, end):
        """
        
        """
        daterange = pd.date_range(start - MonthBegin(),end, freq ='MS')
        links = []
        for month in daterange:
            links += self.find_month_links(month)
       
        #filter links by start and end time
        links = [l for l in links if l[1] >= start and l[1]<=end]
        return links
        
        
    def link_to_hour(self, link):
        """
        Helper function that gets datetime object from pageview url
        """
        link_fmt = 'pagecounts-%Y%m%d-%H%M%S.gz'
        hour = datetime.strptime(link, link_fmt).replace(minute=0, second=0)
        return hour
    
    def find_month_links(self, month):
        """
        Finds wikipedia links from a given month
        Arguments: Month (datetime)
        Returns: List of tuples with linktext
        """
        month_fmt = 'http://dumps.wikimedia.org/other/pagecounts-raw/%Y/%Y-%m/'
        url = month.strftime(month_fmt)
        
        #Download links page 
        response = requests.get(url,
                                proxies = self.proxies,
                                headers = self.headers).content
        soup = bs(response)
        links = soup('a')
        pagecount_links = [(url + l['href'], self.link_to_hour(l['href']))
                            for l in links
                            if 'pagecounts' in l.text]
        return pagecount_links
    

     

    
    def checkhash(self, response, hour, link):
        """
        Check that the md5 hash code of our downloaded file matches that of 
        wikipedia's provided hash code    
        """
        #Download table of hash codes from Wiki website
        baseurl = 'http://dumps.wikimedia.org/other/pagecounts-raw/'
        file_fmt = '%Y/%Y-%m/md5sums.txt'
        hashlink = baseurl + hour.strftime(file_fmt)    
        hash_text = StringIO(urlopen(hashlink))
    
        #Create Lookup table of hash values    
        lookup_table = pd.read_csv(hash_text,
                                sep='  ',
                                index_col=1,
                                header=None,
                                names=['hash','file'])
        filename = link.split('/')[-1]
        
        #Find the expected hash value
        try:
            expected_hash = lookup_table.ix[filename,'hash']
        except:
            return False
        
        #Use md5 hash fxn to make hash
        downloaded_hash = hashlib.md5(response).hexdigest()
        
        return expected_hash == downloaded_hash
               
    def download_link(self, link, hour):
        
        #Loops 5 time to try
        for i in range(5):
            response = requests.get(link,
                                    proxies = self.proxies,
                                    headers = self.headers).content
                                
            if checkhash(response, hour, link):
                break
            else:
                print ("Error downloading file. Try %d" % (i + 1))
        df = reponse_to_df(response)
        return df
    
    def response_to_df(self,reponse):
        try:
            frame = pd.read_csv(StringIO(response),
                                compression='gzip',
                                sep ='\s*',
                                names = ['project','name','hits','size'],
                                )
            frame.index = [hour] * len(frame)
        except Exception as e:
            print (e)
            print (link)
            frame = pd.DataFrame(columns = ['project','name','hits','size'])
        return frame
    
    
    def filter_df(self, df):
        """
        Filter Results DF based on name, hits and projects
        """
        
        assert isinstance(names, list) or isinstance(names, type(None)), "Names Must Be List"
        assert isinstance(projects, list) or isinstance(projects, type(None)), "Projects Must Be List"
        
        if self.names:
            df = df.ix[df.name.isin(self.names)]
        if self.hit_threshold:
            df = df.ix[df.hits > self.hit_threshold]
        if self.projects:
            df = df.ix[df.project.isin(self.projects)]
        df['hits'] = df['hits'].astype(int)
        df['size'] = df['size'].astype(int)
        return df
        
    
    def download_data(self, filename):
        """
        Create HDF5 table of wikipedia hit counts based 
        """
        store = pd.HDFStore(filename, 'a')
        try:
            for link, hour in self.links:
                    
                df = download_link(link, hour)
                df = filter_df(df, self.filters)
                try:                    
                    store.append('df',
                             df,
                             min_itemsize={'project':12})
                except Exception as e:
                    print (e)
                    print ("Error appending %s" % hour) 
                    continue
        finally:
            store.close()
    
    def progress_bar(self, link):
        return
    
