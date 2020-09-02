# importing the async libraries
import asyncio
import aiohttp
import aiofiles
from aiohttp import ClientSession

import urllib.parse

# importing web parser
from bs4 import BeautifulSoup as bs

# importing other libraries
import pathlib
import sys
import json

# importing loggers

import logging
from typing import IO

# importing time module
import time

# setting the logging files

# create a logger with name with filename
logger = logging.getLogger(__name__)

# create a handler
f_start = 'searches/'+time.strftime('%Y_%m_%d')
pwd = pathlib.Path(__file__).parent
log_file = pwd.joinpath(f_start+".log")
# print(f"filename {log_file}")

f_handler = logging.FileHandler(filename=log_file)
f_handler.setLevel(logging.ERROR)

# create a formatter
f_fomatter = logging.Formatter('\n\n%(asctime)s : %(name)s : %(levelname)s : %(message)s\n')
f_handler.setFormatter(f_fomatter)

# adding the handler to the logger
logger.addHandler(f_handler)


# class for scraping the data from indeed website
class Indeed_scrape:

    # defining the constructor
    def __init__(self, key: str, loc: str):

        self.key = key
        self.loc = loc
        self.f_name = f_start+"_"+key.replace(" ","_").replace(",", "_")+"_"+loc.replace(" ","_").replace(",", "_")
        self.url = "https://www.indeed.co.in/jobs?"
        self.url_list = []
        self.pages_found = 0
        self.jobs_found = 0
        
    # function for preaparing the url
    def prepare_url(self) -> None:

        '''
        Params :
            None

        Return :
            None

        Does :
            Loop throught all the keywords and locations,
            and forms all the possible combinations
        '''

        key = ((self.key.replace(" ", "+")).replace(",", "%2C")).replace("/", "%2F")
        loc = ((self.loc.replace(" ", "+")).replace(",", "%2C")).replace("/", "%2F")

        self.url_list.append(self.url+'q='+key+'&l='+loc)

        print(f"\n\n Indeed url-List :  \n\n{self.url_list}\n\n")

    # function for executing url and fetching the data
    async def execute_url(self, url: str, session: ClientSession) -> str:

        '''
        Parameters :
            url : [type -> String] web adress to request
            session : [type -> ClientSession] requests session

        Returns :
            text : [type -> String] returns the data got from excecuting url

        Does :
            It will asynchronously executes the url and wait for response, 
            then returns the reponse text, if server failed to respond then it will,
            raise an exception for any status code other than 200
        '''
        
        raw_data = await session.request(
            method='GET',
            url=url
            )
        raw_data.raise_for_status()
        text = await raw_data.text()

        # print(f"\n\n\tData:\n{text}\n\n")
        return text

    # function for extracting the required information form the scraped data
    async def parse_data(self, data: str, file: IO, session: ClientSession, obj: '__main__._Summary_indeed') -> None:

        '''
        Parameters : 
            data : [type -> String] the data from response
            file : [type -> path or str or IO] file to which the scraped and formatted data will be written
            session : [type -> ClientSession] requests session
            obj : [type -> object of class] objewct of inner class _Summery_indeed
        
        Returns:
            None

        Does : 
             it will fetch the required data from the response data, and 
             asynchronously writes to a file
        '''

        souped_object = bs(data, 'html.parser')
        
        td = souped_object.find(name='td', attrs={"id": "resultsCol"})
        #print(td)
        
        td = bs(str(td), 'html.parser')
        div_cards = td.find_all('div', {"class","jobsearch-SerpJobCard unifiedRow row result"})

        #print(f"\n\n\ttotal:{len(div_cards)}\n\n")


        #jobs = []
        async with aiofiles.open(file, 'a') as wr_file:
            for div in div_cards:
                fetched_data = {
                    'id': "i_"+str(self.jobs_found),
                    'title': (div.find('h2', {'class', 'title'}).a.text).lstrip(),
                    'comp_name': (div.find('div', {'class', 'sjcl'}).find('span',{'class', 'company'}).text).lstrip(),
                    'location': (div.find('div', {'class', 'sjcl'}).find('div', {'class', 'recJobLoc'})['data-rc-loc']).lstrip(),
                    'posting': (div.find('div', {'class', 'jobsearch-SerpJobCard-footer'}).find('span', {'class', 'date'}).text).lstrip(),
                    'link': urllib.parse.urljoin("https://www.indeed.co.in", div.find('h2', {'class', 'title'}).a['href']),
                    'summary': (div.find('div', {'class', 'summary'}).text).lstrip(),
                }
                
                #jobs.append(fetched_data)
                self.jobs_found+=1
                #await wr_file.write(bytes(json.dumps(fetched_data), 'utf-8'))
                await wr_file.write(json.dumps(fetched_data)+"\n")
                await obj.main(data = fetched_data)
            #await wr_file.write(json.dumps(jobs))
        await self.find_next_pages(data, session, file, obj)

    # function for finding next page exist or not and fetch data from there
    async def find_next_pages(self, data: str, session: ClientSession, file: IO, obj: '__main__._Summary_indeed') -> None:

        '''
        Parameters:
            data: [type -> str] response data by executing the url
            session : [type -> ClientSession] requests session
            file : [type -> path or str or IO] file to which the scraped and formatted data will be written
            obj : [type -> object of class] objewct of inner class _Summery_indeed

        Returns:
            None

        Does:
            check if the next page exists then it will fetch data from that page,
            by giving recursive call to get_data method
        '''
        soup_obj = bs(data, 'html.parser')

        td = soup_obj.find(name='td', attrs={'id':'resultsCol'})\
            .find(name='nav', attrs={'role':'navigation'})\
                .find(name='div', attrs={'class':'pagination'})\
                    .find(name='ul', attrs={'class':'pagination-list'})

        li_list = td.find_all(name='li')
        #print(f"\n\nFooters:{len(li_list)}")
        i = 0
        next_url = None
        for li in li_list:
            i+=1
            if i == 1:
                continue
            #x = li.find(name='a', attrs={'aria-label':'next'})
            #li = bs(str(li), 'html.parser')
            #if li.find(name='a', attrs={'aria-label': 'Next'}) != None:
            try:

                if li.a['aria-label'] == 'Next':
                    next_url = urllib.parse.urljoin('https://www.indeed.co.in', li.a['href'])
                    #print(f"\n\n{i}\n\n{next_url}\n\n")
            except TypeError as e:
                continue

            if next_url != None:
                self.pages_found+=1
                await self.get_data(url=next_url, file=file, session=session, obj=obj)
            #print(f"\n\npage-{self.pages_found}\n")

    # function for handling everything asynchronously
    async def get_data(self, url: str, file: IO, session: ClientSession, obj: '__main__._Summary_indeed') -> None:

        '''
        Parameters:
            url : [type -> String] web address to fetch data from
            file : [type -> str or path or IO] file to which the scrapd data should be saved
            session : [type -> ClientSession] session for requesting the data
            obj : [type -> object of class] objewct of inner class _Summery_indeed

        Returns:
            None
        
        Does:
            Function will first asynchronously collect the data from server,
            then pass it to the parse function for parsing the data and saving to the file,
            if server failed to respond then the exception will be raised
        '''

        try:

            raw_data = await self.execute_url(url=url, session=session)

        except Exception as e:
            
            logger.exception(f"Exception while executing url {url}")
        
        else:
            # code fore processing the data
            try:
                await self.parse_data(data=raw_data, file=file, session=session, obj=obj)

            except Exception as e:
                logger.exception(f"\nException occured while parsing dta from url {url}\n\n")
    
    # gathering all jobs in a asynchronous way
    async def get_card_data(self, out_path: IO, obj: '__main__._Summary_indeed') -> None:

        '''
        Parameters:
            out_path: [type -> str or path or IO] path to the file where the scraped data will be saved
            obj : [type -> object of class] objewct of inner class _Summery_indeed

        Returns:
            None

        Does:
            for all the available url from url list the function asynchronously, 
            give a call to each url by calling function get_data and gather each ones data
        '''

        async with ClientSession() as session:
            tasks = []
            for url in self.url_list:

                tasks.append(
                    self.get_data(url=url, file=out_path, session=session, obj=obj)
                )

            await asyncio.gather(*tasks)

    # main function for start point
    def main(self):

        start = time.perf_counter()

        out_path = pwd.joinpath(self.f_name+'_indeed_data.txt')

        with open(out_path, 'w') as write_file:
            write_file.write("")
        write_file.close()

        try:
            obj = self._Summary_indeed(parent_call = self, file_start = self.f_name)
            
            asyncio.run(self.get_card_data(out_path=out_path, obj=obj))

            # call to each job links
            # obj.main()

        except RuntimeError as e:
            print("")

        end = time.perf_counter() - start

        print(f"\n\n\t Time took for indeed data scraping :{end}\n\n")

    # inner class for scraping the individual job data
    class _Summary_indeed:

        # defining the constructor
        def __init__(self, parent_call, file_start: IO):

            self.file_start = file_start
            self.outer = parent_call

        # function for scraping the data and saving it to the file
        async def scrape_full_data(self, data: dict, write_file: IO, session: ClientSession) -> None:

            '''
            Parameters:
                data: [type -> dictionary] dict data of job
                write_file: [type -> IO or str or path] file path to which the scraped data need to be written
                session: [type -> ClientSession] session for requesting the data

            Returns:
                None

            Does:
                > it will first get data from excecuting the url,
                > then scrape through the data and find the required data,
                > then save it to the file
                > everything happens asynchronously

            '''

            try:
                raw_data = await self.outer.execute_url(url=data['link'], session=session)

            except Exception as e:
                logger.exception(f"Exception while executing  url {data['link']}\n in full datat domain\n")
                async with aiofiles.open(write_file, 'a') as wr_file:
                    await wr_file.write(json.dumps(data)+'\n')

            else:
                try:

                    soup = bs(raw_data, 'html.parser')

                    job_card = soup.find(name='div', attrs={'class':'jobsearch-JobComponent'})

                    f_data = {
                        'id' : data['id'],
                        'title': job_card.find(name="div", attrs={'class':'jobsearch-JobInfoHeader-title-container'}).text,
                        'comp_name': job_card.find(name="div", attrs={'class':'jobsearch-CompanyInfoWithoutHeaderImage'}).text,
                        'location': data['location'],
                        'posting': job_card.find(name='div', attrs={'class':'jobsearch-JobMetadataFooter'}).text,
                        'link': '',
                        'summary': job_card.find(name='div', attrs={'id':'jobDescriptionText'}).text,
                    }
                    _l = job_card.find(name='div', attrs={'id':'applyButtonLinkContainer'})
                    if _l != None:
                        f_data['link'] = _l.a['href']
                        #print("\n\n link : %s\n\n",_l.a['href'])
                    else:
                        f_data['link'] = data['link']

                    #print(f"\n\n{data}\n\n")
                    async with aiofiles.open(write_file, 'a') as wr_file:
                        await wr_file.write(json.dumps(f_data)+'\n')

                except Exception as e:
                    async with aiofiles.open(write_file, 'a') as wr_file:
                        await wr_file.write(json.dumps(data)+'\n')

        # main method
        async def main(self, data: dict) -> None:

            '''
            Parameters:
                data: [type -> dict] Dictionary data of job

            Returns:
                None

            Does:
                it will first create a file for saving the data,
                then call the function get summary for fetching the dat

            '''

            write_path = pwd.joinpath(self.file_start+'_indeed_summary.txt')

            f_stat = pathlib.Path(write_path)

            if not f_stat.exists():
                
                with open(write_path, 'w') as wr_file:
                    wr_file.write("")
                wr_file.close()

            try:

                #asyncio.run(self.get_full_data(write_file=write_path))
                async with ClientSession() as session2:
                    await self.scrape_full_data(data=data, write_file=write_path, session=session2)

            except RuntimeError as e:
                print("")
       

# class for scraping the data from linked in website
class Linked_in_scrape:

    # defining the constructor
    def __init__(self, key: str, loc: str):

        self.key = key

        self.loc = loc

        self.f_name = f_start+"_"+key.replace(" ","_").replace(",", "_")+"_"+loc.replace(" ","_").replace(",", "_")

        # url from where we want to scrape
        self.url = "https://www.linkedin.com/jobs/search?"
        
        # url for fetching pages from search keys
        self.more_res_url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?"

        # list to store all key_word and location data
        self.url_list = []

        self.pages_found = 0

        self.jobs_found = 0
        
    # function for preaparing the url
    def prepare_url(self) -> None:

        '''
        Params :
            key: [type -> str] Keyword for search of a particular job
            loc: [type -> str] job location,

        Return :
            None

        Does :
            Loop throught all the keywords and location from keys file,
            and forms all the possible combinations
        '''

        key = ((self.key.replace(" ", "%20")).replace(",", "%2C")).replace("/", "%2F")
        loc = ((self.loc.replace(" ", "%20")).replace(",", "%2C")).replace("/", "%2F")

        self.url_list.append(self.more_res_url+'keywords='+key+'&location='+loc+'&redirect=false&position=1&pageNum=0&start=')
                
        print(f"\n\nLinked in url List :  \n{self.url_list}\n\n")

        
        
    # function for executing url
    async def execute_url(self, url: str, session: ClientSession, start: int = 0) -> str:

        '''
        Parameters :
            url : [type -> String] web adress to request
            session : [type -> ClientSession] requests session
            start : [type -> integer] start point to fetch data

        Returns :
            text : [type -> String] returns the data got from excecuting url

        Does :
            It will asynchronously executes the url and wait for response, 
            then returns the reponse text, if server failed to respond then it will,
            raise an exception for any status code other than 200
        '''
        
        headers = {
            'authority': 'www.linkedin.com',
            #'path': '/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=ui%20developer&location=Hyderabad%2C%20Telangana&redirect=false&position=1&pageNum=0&start='+str(start),
            'scheme': 'https',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
        }
        raw_data = await session.request(
            method='GET',
            url=url,
            headers=headers
        )
        raw_data.raise_for_status()
        text = await raw_data.text()

        return text

    # function for parsing the scraped data
    async def parse_data(self, data: str, file: IO, obj: '__main__._Summary') -> None:

        '''
        Parameters : 
            data : [type -> String] the data from response
            file : [type -> path or str or IO] file to which the scraped and formatted data will be written
            obj: [type -> object of a class] object to class summary
        
        Returns:
            None

        Does : 
             it will fetch the required data from the response data, and 
             asynchronously writes to a file
        '''

        soup = bs(data, 'html.parser')

        _lists = soup.find_all(name='li')

        async with aiofiles.open(file, 'a') as fp:
            
            for l in _lists:
                fetched_data = {
                    'id' : "l_"+str(self.jobs_found),
                    'title' : l.div.h3.text,
                    'comp_name' : l.div.h4.text,
                    'location' : l.div.span.text,
                    'posting' : l.div.time.text,
                    'link' : l.a['href'],
                    'summary' : ' ',
                }
                self.jobs_found+=1
                await fp.write(json.dumps(fetched_data)+'\n')
                await obj.main(data = fetched_data)
                
    # function for scraping the data
    async def get_data(self, url: str, file: IO, session: ClientSession, obj: '__main__._Summary',start: int = 0) -> None:

        '''
        Parameters:
            url : [type -> String] web address to fetch data from
            file : [type -> str or path or IO] file to which the scrapd data should be saved
            session : [type -> ClientSession] session for requesting the data
            start : [type -> integer] Start path for scraping the data
            obj: [type -> object of a class] object to class summary

        Returns:
            None
        
        Does:
            Function will first asynchronously collect the data from server,
            then pass it to the parse function for parsing the data and saving to the file,
            if server failed to respond then the exception will be raised
        '''

        try:

            raw_data = await self.execute_url(url=url+str(start), session=session)

        except Exception as e:
            logger.exception(f"Exception occured while Excecuting url : {url}")
            #print(f"Exception occured while Excecuting url : {url}\n{str(e)}")
            return ''

        else:
            if raw_data:

                try :
                    await self.parse_data(data=raw_data, file=file, obj=obj)

                    await self.get_data(url=url, file=file, session=session, start=start+25, obj=obj)
                
                except Exception as e:

                    logger.exception(f"Exception occured while parsing data from url\n{url}")


            else:
                #print("\n\nData not returned\n\n")
                print("\n\n\tData not returned\n\n")

    # start point for asynchronous excecution
    async def get_card_data(self, file: IO, obj: '__main__._Summary') -> None:

        '''
        Parameters:
            file: [type -> str or path or IO] path to the file where the scraped data will be saved
            obj: [type -> object of a class] object to class summary 

        Returns:
            None

        Does:
            for all the available url from url list the function asynchronously, 
            give a call to each url by calling function get_data and gather each ones data
        '''

        async with ClientSession() as session:

            tasks = []

            for url in self.url_list:

                tasks.append(
                    self.get_data(url=url, session=session, file=file, obj=obj)
                )

            await asyncio.gather(*tasks)
        

    # main method from where the program excecution begins
    def main(self) -> None:
        
        '''
        Parameters:
            None:

        Returns:
            None

        Does:
            The function will create the file for saving data,
            then give call for a get_card_data method for collecting the data,
            then call a inner class for collecting the individual job data
        '''
        start_t = time.perf_counter()

        out_path = pwd.joinpath(self.f_name+'_linked_in_data.txt')

        with open(out_path, 'w') as fp:
            fp.write("")
        fp.close()
        try:
            obj = self._Summary(f_name=self.f_name, parent_call=self)

            asyncio.run(self.get_card_data(file=out_path, obj = obj))
            
            
            #obj.main()
        
        except Exception as e:

            print(str(e))
            logger.exception("Exception in linkedin scraping")

        elapsed_t = time.perf_counter() - start_t

        print(f"\n\n\tTotal Time took for linked in data scraping : {elapsed_t}\n\n")

    # inner class for scraping the individual job data 
    class _Summary:

        # defining the constructor
        def __init__(self, f_name: str, parent_call):

            self.f_start = f_name
            self.outer = parent_call

        # function to parse and save data
        async def parse_and_write(self, url: str, id: str, data: str, file: IO) -> None:

            '''
            Parameters:
                url : [type -> str] url from which we are obtained data
                data : [type -> str] the scraped data from the web
                file : [type -> str or path or IO] the file path to which the data need to be stored

            Returns;
                None

            Does:
                parse the response text and asynchronously save the data to file
            '''

            soup = bs(data, 'html.parser')
            top_card = soup.find(name='section', attrs={'class': 'topcard'}).find(name='div', attrs={'class': 'topcard__content'})
            disc = soup.find(name='section', attrs={'class': 'description'})
            meta = top_card.find(name='div',attrs={'class': 'topcard__content-left'})
            rmeta= top_card.find(name='div',attrs={'class': 'topcard__content-right'})

            fetched_data = {
                'id' : id,
                'title' : str(meta.h2.text),
                'comp_name' : meta.h3.span.text,
                'location' : meta.h3.find(name='span', attrs={'class':'topcard__flavor topcard__flavor--bullet'}).text,
                'posting' : str([h3.span.text for h3 in meta.find_all(name='h3') if h3.find(name='span', attrs={'class':'posted-time-ago__text'})]),
                'link': '',
                'summary': disc.text,
                }
            try:
                fetched_data['link'] = rmeta.a['href']
            except (TypeError, AttributeError) as e:
                fetched_data['link'] = url

            async with aiofiles.open(file, 'a') as fp:
                await fp.write(json.dumps(fetched_data)+"\n")

        # function for getting the data
        async def scrape_data(self, data: dict, file: IO, session: ClientSession) -> None:
            
            '''
            Parameters:
                data : [type -> dictionary] data dictionary
                file : [type -> str or path or IO] file to which the scrapd data should be saved
                session : [type -> ClientSession] session for requesting the data
            
            Returns:
                None

            Does:
                Function will first asynchronously collect the data from server,
                then pass it to the parse function for parsing the data and saving to the file,
                if server failed to respond then the exception will be raised
            '''
            try:
                raw_data = await self.outer.execute_url(url=data['link'], session=session)

            except Exception as e:
                #print(f"Exception occured while executing url :\n {url}\n{str(e)}")
                logger.exception(f"Exception occured while executing url :\n {data['link']}")

                async with aiofiles.open(file, 'a') as fp:
                    await fp.write(json.dumps(data)+"\n") 
            else:
                try:
                    await self.parse_and_write(url = data['link'], id = data['id'], data = raw_data, file=file)

                except Exception as e:
                    logger.exception("Exception fetching failed in linked in ")

                    async with aiofiles.open(file, 'a') as fp:
                        await fp.write(json.dumps(data)+"\n")

         # main function of class           
        async def main(self, data: dict) -> None:

            '''
            Parameters:
                data: [type -> dictionary] the dictionary of data to which we need to fetch details 
            
            Returns:
                None

            Does:
                it will first create a file for saving the data,
                then call the function get summary for fetching the data
            '''
            wr_file = pwd.joinpath(self.f_start+'_linked_in_summary.txt')

            f_stat = pathlib.Path(wr_file)

            if not f_stat.exists():
                async with aiofiles.open(wr_file, 'w') as fp:
                    await fp.write("")
                    fp.close()

            try:
                # asyncio.run(self.get_summary(write_file = wr_file, data = data))

                async with ClientSession() as session2:
                    await self.scrape_data(data = data, file = wr_file, session = session2)
            
            except RuntimeError as re:
                print("Ignored")
            except Exception as e:
                logger.exception("Exception occured : "+str(vars(e)))
                #print("Exception occured : "+str(vars(e)))

def start_linked(key: str, loc: str):

    l = Linked_in_scrape(key = key, loc = loc)
    l.prepare_url()
    l.main()

def start_indeed(key: str, loc: str):

    i = Indeed_scrape(key = key, loc = loc)
    i.prepare_url()
    i.main()

def main(key: str, loc: str):

    import threading
    
    th_1 = threading.Thread(target=start_linked, args=(key, loc,))
    th_2 = threading.Thread(target=start_indeed, args=(key, loc,))

    th_1.start()
    th_2.start()