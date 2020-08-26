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

#importing keys file
import keys as keys

# setting the logging files

# create a logger with name with filename
logger = logging.getLogger(__name__)

# create a handler
f_name = time.strftime('%Y_%m_%d')
pwd = pathlib.Path(__file__).parent
log_file = pwd.joinpath(f_name+".log")
# print(f"filename {log_file}")

f_handler = logging.FileHandler(filename=log_file)
f_handler.setLevel(logging.ERROR)

# create a formatter
f_fomatter = logging.Formatter('%(asctime)s : %(name)s : %(levelname)s : %(message)s')
f_handler.setFormatter(f_fomatter)

# adding the handler to the logger
logger.addHandler(f_handler)


# class for scraping the data from indeed website
class Indeed_scrape:

    # defining the constructor
    def __init__(self):

        self.url = "https://www.indeed.co.in/jobs?"
        self.url_list = []
        self.pages_found = 0
        
    # function for preaparing the url
    def prepare_url(self) -> None:

        '''
        Params :
            NOne

        Return :
            None

        Does :
            Loop throught all the keywords and location from keys file,
            and forms all the possible combinations
        '''

        for key in keys.__keywords__:

            for loc in keys.__locations__:

                key = ((key.replace(" ", "+")).replace(",", "%2C")).replace("/", "%2F")
                loc = ((loc.replace(" ", "+")).replace(",", "%2C")).replace("/", "%2F")

                self.url_list.append(self.url+'q='+key+'&l='+loc)

        print(f"\n\nList :  {self.url_list}\n\n")

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
    async def parse_data(self, data: str, file: IO, session: ClientSession) -> None:

        '''
        Parameters : 
            data : [type -> String] the data from response
            file : [type -> path or str or IO] file to which the scraped and formatted data will be written
            session : [type -> ClientSession] requests session
        
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

        print(f"\n\n\ttotal:{len(div_cards)}\n\n")


        jobs = []
        async with aiofiles.open(file, 'a') as wr_file:
            for div in div_cards:
                fetched_data = {
                    'title': (div.find('h2', {'class', 'title'}).a.text).lstrip(),
                    'link': urllib.parse.urljoin("https://www.indeed.co.in", div.find('h2', {'class', 'title'}).a['href']),
                    'comp_name': (div.find('div', {'class', 'sjcl'}).find('span',{'class', 'company'}).text).lstrip(),
                    'location': (div.find('div', {'class', 'sjcl'}).find('div', {'class', 'recJobLoc'})['data-rc-loc']).lstrip(),
                    'summary': (div.find('div', {'class', 'summary'}).text).lstrip(),
                    'posting': (div.find('div', {'class', 'jobsearch-SerpJobCard-footer'}).find('span', {'class', 'date'}).text).lstrip()
                }
                
                jobs.append(fetched_data)
                #await wr_file.write(bytes(json.dumps(fetched_data), 'utf-8'))
                await wr_file.write(json.dumps(fetched_data)+"\n")

            #await wr_file.write(json.dumps(jobs))ī
        await self.find_next_pages(data, session, file)

    # function for finding next page exist or not and fetch data from there
    async def find_next_pages(self, data: str, session: ClientSession, file: IO) -> None:

        '''
        Parameters:
            data: [type -> str] response data by executing the url
            session : [type -> ClientSession] requests session
            file : [type -> path or str or IO] file to which the scraped and formatted data will be written

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
                await self.get_data(url=next_url, file=file, session=session)
            #print(f"\n\npage-{self.pages_found}\n")

    # function for handling everything asynchronously
    async def get_data(self, url: str, file: IO, session: ClientSession) -> None:

        '''
        Parameters:
            url : [type -> String] web address to fetch data from
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

            raw_data = await self.execute_url(url=url, session=session)

        except Exception as e:
            
            logger.exception(f"Exception while executing url {url}")
        
        else:
            # code fore processing the data
            await self.parse_data(data=raw_data, file=file, session=session)
    
    # gathering all jobs in a asynchronous way
    async def get_card_data(self, out_path: IO) -> None:

        '''
        Parameters:
            out_path: [type -> str or path or IO] path to the file where the scraped data will be saved

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
                    self.get_data(url=url, file=out_path, session=session)
                )

            await asyncio.gather(*tasks)

    # main function for start point
    def main(self):

        start = time.perf_counter()

        out_path = pwd.joinpath(f_name+'_indeed_data.txt')

        with open(out_path, 'w') as write_file:
            write_file.write("")
        write_file.close()

        try:
            asyncio.run(self.get_card_data(out_path=out_path))

            # call to each job links
            obj = self._Summary(read_file=out_path, parent_call=self)
            obj.main()

        except RuntimeError as e:
            print("")

        end = time.perf_counter() - start

        print(f"\n\n\t Time took for indeed data scraping :{end}\n\n")

    # inner class for scraping the individual job data
    class _Summary:

        # defining the constructor
        def __init__(self, read_file: IO, parent_call):

            self.f_start = f_name
            self.read_file = read_file
            self.outer = parent_call

        # function for scraping the data and saving it to the file
        async def scrape_full_data(self, url: str, write_file: IO, session: ClientSession) -> None:

            '''
            Parameters:
                url: [type -> str] url from which data need to be fetched
                write_file: [type -> IO or str or path] file path to which the scraped data need to be written

            Returns:
                None

            Does:
                > it will first get data from excecuting the url,
                > then scrape through the data and find the required data,
                > then save it to the file
                > everything happens asynchronously

            '''

            try:
                raw_data = await self.outer.execute_url(url=url, session=session)

            except Exception as e:
                logger.exception(f"Exception while executing  url {url}\n in full datat domain\n")

            else:
                soup = bs(raw_data, 'html.parser')

                job_card = soup.find(name='div', attrs={'class':'jobsearch-JobComponent'})

                data = {
                    'title': job_card.find(name="div", attrs={'class':'jobsearch-JobInfoHeader-title-container'}).text,
                    'info': job_card.find(name="div", attrs={'class':'jobsearch-CompanyInfoWithoutHeaderImage'}).text,
                    #'sal': job_card.find(name='div', attrs={'class':'jobsearch-JobMetadataHeader-item'}).text,
                    'desc': job_card.find(name='div', attrs={'id':'jobDescriptionText'}).text,
                    'footer': job_card.find(name='div', attrs={'class':'jobsearch-JobMetadataFooter'}).text,
                }
                _l = job_card.find(name='div', attrs={'id':'applyButtonLinkContainer'})
                if _l != None:
                    data['link'] = _l.a['href']
                    #print("\n\n link : %s\n\n",_l.a['href'])
                else:
                    data['link'] = url

                #print(f"\n\n{data}\n\n")
                async with aiofiles.open(write_file, 'a') as wr_file:
                    await wr_file.write(json.dumps(data)+'\n')

        # function for asynchronously fetching the data
        async def get_full_data(self, write_file: IO) -> None:

            '''
            Parameters:
                write_file : [type -> IO or str or path] path to the file where the data wil be stored

            Returns:
                None

            Does:
                initiates the asynchronous call for all the url present in the file of all jobs then,
                call to method get_summary asynchronously for all urls.
            '''

            async with ClientSession() as session:
                pages = []
                jobs = [json.loads(line) for line in open(self.read_file, 'r')]

                for job in jobs:

                    pages.append(
                        self.scrape_full_data(url=job['link'], write_file=write_file, session=session)
                    )

                await asyncio.gather(*pages)

        # main method
        def main(self) -> None:

            '''
            Parameters:
                None

            Returns:
                None

            Does:
                it will first create a file for saving the data,
                then call the function get summary for fetching the dat

            '''

            write_path = pwd.joinpath(f_name+'_indeed_summary.txt')

            with open(write_path, 'w') as wr_file:
                wr_file.write("")
            wr_file.close()

            try:

                asyncio.run(self.get_full_data(write_file=write_path))

            except RuntimeError as e:
                print("")
       

# class for scraping the data from linked in website
class Linked_in_scrape:

    # defining the constructor
    def __init__(self):

        # url from where we want to scrape
        self.url = "https://www.linkedin.com/jobs/search?"
        
        # url for fetching pages from search keys
        self.more_res_url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?"

        # list to store all key_word and location data
        self.url_list = []

        self.pages_found = 0
        
    # function for preaparing the url
    def prepare_url(self) -> None:

        '''
        Params :
            NOne

        Return :
            None

        Does :
            Loop throught all the keywords and location from keys file,
            and forms all the possible combinations
        '''
        for key in keys.__keywords__:
            
            for loc in keys.__locations__:

                key = ((key.replace(" ", "%20")).replace(",", "%2C")).replace("/", "%2F")
                loc = ((loc.replace(" ", "%20")).replace(",", "%2C")).replace("/", "%2F")

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
    async def parse_data(self, data: str, file: IO) -> None:

        '''
        Parameters : 
            data : [type -> String] the data from response
            file : [type -> path or str or IO] file to which the scraped and formatted data will be written
        
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
                    'title' : l.div.h3.text,
                    'link' : l.a['href'],
                    'comp_name' : l.div.h4.text,
                    'location' : l.div.span.text,
                    'summary' : ' ',
                    'posting' : l.div.time.text
                }

                await fp.write(json.dumps(fetched_data)+'\n')
                
    # function for scraping the data
    async def get_data(self, url: str, file: IO, session: ClientSession, start: int = 0) -> None:

        '''
        Parameters:
            url : [type -> String] web address to fetch data from
            file : [type -> str or path or IO] file to which the scrapd data should be saved
            session : [type -> ClientSession] session for requesting the data
            start : [type -> integer] Start path for scraping the data

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

                await self.parse_data(data=raw_data, file=file)

                await self.get_data(url=url, file=file, session=session, start=start+25)

            else:
                #print("\n\nData not returned\n\n")
                print("\n\n\tData not returned\n\n")

    # start point for asynchronous excecution
    async def get_card_data(self, file: IO) -> None:

        '''
        Parameters:
            file: [type -> str or path or IO] path to the file where the scraped data will be saved

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
                    self.get_data(url=url, session=session, file=file) 
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

        out_path = pwd.joinpath(f_name+'_linked_in_data.txt')

        with open(out_path, 'w') as fp:
            fp.write("")
        fp.close()
        try:
            asyncio.run(self.get_card_data(file=out_path))
            
            obj = self._Summary(f_name=f_name, read_file=out_path, parent_call=self)
            obj.main()
        
        except Exception as e:

            print(str(e))
            logger.exception()

        elapsed_t = time.perf_counter() - start_t

        print(f"\n\n\tTotal Time took for linked in data scraping : {elapsed_t}\n\n")

    # inner class for scraping the individual job data 
    class _Summary:

        # defining the constructor
        def __init__(self, f_name: str, read_file: IO, parent_call):

            self.f_start = f_name
            self.read_file = read_file
            self.outer = parent_call

        # function to parse and save data
        async def parse_and_write(self, url: str, data: str, file: IO) -> None:

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
                'title' : str(meta.h2.text),
                'company' : meta.h3.span.text,
                'loc' : meta.h3.find(name='span', attrs={'class':'topcard__flavor topcard__flavor--bullet'}).text,
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
        async def scrape_data(self, url: str, file: IO, session: ClientSession) -> None:
            
            '''
            Parameters:
                url : [type -> String] web address to fetch data from
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
                raw_data = await self.outer.execute_url(url=url, session=session)

            except Exception as e:
                #print(f"Exception occured while executing url :\n {url}\n{str(e)}")
                logger.exception(f"Exception occured while executing url :\n {url}")
            else:

                await self.parse_and_write(url = url, data = raw_data, file=file)

        # function for asynchronously fetching the data
        async def get_summary(self, write_file: IO) -> None:

            '''
            Parameters:
                write_file : [type -> IO or str or path] path to the file where the data wil be stored

            Returns:
                None

            Does:
                initiates the asynchronous call for all the url present in the file of all jobs then,
                call to method get_summary asynchronously for all urls.
            '''
            async with ClientSession() as session:
                
                tasks = []

                for line in open(self.read_file, 'r'):
                    tasks.append(
                        self.scrape_data(
                            url = json.loads(line)['link'], 
                            file = write_file,
                            session = session
                        )
                    )
                try:
                    await asyncio.gather(*tasks)
                except RuntimeError as re:
                    print("")

         # main function of class           
        def main(self) -> None:

            '''
            Parameters:
                None
            
            Returns:
                None

            Does:
                it will first create a file for saving the data,
                then call the function get summary for fetching the data
            '''
            wr_file = pwd.joinpath(self.f_start+'_linked_in_summary.txt')

            with open(wr_file, 'w') as fp:
                fp.write("")

            try:
                asyncio.run(self.get_summary(write_file=wr_file))
            except RuntimeError as re:
                print("Ignored")
            except Exception as e:
                logger.exception()
                #print("Exception occured : "+str(vars(e)))

            



# start the timer

start_time = time.perf_counter()

# get indeed data
i = Indeed_scrape()
i.prepare_url()
i.main()

# get Linked in data
l = Linked_in_scrape()
l.prepare_url()
l.main()

elapsed_time = time.perf_counter() - start_time

print(f"\n\n\t Total time took for excecuting the program : {elapsed_time}\n\n")
