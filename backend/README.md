# JobScraper
> ### Scraping project

## Description

In this project we are trying to scrape the website. Scraping happens here ***asynchronously***.

## Requirements

- Python version used for developing.  
***```Python 3.8.5```***
- create a vertual environment for running project.  
```cmd
>> python -m venv crawler-env
```

- activate the vertual environment.
- then install libraries from [requirements.txt file](https://github.com/raita0100/JobScraper/blob/master/backend/requirements.txt).  
```cmd
>> pip install -r requirements.txt
```  

## Usage  

- For running file [_live_crawler.py](https://github.com/raita0100/JobScraper/blob/master/backend/_live_crawler.py).  
  ```python
  import _live_crawler as crawler
  
  crawler.main(key="key word", loc="location of work")
  
  ```
- #### Running in flask server as api. [app.py](https://github.com/raita0100/JobScraper/blob/master/backend/app.py)
  > For windows
  ```cmd
  >> cd to\the\repo\backend
  >> set FLASK_APP=app.py
  >> flask run
  ```
  
  > For others
  ```shell
  $ cd to/the/repo/backend
  $ export FLASK_APP=app.py
  $ flask run
  ```
## Data we get
- After scraping the scraped data will be stored in the format of.
```json
{
  "id": "job_id", 
  "title": "Title of job", 
  "comp_name": "Name of a company", 
  "location": "job location", 
  "posting": "hosted time", 
  "link": "link for hosted job", 
  "summary": "Detail about job"
}
```
- this data will be displayed at user side.
