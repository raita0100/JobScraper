# JobScraper
> ### Scraping project

## Description

In this project we are trying to scrape the website. Scraping happens here ***asynchronously***.

## Requirements

- Python version used for developing.  
***```Python 3.8.5```***
- create a vertual environment for running project.  
```python -m venv crawler-env```.  
- activate the vertual environment.
- then install libraries from [requirements.txt file](https://github.com/raita0100/JobScraper/blob/master/backend/requirements.txt).  
```pip install -r requirements.txt```  

## Usage  

- For running file [_live_crawler.py](https://github.com/raita0100/JobScraper/blob/master/backend/_live_crawler.py).  
  ```python
  import _live_crawler as crawler
  
  crawler.main(key="key word", loc="location of work")
  
  ```  
  
- for running file [_crawler.py](https://github.com/raita0100/JobScraper/blob/master/backend/_crawler.py).  
  First store your job search keywords and locations in the [keys.py](https://github.com/raita0100/JobScraper/blob/master/backend/keys.py) file.  
  then run 
  ```cmd 
  python _crawler.py 
  ```
  

