from tools.tool import TryExcept, Response, yaml_load, randomTime, userAgents, verify_amazon, flat, region, export_sheet, domain
from bs4 import BeautifulSoup
import pandas as pd
import asyncio
import re


class Amazon:
   

    def __init__(self, base_url, proxy):
       
        self.proxy = proxy
        self.country_domain = domain(base_url)
        self.region = region(base_url)

        # Regular expression pattern for currencies in different regions
        self.currency = r'["$₹R$€£kr()%¥\s]'   
       
        self.rand_time = 3 * 60
        self.base_url = base_url
        self.headers = {'User-Agent': userAgents()}
        self.catch = TryExcept()
        self.scrape = yaml_load('selector')


    async def status(self):
       
        response = await Response(self.base_url).response()
        return response


    async def num_of_pages(self, max_retries = 10):
        
        for retry in range(max_retries):
            try:
                content = await Response(self.base_url).content()
                soup = BeautifulSoup(content, 'lxml')

               
                try:
                    pages = await self.catch.text(soup.select(self.scrape['pages'])[-1])
                except IndexError:
                    pages = '1'

                try:
                    return int(pages)
                except ValueError:
                    return 2
            except ConnectionResetError as se:
                print(f"Connection lost: {str(e)}. Retrying... ({retry + 1} / {max_retries})")
                if retry < max_retries - 1:
                    await asyncio.sleep(5)  # Delay before retrying.
            except Exception as e:
                print(f"Retry {retry + 1} failed: {str(e)}")
                if retry < max_retries - 1:
                    await asyncio.sleep(4)  # Delay before retrying.


    async def split_url(self):
       
        split_url = [self.base_url]

        total_pages = await self.num_of_pages()

        content = await Response(self.base_url).content()
        soup = BeautifulSoup(content, 'lxml')

        next_link = f"""https://www.amazon.{self.country_domain}{await self.catch.attributes(soup.select_one(self.scrape['next_button']), 'href')}"""
        for num in range(1, total_pages):

            next_url = re.sub(r'page=\d+', f'page={num+1}' , next_link)

            next_url = re.sub(r'sr_pg_\d+', f'sr_pg_{num}', next_url)
            split_url.append(next_url)
        return split_url


    async def getASIN(self, url):
       
        pattern = r"(?<=dp\/)[A-Za-z|0-9]+"
        try:
            asin = (re.search(pattern, url)).group(0)
        except Exception as e:
            asin = "N/A"
        return asin


    async def category_name(self):
        resp = Response(self.base_url)
        
        content = await resp.content()
        soup = BeautifulSoup(content, 'lxml')
        try:
            searches_results = soup.select_one(self.scrape['searches_I']).text.strip()
        except AttributeError:
            try:
                searches_results = re.sub(r'["]', '', soup.select_one(self.scrape['searches_II']).text.strip())
            except AttributeError:
                try:
                    searches_results = soup.select_one(self.scrape['searches_III']).text.strip()
                except AttributeError:
                    searches_results = soup.select_one(self.scrape['searches_IV']).text.strip()

        return searches_results


    async def product_urls(self, url, max_retries = 10):
       
        for retry in range(max_retries):
            try:
                content = await Response(url).content()
                soup = BeautifulSoup(content, 'lxml')

                try:
                    soup.select_one(self.scrape['main_content'])
                except Exception as e:
                    return f"Content loading error. Please try again in few minutes. Error message: {e}"
                card_contents = [f"""https://www.amazon.{self.country_domain}{prod.select_one(self.scrape['hyperlink']).get('href')}""" for prod in soup.select(self.scrape['main_content'])]
                return card_contents
            except ConnectionResetError as se:
                print(f"Connection lost: {str(e)}. Retrying... ({retry + 1} / {max_retries})")
                if retry < max_retries - 1:
                    await asyncio.sleep(5)  # Delay for retrying.
            except Exception as e:
                print(f"Retry {retry + 1} failed: {str(e)}")
                if retry < max_retries - 1:
                    await asyncio.sleep(5)  # Delay for retrying.

        raise Exception(f"Failed to retrieve valid data after {max_retries} retries.")


    async def scrape_product_info(self, url, max_retries = 10):
        
        amazon_dicts = []
        for retry in range(max_retries):
            try:
                content = await Response(url).content()
                soup = BeautifulSoup(content, 'lxml')

                product = soup.select_one(self.scrape['name']).text.strip()
                print(product)

                if product == "N/A":
                    raise Exception("Product is 'N/A' retrying...")
                try:
                    image_link = soup.select_one(self.scrape['image_link_i']).get('src')
                except Exception as e:
                    image_link = await self.catch.attributes(soup.select_one(self.scrape['image_link_ii']), 'src')
                try:
                    availabilities = soup.select_one(self.scrape['availability']).text.strip()
                except AttributeError:
                    availabilities = 'In stock'
                price = await self.catch.text(soup.select_one(self.scrape['price_us']))
                if 'Page' in price.split():
                    price = await self.catch.text(soup.select_one(self.scrape['price_us_i']))
                if price != "N/A":
                    price = re.sub(self.currency, '', price)
                try:
                    deal_price = await self.catch.text(soup.select(self.scrape['deal_price'])[0])
                    if 'Page' in deal_price.split():
                        deal_price = "N/A"
                except Exception as e:
                    deal_price = "N/A"
                if deal_price != "N/A":
                    deal_price = re.sub(self.currency, '', deal_price)
                try:
                    savings = await self.catch.text(soup.select(self.scrape['savings'])[-1])
                except IndexError:
                    savings = "N/A"
                try:
                    ratings = float(soup.select_one(self.scrape['review']).text.strip().replace(" out of 5 stars", ''))
                except Exception as e:
                    ratings = "N/A"
                try:
                    rating_count = float(re.sub(r'[,\sratings]', '', soup.select_one(self.scrape['rating_count']).text.strip()))
                except Exception as e:
                    rating_count = "N/A"
                store = await self.catch.text(soup.select_one(self.scrape['store']))
                store_link = f"""https://www.amazon.{self.country_domain}{await self.catch.attributes(soup.select_one(self.scrape['store']), 'href')}"""

                datas = {
                    'Name': product,
                    'ASIN': await self.getASIN(url),
                    'Region': self.region,
                    'Description': ' '.join([des.text.strip() for des in soup.select(self.scrape['description'])]),
                    'Breakdown': ' '.join([br.text.strip() for br in soup.select(self.scrape['prod_des'])]),
                    'Price': price,
                    'Deal Price': deal_price,
                    'You saved': savings,
                    'Rating': ratings,
                    'Rating count': rating_count,
                    'Availability': availabilities,
                    'Hyperlink': url,
                    #'Image': image_link,
                    #'Images': [imgs.get('src') for imgs in soup.select(self.scrape['image_lists'])],
                    'Store': store.replace("Visit the ", ""),
                    #'Store link': store_link,
                }
                amazon_dicts.append(datas)
                return amazon_dicts
            except ConnectionResetError as se:
                print(f"Connection lost: {str(e)}. Retrying... ({retry + 1} / {max_retries})")
                if retry < max_retries - 1:
                    await asyncio.sleep(5)  # Delay for retrying.
            except Exception as e:
                print(f"Retry {retry + 1} failed: {str(e)} | Error URL : {url}")
                if retry < max_retries - 1:
                    await asyncio.sleep(5)  # Delay for retrying.
                return amazon_dicts
        raise Exception(f"Failed to retrieve valid data after {max_retries} retries.")


    async def crawl_url(self):
        
        page_lists = await self.split_url()
        coroutines = [self.product_urls(url) for url in page_lists]
        results = await asyncio.gather(*coroutines)
        return flat(results)


    async def scrape_and_save(self, url):
        
        random_sleep = await randomTime(self.rand_time)
        await asyncio.sleep(random_sleep)
        datas = await self.scrape_product_info(url)
        return datas


    async def csv_sheet(self, url):
       
        frames = await self.scrape_and_save(url)
        return pd.DataFrame(frames)


    async def concurrent_scraping(self):
       
        if await verify_amazon(self.base_url):
            return "I'm sorry, the link you provided is invalid. Could you please provide a valid Amazon link for the product category of your choice?"

       
        print(f"----------------------- |Welcome to Amazon {self.region}. |---------------------------------")
        searches = await self.category_name()
        print(f"Scraping category || {searches}.")

        number_pages = await self.num_of_pages()
        print(f"Total pages || {number_pages}.")

        product_urls = await self.crawl_url()
        print(f"The extraction process has begun and is currently in progress. The web scraper is scanning through all the links and collecting relevant information. Please be patient while the data is being gathered.")

        coroutines = [self.scrape_and_save(url) for url in product_urls]
        dfs = await asyncio.gather(*coroutines)
        return dfs


    async def export_csv(self):
        
        if await verify_amazon(self.base_url):
            return "I'm sorry, the link you provided is invalid. Could you please provide a valid Amazon link for the product category of your choice?"


        print(f"-----------------------| Welcome to Amazon {self.region}. |---------------------------------")
        await asyncio.sleep(2)
        print(f"Scraping and exporting to CSV.")
        searches = await self.category_name()
        print(f"Scraping category || {searches}.")

        categ_name = f"{self.region} - {searches}"
        url_lists = await self.crawl_url()
 
        print(f"The extraction process has begun and is currently in progress. The web scraper is scanning through all the links and collecting relevant information. Please be patient while the data is being gathered.")
        coroutines = [self.csv_sheet(url) for url in url_lists]
        dfs = await asyncio.gather(*coroutines)
        results = pd.concat(dfs)
        await export_sheet(results, categ_name)
        return results

