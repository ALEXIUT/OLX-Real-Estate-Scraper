import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import json
import concurrent.futures
import os
import pandas as pd
import re


def append_data(new_data, filename='urls.json'):
    # Read existing data
    with open(filename, 'r') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            data = []

    # Append new data
    data.append(new_data)

    # Write back to file
    with open(filename, 'w') as f:
        json.dump(data, f)


def get_urls(url_format):

    ua = UserAgent()
    headers = {'User-Agent': ua.chrome}

    with open("urls.json", "r") as f:
        file_content = f.read()
        if not file_content:
            existing_urls = []
        else:
            existing_urls = json.loads(file_content)

    found_urls = []
    skipped_urls = 0
    
    response = requests.get(url_format, headers=headers)
    soup = BeautifulSoup(response.text, 'lxml')

    pager = soup.find('div', {'data-testid': 'pagination-wrapper'})

    print("Found the pager!")
    print("Start parsing the pager...")

    last_page_li = pager.find_all('li', {'data-testid': 'pagination-list-item'})[-1]
    number_of_pages = int(last_page_li.text)
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
    
        for i in range(number_of_pages + 1):
            print(f"Processing page {i} out of {number_of_pages}")
        
            link = url_format.format(i)
            req = requests.get(link, headers=headers)
        
            soup = BeautifulSoup(req.text, 'lxml')

            existing_urls = [url for sublist in existing_urls for url in sublist]

            for div in soup.find_all('div', {'data-cy': 'l-card'}):
                link = div.find('a', href=True)
                if link:
                    link_url = 'https://www.olx.uz' + link.get('href')
        
                    if any(existing_url in link_url for existing_url in existing_urls):
                        skipped_urls += 1
                        print(f"Skipped the URL: {link_url}")
                        continue
        
                    found_urls.append(link_url) # `append_data` function will be called with `found_urls` as an argument        

            futures.append(executor.submit(get_urls, [link_url]))
        concurrent.futures.wait(futures)
        
    append_data(found_urls)
    
    print(f"Skipped URLs: {skipped_urls}")
    print(f"Stored URLs: {len(found_urls)}")



def get_data():

    with open('urls.json', 'r', encoding='UTF-8') as f:
        urls = json.load(f)
        print("Number of posts: ", len(urls))

    ua = UserAgent()
    headers = {'User-Agent': ua.chrome}

    data_list = []  # Initialize the list to store the data

    try:
        for url_index, url in enumerate(urls, start=1):
            parse_number = input(f"Enter how many URLs you want to parse (max {len(urls)}): ")
            if parse_number:
                parse_number = int(parse_number)
                if url_index > parse_number:
                    break
            

            print(f"Processing URL {url_index} out of {len(urls)}")  # Print the progress
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.text, 'lxml')
            
            posts = soup.find_all("div", id="hydrate-root")  # Find all divs with this id

            

            for post in posts:
                data = post.find_all('p', class_='css-b5m1rv er34gjf0')
                price_container = post.find('div', {'data-testid': 'ad-price-container'})
                price = price_container.find('h3', class_='css-12vqlj3').text if price_container else 'N/A'

                info = {'Price': price}
                for tag in data:
                    text = tag.text
                    split_text = re.split(r': ', text, 1)  # Split the text into category and value
                    if len(split_text) == 2:  # If the text could be split into two parts
                        category, value = split_text
                        info[category] = value  # Store the value in the dictionary under the category

                data_list.append(info)  # Append the data to the list

            print("Writing data to JSON file")  # Print a message before writing to the file
            with open('data.json', 'w', encoding='UTF-8') as f:
                json.dump(data_list, f, ensure_ascii=False)  # Write the data to the JSON file
            print("Data successfully written to JSON file")  # Print a message after writing to the file

    except Exception as e:
        print(f"An error occurred: {e}")


def main():


    if not os.path.isfile('urls.json'):
        with open('urls.json', 'w') as f:
            pass
        
    skip_url_parsing = input("Do you want to skip URL parsing? (y/n): ")
    
    if skip_url_parsing.lower() == 'n':
        url = 'https://www.olx.uz/nedvizhimost/?page={}'
        get_urls(url)
    elif skip_url_parsing.lower() == 'y':
        with open("urls.json", "r") as f:
            file_content = f.read()
            
        if not file_content:
            print("No URLs found. Please run URL scraping first.")
            return main()
        print("Skipped URL parsing!")        
    
    else:
        raise ValueError("Please enter 'y' or 'n'!")
    

    if not os.path.isfile('data.json'):
        with open('data.json', 'w', encoding='UTF-8') as f:
            pass

    get_data()
    
    
    
    

if __name__ == '__main__':
    main()