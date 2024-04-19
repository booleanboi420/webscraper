from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import sqlite3
import requests
import pandas as pd
import time
import random
import re
import traceback


rental_data = pd.DataFrame(columns=['Index', 'Rent Price', 'Square Meter', 'Address',  'URL'])


def scrape_listing_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        driver = webdriver.Chrome()
        try:
            driver.get(url)
            try:
                # Try and click the accept cookies button
                accept_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "didomi-notice-agree-button"))
                )
                accept_button.click()
            except:
                pass  # No cookie banner found or button not clickable
            
            # Scroll slowly
            section_height = driver.execute_script("return window.innerHeight;")
            total_height = driver.execute_script("return document.body.scrollHeight;")
            num_sections = total_height // section_height
            for section in range(num_sections):
                driver.execute_script(f"window.scrollTo(0, {section * section_height});")
                time.sleep(0.5)
            
            # Wait for it all to load
            WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, 'div')))
            
            # Pass it to beautifulsoup
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Rent price
            rent_price_element = soup.find('span', class_='Text-sc-10o2fdq-0 fisxbM', attrs={'data-testid': 'contact-box-price-box-price-value-0'})
            if rent_price_element:
                rent_price_parts = rent_price_element.text.replace('€', '').replace('ab', '').strip().replace('.', '').split(',')
                rent_price = int(rent_price_parts[0])
            else:
                rent_price = None

            # Square meter
            square_meter_element = soup.find('div', string=lambda text: text and 'm²' in text)
            if square_meter_element:
                square_meter = re.search(r'([\d\.,]+)', square_meter_element.text.strip()).group()
                square_meter = float(square_meter.replace(',', '.'))
            else:
                square_meter = None

            # Address
            address_element = soup.find('div', class_='Box-sc-wfmb7k-0', attrs={'data-testid': 'object-location-address'})
            address = address_element.text.strip() if address_element else "None"
            
            return rent_price, square_meter, address

        finally:
            driver.quit()
    else:    
        print(f"We have encountered an error when scraping {url}")
        return None, None, None


def scrape_and_append_multiple_listings(urls, delay=1, maximum = 0):
    num_scraped = 0
    for index, url in enumerate(urls, start=1):
        if maximum is not None and num_scraped >= maximum:
            break
        rent_price, square_meter, address = scrape_listing_data(url)
        time.sleep(delay + random.uniform(-0.5, 0.5))
        if rent_price is not None:
            append_to_dataframe(index, rent_price, square_meter, address, url)
        
        num_scraped += 1
        print(f"Listing number {index} scraped successfully")
        time.sleep(delay + random.uniform(-0.5, 0.5))


def append_to_dataframe(index, rent_price, square_meter, address, url):
    global rental_data
    rental_data.loc[len(rental_data)] = [index, rent_price, square_meter, address, url]


def get_listing_urls(base_url="https://www.willhaben.at/iad/immobilien/mietwohnungen/wien", num_pages=1):
    try:
        driver = webdriver.Chrome()
        listing_urls = []
        for page in range(1, num_pages + 1):
            url = f"{base_url}?page={page}"
            driver.get(url)
            
            # Try and click the accept cookies button
            try:
                accept_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.ID, "didomi-notice-agree-button"))
                )
                accept_button.click()
            except:
                pass  # No cookie banner found or button not clickable
            
            # Scroll slowly
            section_height = driver.execute_script("return window.innerHeight;")
            total_height = driver.execute_script("return document.body.scrollHeight;")
            num_sections = total_height // section_height
            for section in range(num_sections):
                driver.execute_script(f"window.scrollTo(0, {section * section_height});")
                time.sleep(0.5)
            
            # Wait for the links
            WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.TAG_NAME, 'a')))
            
            # Scrape with bs
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Find all anchor tags with href attribute
            listing_links = soup.find_all('a', href=True)
            
            # Extract and append listing URLs
            for link in listing_links:
                href = link['href']
                if '/iad/immobilien/d/mietwohnungen/wien/' in href:
                    full_url = f"https://www.willhaben.at{href}"
                    listing_urls.append(full_url)
        
        print(f"I just scraped {len(listing_urls)} urls")
        return listing_urls
    finally:
        driver.quit()

def extract_plz(address):
    # Searches a 4 digit number starting with 1 ending with 0
    plz_candidate = re.search(r'\b1\d{2}0\b', address)
    if plz_candidate:
        return plz_candidate.group()
    return None

def append_to_sql(db, table, data):
    # Connect to the database
    conn = sqlite3.connect(db)

    data_without_index = data.drop(columns=['Index'])

    existing_urls = pd.read_sql(f"SELECT URL FROM {table}", conn)['URL']
    new_data = data_without_index[~data_without_index['URL'].isin(existing_urls)]

    new_data.to_sql(table, conn, if_exists='append', index=False)

    conn.commit()
    print(f"Succesfully committed {len(new_data)} rows to the SQL database")
    conn.close()


def main():
    base_url = 'https://www.willhaben.at/iad/immobilien/mietwohnungen/wien'
    num_pages = 1
    delay = 5
    maximum = 1
    try:
        listing_urls = get_listing_urls(base_url, num_pages)
        scrape_and_append_multiple_listings(listing_urls, delay, maximum)
        rental_data['PLZ'] = rental_data['Address'].apply(extract_plz)

        print(rental_data)
        append_to_sql("rental_data.db", "rental_data", rental_data)

    except Exception as e:
            print(f"An error occurred: {str(e)}")
            traceback.print_exc()


if __name__ == "__main__":
    main()