from bs4 import BeautifulSoup as bs
import pandas as pd
from urllib import *
import requests
import time
import random
from termcolor import colored
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")
import re


def get_url(limit=80):
    url1 = "https://www.immobiliare.it/vendita-case/milano/?criterio=rilevanza"
    urls = []
    for pages in range(limit):
        url = (
            "https://www.immobiliare.it/vendita-case/milano/?criterio=rilevanza&pag="
            + str(pages)
        )
        urls.append(url)

    urls.append(url1)
    return urls


# WEBSITE DECLARATION AND REQUEST
def get_all_announcements_urls(urls):
    print(colored("Fecthing all the announcements urls...", "blue", attrs=["bold"]))
    all_announcements_urls = []
    for index, url in enumerate(urls):
        if index % 10 == 0:
            print("  Page: ", index, " of ", len(urls), end="\r")
        try:
            response = requests.get(url)
            soup = bs(response.content, "html.parser")
            page_urls = soup.select(".in-card__title")
            page_urls = [url.get("href") for url in page_urls]
            all_announcements_urls.append(page_urls)

        except:
            print(colored("ERROR in", "red"))
            print(colored(url, "red"))
            pass

    all_announcements_urls = [url for page in all_announcements_urls for url in page]
    return all_announcements_urls


# GO TO EACH ANNOUNCEMENT AND GET INFO
def get_home_soup(url):
    response = requests.get(url)
    soup = bs(response.content)
    return soup, url


# GET PRICE
def get_price(soup):
    div2 = soup.select(".nd-list__item.in-feat__item.in-feat__item--main")
    return div2[0].get_text()


# GET INFORMATION ABOUT THE
def get_main_items(soup):
    main_items = soup.select(".nd-list__item.in-feat__item")
    items_label = ["price", "rooms", "m2", "bathrooms", "floor"]
    items_value = [item.get_text() for item in main_items]
    d_items_main = dict(zip(items_label, items_value))
    return d_items_main


# OTHER ITEMS
def get_other_items(soup):
    other_items = soup.select(".in-realEstateFeatures__list")
    items_label = ["description", "spese_condominio", "energy_class"]
    items_value = [item.get_text() for item in other_items]
    d_items_others = dict(zip(items_label, items_value))
    return d_items_others


# GET ALL ITEMS
def get_all_items(soup):
    all_items = soup.select(".in-realEstateFeatures__title")
    all_items_labels = [item.get_text() for item in all_items]
    all_values = soup.select(".in-realEstateFeatures__value")
    all_items_values = [item.get_text() for item in all_values]
    d_all = dict(zip(all_items_labels, all_items_values))
    return d_all


# ADDRESS
def get_address(soup):
    address = soup.select(".in-location")
    address = [a.get_text() for a in address]
    location_id = ["city", "neighborhood", "address"]
    d_location = dict(zip(location_id, address))
    return d_location


# CREATE PANDAS DATAFRAME
def make_dataframe(href):
    soup, url = get_home_soup(href)
    mergedDict = (
        get_main_items(soup)
        | get_other_items(soup)
        | get_all_items(soup)
        | get_address(soup)
    )
    df = pd.DataFrame(mergedDict, index=[0])
    df["href"] = url
    return df


def read_parquet():
    return pd.read_parquet(
        Path(".").resolve() / "Data" / "Raw" / "milano_housing_price_raw.parquet.gzip"
    )


def find_new_announcements(df, all_announcements_urls):
    href_done = df["href"].tolist()
    diff = list(set(all_announcements_urls).difference(set(href_done)))
    return diff


def main():
    sleep = random.randint(1, 10) / 10

    df = read_parquet()
    href = get_url()
    all_announcements_urls = get_all_announcements_urls(href)
    diff = find_new_announcements(df, all_announcements_urls)

    if diff == 0:
        print(colored("No new data to scrape. Try tomorrow", "yellow"))
        pass

    else:
        print(colored(f"Found {len(diff)} new announcements to scrape", "green"))

        df_update = pd.DataFrame()
        for index, url in enumerate(diff):
            ads_info = make_dataframe(url)
            df_update = pd.concat([df_update, ads_info], axis=0)
            time.sleep(sleep)
            print(f"  Scraped {index}/{len(diff)} webpage", end="\r")

        new_listings = df_update

        df_update = pd.concat([df, df_update], axis=0)
        df_update.to_parquet(
            Path(".").resolve()
            / "Data"
            / "Raw"
            / "milano_housing_price_raw.parquet.gzip",
            compression="gzip",
        )
        if len(diff) != 0:
            print(
                colored(
                    f"Saved {len(diff)} more annoucements\n", "green", attrs=["bold"]
                )
            )

    return new_listings


# %%
