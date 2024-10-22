import polars as pl
import requests
from concurrent.futures import ThreadPoolExecutor
from tqdm.auto import tqdm
import constants


def scraping(urls, index):
    df = pl.DataFrame(schema={"html": pl.Utf8, "url": pl.Utf8})

    for url in tqdm(urls, leave=False):
        try:
            raw_html = requests.get(
                url, cookies=constants.COOKIES, headers=constants.HEADERS
            )
            html_string = raw_html.content
            df.vstack(
                pl.DataFrame(data={"html": html_string.decode("utf-8"), "url": url}),
                in_place=True,
            )
        except requests.TooManyRedirects:
            continue
        except requests.exceptions.ConnectionError:
            continue
        except requests.exceptions.MissingSchema:
            continue
        except requests.exceptions.InvalidURL:
            continue

    df.write_parquet(f"data/scraped/nettavisen/{index}.pq")


url_list = pl.read_parquet("data/sitemaps/nettavisen_sites.parquet").to_dict()

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(scraping, url_list["sites"], url_list["index"])

""" Hente sitemap til nettavisen
year = [[i for i in range(1,366)], [i for i in range(1,366)], [i for i in range(1,366)], [i for i in range(1,365)], [i for i in range(1,366)], 2023]
days = [range(1,32),range(1,29),range(1,32),range(1,31),range(1,32),range(1,31),range(1,32),range(1,32),range(1,31),range(1,32),range(1,31),range(1,32)]
months = [0, 1, 2, 3, 4, 5,6, 7, 8, 9, 10, 11]

sitemaps = []
year_num = 2018
for year in tqdm(year):
    if year != 2023:
        print(year_num)
        for day in tqdm(year, leave=False):
            try:
                url = f"https://www.nettavisen.no/templates/v1/resources/sitemap/?date={day}.01.{year_num}]"
                r = requests.get(url)
                root = etree.fromstring(r.content)
                try:
                    sitemaps.append([i[0].text for i in root])
                except IndexError:
                    continue
            except etree.XMLSyntaxError:
                continue
            except requests.exceptions.ConnectionError:
                continue
        year_num += 1


    elif year == 2023:
        for month in tqdm(months, leave=False):
            for day in days[month]:
                try:
                    r = requests.get(f"https://www.nettavisen.no/templates/v1/resources/sitemap/?date={day}.{month}.2023")
                    root = etree.fromstring(r.content)
                    try:
                        sitemaps.append([i[0].text for i in root])
                    except IndexError:
                        continue
                except etree.XMLSyntaxError:
                    continue
                except requests.exceptions.ConnectionError:
                    continue
        year_num += 1
    else:
        break
"""
