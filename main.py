from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd
import concurrent.futures
import os

headers = {
"User-Agent": os.environ.get("User-Agent"),
"Accept-Language": os.environ.get("Accept-Language")
}


URL = "https://nextspaceflight.com/launches/past/?page=1&search="

a = np.array([["Organisation", "Location", "Datetime", "Details", "Price", "Status", "Mission_Status"]])


class DataBot:
    def __init__(self):

        self.data = []

    def get_data(self, pages):
        for page in pages:

            content = requests.get(f"https://nextspaceflight.com/launches/past/?page={page}&search=", headers=headers).text
            soup = BeautifulSoup(content, "html.parser")

            table = soup.select(selector=".mdl-cell")

            for data in table:

                columns = []

                # organisation
                columns.append(data.find(class_="mdl-card__title-text").text.strip())

                loc_time = data.find(class_="mdl-card__supporting-text").text.strip().split("\n")

                columns.append(loc_time[-1].strip())
                columns.append(loc_time[0].strip())

                # details
                columns.append(data.find(name="h5").text.strip())

                url = data.find("button")["onclick"].split("'")[1]
                response = requests.get(f"https://nextspaceflight.com{url}"  , headers=headers).text
                new_soup = BeautifulSoup(response, "html.parser")

                text = new_soup.find_all(class_="mdl-card__supporting-text")[1]

                price = ([item.text.split(": ")[1] for item in text.find_all_next(class_="mdl-cell") if "Price" in item.text])
                status = ([item.text.split(": ")[1] for item in text.find_all_next(class_="mdl-cell") if "Status" in item.text])

                columns.append(price[0] if bool(price) else "") # if price is not found then ""
                columns.append(status[0])

                # Mission Status
                columns.append(new_soup.find(class_="status").text.strip())
                self.data.append(columns)


    def get_total_pages(self) -> int:
        content = requests.get(URL, headers=headers).text
        soup = BeautifulSoup(content, "html.parser")
        buttons = soup.find_all("button")
        a : str = buttons[-1]["onclick"]
        return int(a.split("page=")[-1].split("&")[0])


bots = []
for _ in range(0, 60):
    bots.append(DataBot())

# to get the total number of pages
pages = np.arange(start=1, stop=bots[0].get_total_pages() + 1, step=1)

# to boost up the data collection
with concurrent.futures.ThreadPoolExecutor(max_workers=60) as executor:
    for bot, page in zip(bots, np.array_split(pages, 60)):
        executor.submit(bot.get_data, page)

# Merge the data collected from each bot
for bot in bots:
    a = np.append(a, bot.data, axis=0)

# Save the data as a CSV
df = pd.DataFrame(a)
df.to_csv("SpaceData.csv", header=False)
