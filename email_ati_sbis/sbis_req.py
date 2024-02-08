import requests

import requests
import re
import pandas as pd
from tqdm import tqdm
import time
import random

from urllib3 import HTTPSConnectionPool

print("Program Started")
start_time = time.time()

exists_df = pd.read_excel("./files/emails_ati_sbis.xlsx").to_dict("index")
exists_ati_ids = list(map(lambda x: x["atiId"], exists_df.values()))
print("Exists ATI:", len(exists_ati_ids))

df = pd.read_excel("ati_rus_04.12.2023.xlsx")
df["INN"] = df["INN"].fillna(0)

lst_df = list(df.to_dict("index").values())
print("Len of ATI:", len(lst_df))

ati_rus_lst = list(
    filter(
        lambda x: x["atiId"] not in exists_ati_ids, lst_df
    )
)
print("Filtered Len ATI:", len(ati_rus_lst))

base_url = "https://sbis.ru/contragents/{}"

headers_sbis = {
    "authority": "sbis.ru",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "max-age=0",
    "cookie": "lang=en; is_multitouch=true; tz=-360; s3reg=77; s3ds=1366%7C768%7C663%7C641%7C1366%7C728; adaptiveAspects=%7B%22isPhone%22%3Afalse%2C%22isTablet%22%3Afalse%2C%22isTouch%22%3Atrue%2C%22windowInnerWidth%22%3A663%2C%22windowInnerHeight%22%3A641%2C%22windowOuterWidth%22%3A1366%2C%22windowOuterHeight%22%3A728%2C%22containerClientWidth%22%3A663%2C%22containerClientHeight%22%3A641%2C%22isVertical%22%3Afalse%7D",
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}
# session = requests.Session()
# session.headers.update(headers_sbis)

data = []


pbar = tqdm(total=len(ati_rus_lst))
for i in ati_rus_lst:
    try:
        d = {}

        email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}\b"
        url = base_url.format(int(i["INN"]))
        res = requests.get(url, headers=headers_sbis)
        d["atiId"] = str(i["atiId"])
        d["sbis_url"] = url
        d["INN"] = str(int(i["INN"]))
        d["Email Address"] = ", ".join(set(re.findall(email_pattern, str(res.text))))

        data.append(d)
        
    # except HTTPSConnectionPool as e:
    #     print("INN:", i["INN"])
    #     print("Link:", url)
    #     print("Exception:", e)
    #     continue

    except Exception as e:
        print("INN:", i["INN"])
        print("Link:", url)
        print("Exception:", e)
        # continue
        break

    pbar.update()

pbar.close()

len(data)


df = pd.DataFrame(data=data)
writer = pd.ExcelWriter(
    "emails_ati_sbis_-3500.xlsx",
    engine="xlsxwriter",
    engine_kwargs={"options": {"strings_to_urls": False}},
)

df.to_excel(writer, index=False, freeze_panes=(1, 1))
writer.close()


end_time = time.time()
print("Execution Time: {:.3f} s".format(end_time - start_time))
