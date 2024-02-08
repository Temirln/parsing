import requests
import re
import pandas as pd
from tqdm import tqdm
import time
import random



df = pd.read_excel(
    r"D:\Job\Prometeo\ati.su\xlsx_files\ati_rus_04.12.2023.xlsx"
)
df['INN'] = df['INN'].fillna(0)

ati_rus_lst = list(df.to_dict("index").values())
print("Len of ATI:", len(ati_rus_lst))


start_time = time.time()

pbar = tqdm(total=len(ati_rus_lst))
base_url = "https://sbis.ru/contragents/{}"

with open("http_proxies.txt",'r') as f:
    proxy = f.read().splitlines()
    




headers = {
    'authority': 'sbis.ru',
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-US,en;q=0.9", 
    "cache-control": "max-age=0",
    "cookie": "lang=en; is_multitouch=true; tz=-360; s3reg=77; s3ds=1366%7C768%7C811%7C641%7C1366%7C728; adaptiveAspects=%7B%22isPhone%22%3Afalse%2C%22isTablet%22%3Afalse%2C%22isTouch%22%3Afalse%2C%22windowInnerWidth%22%3A811%2C%22windowInnerHeight%22%3A641%2C%22windowOuterWidth%22%3A1366%2C%22windowOuterHeight%22%3A728%2C%22containerClientWidth%22%3A811%2C%22containerClientHeight%22%3A641%2C%22isVertical%22%3Afalse%7D", 
    "sec-ch-ua": "\"Not_A Brand\";v=\"8\", \"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\"",
    "sec-ch-ua-mobile": "?0", 
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "document", 
    "sec-fetch-mode": "navigate", 
    "sec-fetch-site": "same-origin", 
    "sec-fetch-user": "?1", 
    "upgrade-insecure-requests": "1", 
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

for i in range(len(ati_rus_lst)):
    p = random.choice(proxy)
    proxies = {
        'http': f'http://{p}',
        'https': f'https://{p}'
    }
    
    email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}\b"
    
    res = requests.get(base_url.format(int(ati_rus_lst[i].get("INN"))), proxies=proxies, headers = headers)
    ati_rus_lst[i]["Email Adresses"] = ", ".join(set(re.findall(email_pattern, str(res.text))))
    pbar.update()

pbar.close()

end_time = time.time()
print("Execution Time: {:.3f} s".format(end_time - start_time))



df = pd.DataFrame(data=ati_rus_lst)
writer = pd.ExcelWriter(
    "ati.xlsx",
    engine="xlsxwriter",
    engine_kwargs={"options": {"strings_to_urls": False}},
)

df.to_excel(writer, index=False, freeze_panes=(1, 1))
writer.close()