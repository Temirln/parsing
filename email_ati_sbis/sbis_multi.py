import multiprocessing
import re

import pandas as pd
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from tqdm import tqdm
import traceback

# from custom_utils.scrap_utils import MAX_RETRIES, logger

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

session = requests.Session()

# log = logger()

def parser_data(page):

    
    try:
        email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}\b"
        url, ati = page
        link_res = session.get(url, verify=False)
        ati["Email Adresses"] = ", ".join(set(re.findall(email_pattern, str(link_res.text))))
        return ati
    except Exception as e:
        print(traceback.format_exc())
        print(f"Exception occured in {url}, error: {e}")
        # log.error(f"Exception occured in {url}, error: {e}")


def main(atis, output_file):
    base_url = "https://sbis.ru/contragents/{}"
    
    # all_links = [print(ati.get("INN")) for ati in atis]
    all_links = [(base_url.format(int(ati.get("INN"))),ati) for ati in atis]
    print("!!! Parsing of links !!!")
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        results = list(
            tqdm(pool.imap_unordered(parser_data, all_links), total=len(all_links))
        )


    print("!!! Saving data to files xlsx and csv !!!")
    df = pd.DataFrame(data=results)

    writer = pd.ExcelWriter(
        f"{output_file}.xlsx",
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_urls": False}},
    )

    df.to_excel(writer, index=False, freeze_panes=(1, 1))
    writer.close()
    # df.to_csv(f"./{output_file}.csv", index=False)
    print("!!! Parsing Completed !!!")


if __name__ == "__main__":
    import requests

    output_file = "ati_rus"
    df = pd.read_excel(r"D:\Job\Prometeo\ati.su\xlsx_files\ati_rus_04.12.2023.xlsx")
    df['INN'] = df['INN'].fillna(0)
    ati_rus = list(df.to_dict('index').values())

    # print(rest_ati_ids)
    print("Len of ATI:", len(ati_rus))

    main(ati_rus, output_file)

