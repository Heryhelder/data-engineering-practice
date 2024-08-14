import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
import glob

def main():
    BASE_URL = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
    BASE_PATH = os.path.join(os.path.curdir, 'downloads')

    search_date = '2024-01-19 09:51' #'2022-02-07 14:03'
    content = ''
    files_links = []
    
    response = requests.get(BASE_URL)

    if response.status_code == 200:
        content = response.content

        soup = BeautifulSoup(content, 'html.parser')

        table = soup.find('table')

        table_rows = table.find_all('tr')
        
        if table_rows is not None:
            files_links = [BASE_URL + row.find_all('td')[0].text.strip() for row in table_rows if len(row.find_all('td')) >= 2 and row.find_all('td')[1].text.strip() == search_date]

    if files_links is not None:
        for file_link in files_links:
            with requests.get(file_link) as response:
                if response.status_code == 200:
                    if not os.path.exists(BASE_PATH):
                        os.mkdir(BASE_PATH)

                    with open(BASE_PATH + '/' + os.path.basename(file_link), 'wb') as f:
                        f.write(response.content)

    csv_files = glob.glob(os.path.join(BASE_PATH, '/*.csv'))

    df_list = []

    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        df_list.append(df)

    if len(df_list) > 0:
        df = pd.concat(df_list, ignore_index=True)

        df['HourlyDryBulbTemperature'] = pd.to_numeric(df['HourlyDryBulbTemperature'], errors='coerce')
        
        print(df['HourlyDryBulbTemperature'].max())
        print(df.loc[df['HourlyDryBulbTemperature'].max() == df['HourlyDryBulbTemperature']]['HourlyDryBulbTemperature'])
    else:
        print('No files found')
        
if __name__ == "__main__":
    main()
