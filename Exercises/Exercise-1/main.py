import aiohttp
import asyncio
import os
from zipfile import ZipFile

download_uris = [
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip'
]

async def main():
    BASE_PATH = os.path.join(os.path.curdir, 'downloads')

    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH)

    async with aiohttp.ClientSession() as session:
        for uri in download_uris:
            async with session.get(uri) as res:
                if res.status == 200:
                    file_name = uri.split('/')[3]

                    with open(BASE_PATH + '/' + file_name, 'wb') as f:
                        async for chunk in res.content.iter_chunked(1024):
                            f.write(chunk)

                    await extract_csv(BASE_PATH + '/' + file_name, BASE_PATH)

async def extract_csv(file, dest):
    if os.path.exists(file):
        with ZipFile(file, 'r') as z:
            z.extractall(dest)
            os.remove(file)

if __name__ == "__main__":
    asyncio.run(main())