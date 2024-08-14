import boto3
import gzip
import os

def main():
    BUCKET_NAME = 'commoncrawl'
    
    PATH_FILE_KEY = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
    PATH_FILE_NAME = 'wet.paths.gz'

    FINAL_FILE_URI = ''
    FINAL_FILE_NAME = ''

    s3 = boto3.client('s3')
    s3.download_file(BUCKET_NAME, PATH_FILE_KEY, PATH_FILE_NAME)

    with gzip.open('wet.paths.gz', 'rt') as f:
        FINAL_FILE_URI = f.readline().strip()
        FINAL_FILE_NAME = os.path.basename(FINAL_FILE_URI)

    s3.download_file(BUCKET_NAME, FINAL_FILE_URI, FINAL_FILE_NAME)

    with gzip.open(FINAL_FILE_NAME, 'rt') as f:
        for line in f:
            print(line)

if __name__ == "__main__":
    main()