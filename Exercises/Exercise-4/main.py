import glob
import csv
import json
import os
from flatten_json import flatten

def main():
    BASE_PATH = os.path.join(os.path.curdir, 'data')
    files = [file for file in glob.iglob(BASE_PATH + '/**/*.json', recursive=True) if os.path.isfile(file)]

    for file in files:
        json_value = json.load(open(file))

        with open(file + '.csv', 'w') as csv_file:
            writer = csv.writer(csv_file)

            writer.writerow(flatten(json_value).keys())
            writer.writerow(flatten(json_value).values())

if __name__ == "__main__":
    main()
