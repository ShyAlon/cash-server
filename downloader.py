import urllib2
import time
import getopt
import sys
import json
import csv
import os
from multiprocessing import Process, Queue
from db import Database

API_KEY = '114e933ff74d41b9f4bddeeb74c81ccd&symbol'
ITERATIONS = 5
CHUNK_SIZE = 100
SAMPLES = 98


def get_data(url, finished, counter):
    try:
        finished.put(urllib2.urlopen(url).read().strip())
    except Exception:
        print(0.1)
        print("failed retrieving url {}".format(counter))
        finished.put(0)
    return 0


def define_url():
    fields = ''
    symbolList = []
    fieldList = []

    with open('resources/field_list.csv', 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            fieldList.append(row[0])

    for i in range(0, len(fieldList)):
        fields = fields + fieldList[i] + ','

    with open('resources/snp_constituents.csv', 'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            symbolList.append(row[0])

    symbols = ['', '', '', '', '']
    chunk_size = len(symbolList) / ITERATIONS
    for i in range(0, chunk_size):
        for offset in range(0, ITERATIONS):
            symbols[offset] = symbols[offset] + symbolList[offset * chunk_size + i] + ','

    urls = []
    for offset in range(0, ITERATIONS):
        suffix = (API_KEY, symbols[offset], fields)
        urls.append('http://marketdata.websol.barchart.com/getQuote.json?apikey=%s&symbols=%s&fields=%s' % suffix)

    return urls, symbolList

def is_res_success(res):
    if isinstance(res, basestring):
        return res.lower().find("success") > -1
    else:
        return (int(res) == 200)

def create_files(urls, symbolList):
    stock_data = []
    stock_data_db = {}
    time_stamp = time.time()
    runTime = time.strftime("%Y%m%d-%H%M%S")
    sample = 0
    while sample < SAMPLES:
        time_stamp = time.time()
        finished = Queue()
        processes = []
        for i in range(ITERATIONS):
            p = Process(target=get_data, args=(urls[i], finished, i))
            p.start()
            processes.append(p)
            time.sleep(1)

        counter = 1
        while finished.qsize() < ITERATIONS:
            if counter % 10 == 0:
                print("waiting for {} members to finish. Counter {}".format(ITERATIONS - finished.qsize(), counter))
            counter += 1
            time.sleep(1)

        del stock_data[:]
        for i in range(0, ITERATIONS):
            stock_data.append(finished.get())

        for process in processes:
            process.terminate()

        jsons = []
        for i in range(0, ITERATIONS):
            jsons.append(json.loads(stock_data[i]))

        results = []
        status = 0
        for i in range(0, ITERATIONS):
            res = jsons[i]["status"].values()[1]
            if is_res_success(res):  # (res == 'Success') or (int(res) != 200):
                print("Got value #{} for sample {}: {}".format(i, sample, jsons[i]["status"].values()[1]))
                status += 1

        if status > 0:
            continue
        else:
            for i in range(0, ITERATIONS):
                results.append(jsons[i]["results"])

        if sample == 0:
            for j in range(0, len(symbolList)):
                stock_data_db[symbolList[j].replace(".", "_")] = [results[0][0].keys()]
        mydb = Database()
        data_type = "full_single_sample"
        runTime = time.strftime("%Y%m%d-%H%M%S")

        for i in range(0, len(results[0])):
            for j in range(0, ITERATIONS):
                if i == 0 and j == 0:
                    print(results[j][i].values())
                stock_data_db[results[j][i].values()[23].replace(".", "_")] = results[j][i].values()

        ret_val = {"data_type": data_type, "date_and_time": runTime, "time_stamp": time_stamp, "rows": stock_data_db}
        mydb.insert_result(ret_val)
        sample += 1
        print(0.4)
        print(sample)
        time.sleep(280)
        print(0.5)
        print(sample)

    # mydb = Database()
    data_type = "raw_stock_data"
    ret_val = {"data_type": data_type, "date_and_time": runTime, "time_stamp": time_stamp, "rows": stock_data_db,
               "samples": SAMPLES}
    # mydb.insert_result(retVal)
    print(0.6)
    print("done")

    return time_stamp, runTime, ret_val


def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "vhc:", ["help", "characters="])
    except getopth.GetoptError, err:
        print(0.7)
        print(str(err))
        sys.exit(2)
    urls, symbolList = define_url()
    create_files(urls, symbolList)


if __name__ == "__main__":
    main()
