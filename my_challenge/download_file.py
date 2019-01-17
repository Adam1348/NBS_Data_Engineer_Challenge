import requests
import sys
import os
import csv
import gzip
from io import BytesIO
from datetime import datetime, timedelta


def pre_delta(start, end, delta):
    curr = start
    while curr <= end:
        yield curr
        curr += delta


def url_generator(args):
    """
    python download_file.py 2012/1/1-0 2012/1/1-3
    :param args: strings starttime, endtime
    :return: generator object
    """

    start = datetime.strptime(args[0], '%Y/%m/%d-%H')
    end = datetime.strptime(args[1], '%Y/%m/%d-%H')

    urls_out = []
    for date in pre_delta(start, end, timedelta(hours=1)):

        url_temp = 'http://dumps.wikimedia.org/other/pagecounts-raw/{}/{}-{:02d}/pagecounts-{}-{:02d}0000.gz'.\
            format(date.year, date.year, date.month, date.strftime('%Y%m%d'), date.hour)

        urls_out.append(url_temp)

    return urls_out


def file_reader(file_object):
    """
    Generator to read a file line by line.
    """
    while True:
        try:
            data = file_object.readline()
            if not data:
                break
            yield data
        except UnicodeDecodeError:
            continue


def clean_helper(row, time):
    data = row.split() + [datetime.strptime(time, '%Y%m%d%H').strftime('%Y/%m/%d-%H')]
    if len(data) != 5:
        return

    if sum(x in data[1] for x in ['Special', 'User', 'File']):
        return

    if '.' in data[0]:
        data[0] = data[0].split('.')[0]

    return data


def file_cleaner(path, time):
    """
    Clean the downloaded file, convert it to csv and delete the original file
    language: split, take only language part
    drop whole row if there is null value
    Add date time
    """

    print('Working on file: {}'.format(path))

    m = open(path, encoding="utf8")

    with open(path + '.csv', 'w', newline='', encoding='utf-8') as out:
        writer = csv.writer(out)
        writer.writerow(['Language', 'Page', 'View_count', 'Bytes_transferred', 'Data_time'])

        for line in file_reader(m):
            data = clean_helper(line, time)

            if data:
                writer.writerow(data)

    m.close()
    os.remove(path)
    return


def download_helper(url):
    req = requests.get(url)

    if not req.ok:
        # print('Bad response from {}.'.format(url))
        return

    return req


def downloader(urls, file_path):
    for url in urls:
        print('Downloading file: {}'.format(url))
        local_filename = url.split('/')[-1][:-3]

        req = download_helper(url)

        if req:
            with open(file_path + '/' + local_filename, 'wb') as f:
                f.write(gzip.GzipFile(fileobj=BytesIO(req.content)).read())

            curr_time = ''.join(local_filename.split('-')[1:])[:-4]
            file_cleaner(file_path + '/' + local_filename, curr_time)
        else:
            print('Bad response from {}.'.format(url))

    return


if __name__ == '__main__':

    url_s = url_generator(sys.argv[1:])
    folder_name = 'wiki_page_view_count'
    path_to_file = os.path.abspath(os.path.join(os.getcwd(), os.pardir)) + '/' + folder_name
    # path_to_file = os.getcwd() + '/' + folder_name

    if not os.path.exists(path_to_file):
        os.mkdir(path_to_file)

    downloader(url_s, path_to_file)

    print('Completed')
