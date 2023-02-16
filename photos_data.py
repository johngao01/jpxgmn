# 获取所有写真和写真所有图片的数据
import os
from multiprocessing import Pool

from utils import get_organization_had_info
from utils import get_photos_info
from utils import write_photos_info_to_mysql


def get_all_data(url: str):
    org = url.split('/')[3]
    photos_info = get_photos_info(url, org)
    write_photos_info_to_mysql(photos_info)


def no_info_photos():
    no_info_urls = []
    had_info = get_organization_had_info(None)
    for file in os.listdir('files/txt/'):
        count = 0
        with open(f'files/txt/{file}', encoding='utf-8') as f:
            for line in f.readlines():
                url = line.strip().split('\t')[-1]
                if url not in had_info:
                    count += 1
                    no_info_urls.append(url)
        if count > 0:
            print(file[0:-4], count)
    return no_info_urls


if __name__ == '__main__':
    photos_url = no_info_photos()
    print(len(photos_url))
    with Pool(24) as p:
        p.map(func=get_all_data, iterable=photos_url)
