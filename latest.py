# 获取最新的写真和写真所有图片的数据
import os
import sys
from multiprocessing import Pool, Manager, cpu_count
from utils import *


def add(db, url, data, logger, organ):
    pre_add = data['organization_add']
    response = do_request(url)
    response.encoding = 'utf-8'
    tree = etree.HTML(response.text)
    posts = tree.xpath("//div[@class='related_posts']//li[@class='related_box']")
    for item in posts:
        href = item[0].get('href').rstrip()
        post_date = str(item[1][0].text)
        title = item[0].get('title') or ''
        info = post_date + "\t" + href
        if href not in data['had_info']:
            data['photos_urls'].insert(data['organization_add'], info)
            data['organization_add'] += 1
            data['new_photos_url'].append(href)
            write_org_photos(db, organ, href, title, post_date)
            logger.info(str(data['organization_add']) + " " + href + " " + title)
    new_add = data['organization_add'] - pre_add
    if new_add == len(posts):
        return True
    else:
        return False


def get_photos_data(photos):
    photos_info = get_photos_info(*photos)
    write_photos_info_to_mysql(photos_info)


def get_org_latest_photos_url(org, all_new_photos, locks):
    database = get_db()
    logger = log2file(org, f'files/logs/{org}', ch=True, mode='a')
    now_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    logger.info(f"获取 {org} 截至到 {now_time} 新增的写真")
    # 获取这个机构最新的写真url
    data = {'organization_add': 0,
            'photos_urls': [line.strip() for line in open(f'files/txt/{org}.txt', encoding='utf-8').readlines()],
            'base_url': domain + "/" + org,
            'had_info': get_organization_had_info(org),
            'new_photos_url': []}
    add(database, data['base_url'], data, logger, org)
    if data['organization_add'] == 0:
        logger.info(f"{org}今天没有新增写真")
        return
    page = 2
    while True:
        next_url = data['base_url'] + "/page_" + str(page) + ".html"
        flag = add(database, next_url, data, logger, org)
        if flag:
            page += 1
        else:
            break
    logger.info(f"{org}新增了{data['organization_add']}个写真")
    # 将最新的写真url写入txt
    with open(f'files/txt/{org}.txt', mode='w', encoding='utf-8') as f:
        for url in data['photos_urls']:
            f.write(url)
            f.write('\n')
    with locks:
        for photos in data['new_photos_url']:
            all_new_photos.append([photos, org])
    database.close()


if __name__ == '__main__':
    p = Pool(20)
    all_latest_photos_url = Manager().list()
    lock = Manager().Lock()
    for file in os.listdir('files/txt'):
        # get_org_latest_photos_url(file[0:-4], all_latest_photos_url, lock, )
        p.apply_async(func=get_org_latest_photos_url, args=(file[0:-4], all_latest_photos_url, lock,))
    p.close()
    p.join()
    nums = len(all_latest_photos_url)
    if nums == 0:
        sys.exit(0)
    process_num = cpu_count() if nums > cpu_count() else nums
    p = Pool(process_num)
    print(f"开始获取{nums}个写真的详细数据，开启 {process_num} 个进程")
    for photos_url in all_latest_photos_url:
        # get_photos_data(photos_url)
        p.apply_async(get_photos_data, args=(photos_url,))
    p.close()
    p.join()
