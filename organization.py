# 这个脚本用来获取网站上各个写真机构的每个写真的地址，并写入txt文件
import os
from multiprocessing import Pool

from lxml import etree

from utils import do_request
from utils import domain
from utils import write_org_photos
from utils import get_db


# 获取一页里所有的写真地址
def get_photos_urls(db, url, organ):
    photos_items = []
    response = do_request(url)
    response.encoding = 'utf-8'
    tree = etree.HTML(response.text)
    posts = tree.xpath("//div[@class='related_posts']//li[@class='related_box']")
    for item in posts:
        href = item[0].get('href').rstrip()
        post_date = str(item[1][0].text)
        title = item[0].get('title') or ''
        write_org_photos(db, organ, href, title, post_date)
        post_info = post_date + "\t" + href
        photos_items.append(post_info)
    if tree.xpath('/html/head/title')[0].text == '访问页面出错了':
        print(organ + '完成')
        return False
    else:
        return photos_items


def get_organization_all_photos(org):
    database = get_db()
    # 机构首地址
    base_url = f"{domain}/{org}/"
    # 用来保存写真的地址
    photos_urls = []
    # 得到了第一页所有写真的地址
    first_page = get_photos_urls(database, base_url, org)
    photos_urls.extend(first_page)
    print(base_url, len(photos_urls))
    count = 2
    while True:
        url = base_url + "page_" + str(count) + ".html"
        next_page = get_photos_urls(database, url, org)
        if isinstance(next_page, list):
            photos_urls.extend(next_page)
            print(url, len(photos_urls))
        else:
            with open(f"files/txt/{org}.txt", encoding='utf-8', mode='w') as fd:
                for item in photos_urls:
                    fd.write(item)
                    fd.write('\n')
            break
        count = count + 1
    database.close()


if __name__ == '__main__':
    with Pool(24) as p:
        p.map(func=get_organization_all_photos,
              iterable=['Xiuren', 'MyGirl', 'YouWu', 'IMiss', 'MiiTao', 'Uxing', 'FeiLin', 'MiStar', 'Tukmo', 'WingS',
                        'LeYuan', 'Taste', 'MFStar', 'Huayan', 'DKGirl', 'Candy', 'YouMi', 'MintYe', 'Micat', 'Mtmeng',
                        'HuaYang', 'XingYan', 'XiaoYu', 'Xgyw', 'Tuigirl', 'Ugirls', 'YouMei', 'BoLoli', 'Mtcos',
                        'Aiyouwu', 'Tgod', 'TouTiao', 'Girlt', 'Slady', 'Artgravia', 'DJAWA', 'Cosplay'])
    for file in os.listdir('files/txt/'):
        with open(f'files/txt/{file}', encoding='utf-8') as f:
            print(file, len(f.readlines()))
