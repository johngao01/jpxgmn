# 小工具
import logging
import re
import time
from typing import Optional
import cx_Oracle
import requests
import urllib3
from lxml import etree
from retrying import retry

urllib3.disable_warnings()
header = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                        '(KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'}

domain = "https://www.xgmn02.com"


def get_organization_had_info(organization: Optional[str, None]):
    database_had_info = []
    oracle_db = get_db()
    cursor = oracle_db.cursor()
    if organization:
        sql = f"select url from photos_data where organ='{organization.lower()}'"
    else:
        sql = 'select url from photos_data'
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        # 已经获取过写真数据的写真地址
        database_had_info = [photos[0] for photos in result]
    except cx_Oracle.Error:
        pass
    finally:
        oracle_db.close()
        return database_had_info


# 创建各个写真机构的数据表
def get_db():
    dsn_tns = cx_Oracle.makedsn('localhost', '1522', service_name='orcl')
    conn = cx_Oracle.connect(user='jpxgmn', password='123456', dsn=dsn_tns)
    return conn


@retry(stop_max_attempt_number=10)
def do_request(url: str, stream=False):
    try:
        response = requests.get(url, stream=stream,
                                headers=header, verify=False)
        return response
    except Exception as e:
        print(e, url)
        time.sleep(20)
        raise e


def log2file(name, filename, ch=False, mode='a', is_time=False):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    filepath = f'{filename}.log'
    handler = logging.FileHandler(filepath, mode=mode, encoding='utf-8')
    handler.setLevel(logging.NOTSET)
    if is_time:
        fours = logging.Formatter("%(asctime)s - %(message)s")
    else:
        fours = logging.Formatter("%(message)s")
    handler.setFormatter(fours)
    if ch:
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(fours)
        logger.addHandler(ch)
    logger.addHandler(handler)
    return logger


def get_photos_info(photos_url, organization):
    # 保存图片的网址
    count = 1
    photo_infos = {}
    photos = []
    url = domain + photos_url
    response = do_request(url)
    if response.status_code == 404:
        return False
    response.encoding = 'utf-8'
    tree = etree.HTML(response.text)
    photo_infos['url'] = photos_url
    star = tree.xpath("//span[@class='item item-2']/a")[0].text
    if star is None:
        photo_infos['star'] = 'unknown'
    else:
        photo_infos['star'] = star.strip()
    title = tree.xpath('//h1[@class="article-title"]')[0].text
    photo_infos['title'] = re.sub('[\\\\/:*?"<>|\n]', '', title)
    photo_infos['organization'] = organization.lower()
    urls = tree.xpath("//p[@style='text-align: center']/img")
    srcs = []
    db = get_db()
    for pic_url in urls:
        src = str(pic_url.get('src'))
        srcs.append(src)
        write_photo_info_to_mysql(database=db, src=src, page=0, page_url=photos_url,
                                  photos_info=photo_infos, organization=organization)
    photos.extend(srcs)
    print(url, count, len(photos))
    while True:
        srcs = []
        next_url = url[0:-5] + "_" + str(count) + ".html"
        response = do_request(next_url)
        response.encoding = 'utf-8'
        tree = etree.HTML(response.text)
        if response.status_code == 404 or tree.xpath('/html/head/title')[0].text == '访问页面出错了':
            break
        urls = tree.xpath("//p[@align='center']/img")
        for pic_url in urls:
            src = str(pic_url.get('src'))
            srcs.append(src)
            write_photo_info_to_mysql(database=db, src=src, page=count, page_url=next_url,
                                      photos_info=photo_infos,
                                      organization=organization)
        photos.extend(srcs)
        print(next_url, count, len(photos))
        count += 1
    photo_infos['photos_nums'] = len(photos)
    photo_infos['url_pages_nums'] = count
    photo_infos['src'] = ','.join(photos)
    db.close()
    return photo_infos


def write_org_photos(db, org, p_url, title, p_date):
    sql = "insert into organ_data (organ, photos_url, title, up_date) values (:1,:2,:3,:4)"
    try:
        db.cursor().execute(sql, (org, p_url, title, p_date))
        db.commit()
    except cx_Oracle.Error:
        db.rollback()


def write_photo_info_to_mysql(database, src, page, page_url, photos_info, organization: str):
    cursor = database.cursor()
    star_path = r"/{}/{}".format(photos_info['star'], photos_info['title'])
    org_path = r"/{}/{}".format(organization, photos_info['title'])
    sql = "insert into photo (src, html_page, page_url, photos_url, star, organ, photos_title, " \
          "star_path, org_path, file_status) values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)"
    try:
        cursor.execute(sql, (
            src, page, page_url, photos_info["url"],
            photos_info["star"], organization, photos_info["title"], star_path, org_path, 0
        ))
        database.commit()
    except cx_Oracle.Error:
        database.rollback()
    finally:
        cursor.close()


def write_photos_info_to_mysql(photos_info):
    oracledb = get_db()
    cursor = oracledb.cursor()
    sql = "insert into photos_data (url, star, title, organ, photos_nums, url_pages_nums, photos_src) " \
          "values (:1,:2,:3,:4,:5,:6,:7)"
    if len(photos_info['src']) > 15000:
        photos_info['src'] = ''
    flag = True
    try:
        cursor.execute(sql, (
            photos_info['url'], photos_info['star'], photos_info['title'], photos_info['organization'],
            photos_info['photos_nums'],
            photos_info['url_pages_nums'], photos_info['src']
        ))
    except cx_Oracle.Error as e:
        print(photos_info['url'] + "信息插入数据库失败")
        oracledb.rollback()
        flag = e
    else:
        print(photos_info['url'] + "信息插入数据库成功")
        oracledb.commit()
    finally:
        cursor.close()
        oracledb.close()
        return sql, flag
