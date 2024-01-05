# 小工具
import logging
import re
from datetime import datetime
from typing import Optional

import cx_Oracle
import requests
import urllib3
from lxml import etree
from retrying import retry

urllib3.disable_warnings()
header = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                        '(KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'}

domain = "https://www.xgmn01.cc"


def get_organization_had_info(organization: Optional[str]):
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
    dsn_tns = cx_Oracle.makedsn('localhost', '1521', service_name='orcl')
    conn = cx_Oracle.connect(user='jpxgmn', password='123456', dsn=dsn_tns)
    return conn


@retry(stop_max_attempt_number=10)
def do_request(url: str, stream=False, page_url=''):
    try:
        response = requests.get(url, stream=stream, timeout=20, headers=header, verify=False)
        response.raise_for_status()
    except Exception as e:
        if stream:
            print(e, url, page_url)
    else:
        return response


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
    def pic_info(page, page_url, pics):
        for pic_url in pics:
            src = pic_url.get('src')
            if src is None:
                continue
            srcs.append(src)
            path = src.split('?')[0]
            filename = path.split('/')[-1]
            filename, file_type = filename.split('.')
            pic_dict = {'src': str(src), 'html_page': page, 'page_url': page_url,
                        'star_path': r"/{}/{}".format(photo_infos['star'], photo_infos['photos_title']),
                        'org_path': r"/{}/{}".format(organization, photo_infos['photos_title']),
                        'file_status': 0, 'filename': filename, 'file_type': file_type}
            pic_dict.update(photo_infos)
            photos.append(pic_dict)

    count = 1
    photo_infos = {}
    photos = []
    srcs = []
    url = domain + photos_url
    response = do_request(url)
    if response.status_code == 404:
        return False
    response.encoding = 'utf-8'
    tree = etree.HTML(response.text)
    photo_infos['photos_url'] = photos_url
    star = tree.xpath("//span[@class='item item-2']/a")[0].text
    if star is None:
        photo_infos['star'] = 'unknown'
    else:
        photo_infos['star'] = star.strip()
    title = tree.xpath('//h1[@class="article-title"]')[0].text
    photo_infos['organ'] = organization.lower()
    photo_infos['photos_title'] = re.sub('[\\\\/:*?"<>|\n]', '', title)
    urls = tree.xpath("//p[@style='text-align: center']/img")
    db = get_db()
    # 获取写真首页的图片数据
    pic_info(0, photos_url, urls)
    while True:
        srcs = []
        next_url = url[0:-5] + "_" + str(count) + ".html"
        response = do_request(next_url)
        if response is None:
            break
        response.encoding = 'utf-8'
        tree = etree.HTML(response.text)
        if response.status_code == 404 or tree.xpath('/html/head/title')[0].text == '访问页面出错了':
            break
        urls = tree.xpath("//p[@align='center']/img")
        pic_info(count, next_url, urls)
        count += 1
    photo_infos['photos_nums'] = len(photos)
    photo_infos['url_pages_nums'] = count
    photo_infos['photos_src'] = ','.join(srcs)
    write_photo_info_to_mysql(db, photos)
    db.close()
    return photo_infos


def write_org_photos(db, org, p_url, title, p_date):
    sql = "insert into organ_data (organ, photos_url, title, up_date) values (:1,:2,:3,:4)"
    try:
        db.cursor().execute(sql, (org, p_url, title, p_date))
        db.commit()
    except cx_Oracle.Error:
        db.rollback()


def write_photo_info_to_mysql(database, photos):
    cursor = database.cursor()
    sql = ("insert into photo values (:src, :html_page, :page_url, :photos_url, :star, :organ, :photos_title, "
           ":star_path, :org_path, :file_status, :filename, :file_type)")
    try:
        cursor.executemany(sql, photos)
        database.commit()
    except cx_Oracle.Error as e:
        print(e)
        database.rollback()
    finally:
        cursor.close()


def get_organs():
    oracledb = get_db()
    cursor = oracledb.cursor()
    sql = ("select organ from (select organ, max(UP_DATE) up_date from ORGAN_DATA group by organ) "
           "where up_date > date '2023-06-07'")
    cursor.execute(sql)
    organs = [organ[0] for organ in cursor.fetchall()]
    cursor.close()
    oracledb.close()
    return organs


def write_photos_info_to_mysql(photos_info):
    oracledb = get_db()
    cursor = oracledb.cursor()
    sql = "insert into photos_data (url, star, title, organ, photos_nums, url_pages_nums, photos_src, scrapy_date) " \
          "values (:1,:2,:3,:4,:5,:6,:7,:8)"
    if len(photos_info['photos_src']) > 15000:
        photos_info['photos_src'] = ''
    flag = True
    try:
        cursor.execute(sql, (
            photos_info['photos_url'], photos_info['star'], photos_info['photos_title'], photos_info['organ'],
            photos_info['photos_nums'], photos_info['url_pages_nums'], photos_info['photos_src'], datetime.now().date()
        ))
    except cx_Oracle.Error as e:
        print(photos_info['photos_url'] + "信息插入数据库失败")
        oracledb.rollback()
        flag = e
    else:
        print(photos_info['photos_url'] + "信息插入数据库成功")
        oracledb.commit()
    finally:
        cursor.close()
        oracledb.close()
        return sql, flag
