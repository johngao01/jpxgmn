# 这个脚本用来下载写真所有图片
import os
from datetime import datetime
from multiprocessing import Manager, Pool
from urllib.parse import urlparse
import requests
import urllib3
from utils import domain
from utils import get_db, do_request
from PIL import Image

urllib3.disable_warnings()
header = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                        '(KHTML, like Gecko) Chrome/86.0.4process_num0.75 Safari/537.36'}


def photo_need_download():
    # 返回所有需要下载的图片
    db = get_db()
    _cursor = db.cursor()
    photo = []
    sql = "select * from photo where file_status=0"
    _cursor.execute(sql)
    for data in _cursor.fetchall():
        photo.append(data)
    db.close()
    return photo


def photo_titles(photos):
    # 获取这些图片所属写真的标题
    url_titles = []
    titles = []
    for photo in photos:
        if photo[6] in titles:
            continue
        titles.append(photo[6])
        url = domain + photo[3]
        url_titles.append(url + "   " + photo[6])
    return url_titles


def is_valid_image(file_path):
    try:
        with Image.open(file_path) as image:
            image.verify()
        return True
    except (IOError, SyntaxError):
        return False


def download(photo, lock, done_photo):
    url = domain + photo[0]
    star_path = photo[7][1:]
    org_path = photo[8][1:]
    save_root = os.path.join(os.path.expanduser('~'), 'Desktop', 'medias')
    star_dir = os.path.join(save_root, 'xgmn', star_path)
    org_dir = os.path.join(save_root, 'jpxgmn', org_path)
    file_name = os.path.basename(urlparse(url).path)
    star_file = star_dir + "/" + file_name
    org_file = org_dir + "/" + file_name
    if os.path.exists(star_file) and os.path.exists(org_file):
        print(url, file_name, os.path.getsize(star_file))
        with lock:
            done_photo.append({'src': photo[0]})
    else:
        response = do_request(url, True)
        if isinstance(response, requests.Response):
            os.makedirs(org_dir, exist_ok=True)
            os.makedirs(star_dir, exist_ok=True)
            with open(star_file, 'wb') as f:
                f.write(response.content)
            if is_valid_image(star_file):
                with open(org_file, 'wb') as f:
                    f.write(response.content)
                print(url, file_name, len(response.content))
                with lock:
                    done_photo.append({'src': photo[0]})
            else:
                os.remove(star_file)
        else:
            with lock:
                with open('files/出错记录.txt', mode='a', encoding='utf-8') as f_write:
                    f_write.write(url)
                    f_write.write('\n')


if __name__ == '__main__':
    all_photo = photo_need_download()
    latest_photos_title = photo_titles(all_photo)
    m = Manager()
    lock_ = m.Lock()  # 这里用 multiprocessing.Lock 下面多进程运行不起来
    download_photo = m.list()
    print(f"一共有 {len(all_photo)} 个图片等待下载")
    p = Pool()
    for photo_to_download in all_photo:
        # download(photo_to_download, lock_, download_photo)
        p.apply_async(func=download, args=(photo_to_download, lock_, download_photo,))
    p.close()
    p.join()
    with get_db() as connect:
        with connect.cursor() as cursor:
            src_data = list(download_photo)
            cursor.executemany("update photo set file_status=1 where src= :src", src_data)
        connect.commit()
    with open(r'files/下载历史.txt', mode='r+', encoding='utf-8') as file:
        latest_photos_title.insert(0, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        split = '\n\n*******************************\n\n'
        content = file.read()
        file.seek(0, 0)
        file.write('\n'.join(latest_photos_title) + split + content)
