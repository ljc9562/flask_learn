import requests
import pymysql
import pandas as pd
import math
from snownlp import SnowNLP
from bs4 import BeautifulSoup
# from time import sleep


def db_insert(
        id,
        status,
        address,
        province,
        city,
        zone,
        formatted_address,
        lng,
        lat,
        wgs_lng,
        wgs_lat,
        bd_lng,
        bd_lat,
        level):
    '''插入数据库'''
    conn = pymysql.connect(user='root', passwd='!QAZ2wsx',
                           host='localhost', db='marry_show', charset='utf8')
    cur = conn.cursor()
    sql = "INSERT INTO hbh_address (id,status,address,province,city,zone,formatted_address,lng,lat,wgs_lng,wgs_lat,bd_lng,bd_lat,level) VALUES ( '{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}' );".format(
        id, status, address, province, city, zone, formatted_address, lng, lat, wgs_lng, wgs_lat, bd_lng, bd_lat, level)
    print(sql)
    sta = cur.execute(sql)
    if sta == 1:
        print('Done')
    else:
        print('Failed')
    conn.commit()
    cur.close()
    conn.close()


def fan_to_jian(text):
    '''使用snownlp庫進行繁體轉簡體'''
    fan = SnowNLP(text)
    return fan.han


def gcj02_to_bd09(lng, lat):
    """
    火星坐标系(GCJ-02)转百度坐标系(BD-09)
    谷歌、高德——>百度
    :param lng:火星坐标经度
    :param lat:火星坐标纬度
    :return:
    """
    z = math.sqrt(lng * lng + lat * lat) + 0.00002 * math.sin(lat * x_pi)
    theta = math.atan2(lat, lng) + 0.000003 * math.cos(lng * x_pi)
    bd_lng = z * math.cos(theta) + 0.0065
    bd_lat = z * math.sin(theta) + 0.006
    return [bd_lng, bd_lat]


def gcj02_to_wgs84(lng, lat):
    """
    GCJ02(火星坐标系)转GPS84
    :param lng:火星坐标系的经度
    :param lat:火星坐标系纬度
    :return:
    """
    if out_of_china(lng, lat):
        return lng, lat
    dlat = _transformlat(lng - 105.0, lat - 35.0)
    dlng = _transformlng(lng - 105.0, lat - 35.0)
    radlat = lat / 180.0 * pi
    magic = math.sin(radlat)
    magic = 1 - ee * magic * magic
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi)
    dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * pi)
    mglat = lat + dlat
    mglng = lng + dlng
    return [lng * 2 - mglng, lat * 2 - mglat]


def _transformlat(lng, lat):
    ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + \
        0.1 * lng * lat + 0.2 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
            math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lat * pi) + 40.0 *
            math.sin(lat / 3.0 * pi)) * 2.0 / 3.0
    ret += (160.0 * math.sin(lat / 12.0 * pi) + 320 *
            math.sin(lat * pi / 30.0)) * 2.0 / 3.0
    return ret


def _transformlng(lng, lat):
    ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + \
        0.1 * lng * lat + 0.1 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
            math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lng * pi) + 40.0 *
            math.sin(lng / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(lng / 12.0 * pi) + 300.0 *
            math.sin(lng / 30.0 * pi)) * 2.0 / 3.0
    return ret


def out_of_china(lng, lat):
    """
    判断是否在国内，不在国内不做偏移
    :param lng:
    :param lat:
    :return:
    """
    return not (lng > 73.66 and lng < 135.05 and lat > 3.86 and lat < 53.55)
# get_address_coordinate('111','昌岗中路237号信和广场5楼519单元广东安信投资集团有限公司')


def ak_judge(index):
    '''设置AK列表，PS: count 设置当前已经使用多少AK的配额'''
    count = 0
    list = ['c6a89a5982f624cbbf566f093aefbd62',
        '97bcefce739a4e5f7f75a65a92a04c59',
        '3827e4db0b81de291c15a5f41f5ce02a',
        '711082a2c4fd29c85f9f81c21151d2cd',
        '624713d191dceec3a8520f22f3137f07',
        '5304bbf104af112da97306c2754988b5',
        'b839eafe4bd883a7cf05e3b3143ddf30',
        '5f372219b88c76394af07551f4ac2174',
        '6d6e240329acdeabe080e712ce8481d7',
        '559849079437dec46a67a4fc6cc8c1f2']
    cnt = (index + count +1) / 6000
    return list[int(cnt)]


def get_address_coordinate(id, address, index):
    '''調用api下載數據'''
    ak = ak_judge(index)
    map = requests.get(
        'http://restapi.amap.com/v3/geocode/geo?address={}&output=XML&key={}'.format(
            fan_to_jian(address), ak))
    # id, status, address, province, city, zone, formatted_address, lng, lat, wgs_lng, wge_lat, level
    info = BeautifulSoup(map.content, 'lxml')
    id = id
    address = fan_to_jian(address)
    try:
        status = info.select('response > status')[0].text
    except BaseException:
        status = ''
    try:
        formatted_address = info.select('geocode > formatted_address')[0].text
    except BaseException:
        formatted_address = ''
    try:
        province = info.select('geocode > province')[0].text
    except BaseException:
        province = ''
    try:
        city = info.select('geocode > city')[0].text
    except BaseException:
        city = ''
    try:
        zone = info.select('geocode > district')[0].text
    except BaseException:
        zone = ''
    try:
        lng = info.select('geocode > location')[0].text.split(',')[0]
    except BaseException:
        lng = ''
    try:
        lat = info.select('geocode > location')[0].text.split(',')[1]
    except BaseException:
        lat = ''
    try:
        level = info.select('geocode > level')[0].text
    except BaseException:
        level = ''
    try:
        wgs_lng = str(gcj02_to_wgs84(float(lng), float(lat))[0])
        wgs_lat = str(gcj02_to_wgs84(float(lng), float(lat))[1])
    except BaseException:
        wgs_lng = ''
        wgs_lat = ''
    try:
        bd_lng = str(gcj02_to_bd09(float(lng), float(lat))[0])
        bd_lat = str(gcj02_to_bd09(float(lng), float(lat))[1])
        # print(bd_lng, bd_lat)
    except BaseException:
        bd_lng = ''
        bd_lat = ''

    db_insert(
        id,
        status,
        address,
        province,
        city,
        zone,
        formatted_address,
        lng,
        lat,
        wgs_lng,
        wgs_lat,
        bd_lng,
        bd_lat,
        level)
    # print(id, status, address, province, city, zone, formatted_address, lng, lat, wgs_lng, wgs_lat,bd_lng,bd_lat, level)
# gcj02_to_bd09(113.274536,23.090049)[0]
# gcj02_to_wgs84(113.274536,23.090049)


def main(data):
    '''control funtion'''
    id = data[0]
    address = data[1]
    index = data[2]
    # print(id, address)
    try:
        get_address_coordinate(id, address, index)
    except BaseException:
        pass


if __name__ == '__main__':
    x_pi = 3.14159265358979324 * 3000.0 / 180.0
    pi = 3.1415926535897932384626  # π
    a = 6378245.0  # 长半轴
    ee = 0.00669342162296594323  # 扁率

    # get_address_coordinate('12222', '广东省青凤社区星光老年之家')
    soure_file = pd.read_excel(
        r"C:\Users\85442\Desktop\20171228地址坐标工作.xlsx",
        sheetname='余量')
    soure_file['index'] = soure_file.index
    soure_file.apply(main, axis=1)
