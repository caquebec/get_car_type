# -*- coding:utf-8 -*-
from bs4 import BeautifulSoup
import pymysql
import datetime
import calendar
import requests
import time
from configparser import ConfigParser
import json
import redis
import urllib.request
import os
import time
import func_timeout
from func_timeout import func_set_timeout

cp = ConfigParser()
cp.read("config.ini")
section = cp.sections()[0]
options = cp.options(section)
config = {}
for o in options:
    if o != 'port':
        config[o] = cp.get(section, o)
    else:
        config[o] = cp.getint(section, o)
#print(config)


dbconn = ''
cursor = ''


def get_connection():
    global dbconn
    global cursor
    try:
        #print(config)
        dbconn = pymysql.Connect(**config)
        print('conneting to host: ' + config['host'])
        print('数据库连接成功')
    except Exception as e:
        print('连接数据库失败！', e.__str__())

    cursor = dbconn.cursor()
    return dbconn, cursor


get_connection()


def init_census(start, end):
    global dbconn
    global cursor

    for i in range(start, end):
        sql = "insert into batch_census(batch, recordtotal, recordsaved, is_censused) values('" \
              + i.__str__() + "','0','0','0')"
        try:
            cursor.execute(sql)
            dbconn.commit()  # 提交到数据库执行
        except Exception as e:
            print('插入batch_census失败', e.__str__())


def get_batch_census_task():
    global dbconn
    global cursor
    sql = "select batch from batch_census where is_censused='0'"
    try:
        cursor.execute(sql)
        dbconn.commit()  # 提交到数据库执行
        task = list(cursor.fetchall())
        result = []
        if task.__len__() != 0:
            for i in range(len(task)):
                task[i] = list(task[i])
            for t in task:
                result.append(t[0])
            return result
        else:
            return 0
    except Exception as e:
        print('获取batch_census_task任务列表失败', e.__str__())


def get_batch_task():
    global dbconn
    global cursor
    sql = "select batch from batch_census where is_done='0' and recordtotal!='0'"
    try:
        cursor.execute(sql)
        dbconn.commit()  # 提交到数据库执行
        task = list(cursor.fetchall())
        result = []
        if task.__len__() != 0:
            for i in range(len(task)):
                task[i] = list(task[i])
            for t in task:
                result.append(t[0])
            return result
        else:
            return 0
    except Exception as e:
        print('获取batch_task任务列表失败', e.__str__())



def save_census_batch(batch, recordtotal):
    global dbconn
    global cursor

    # 记录已经存在，更新recordtotal
    sql_1 = "update batch_census set recordtotal='" + recordtotal + "' where batch='" + batch + "'"
    sql_2 = "update batch_census set is_censused='1' where batch='" + batch + "'"
    try:
        cursor.execute(sql_1)
        cursor.execute(sql_2)
        dbconn.commit()  # 提交到数据库执行
    except Exception as e:
        print('更新普查信息失败', e.__str__())


def get_recordtotal(batch):
    global dbconn
    global cursor
    sql = "select recordtotal from batch_census where batch='" + batch + "'"
    try:
        cursor.execute(sql)
        dbconn.commit()  # 提交到数据库执行
        task = list(cursor.fetchone())
        return task[0]
    except Exception as e:
        print('获取recordtotal任务列表失败', e.__str__())


def get_recordsaved(batch):
    global dbconn
    global cursor
    sql = "select recordsaved from batch_census where batch='" + batch + "'"
    try:
        cursor.execute(sql)
        dbconn.commit()  # 提交到数据库执行
        task = list(cursor.fetchone())
        return task[0]
    except Exception as e:
        print('获取recordsaved任务列表失败', e.__str__())


def set_isdone(batch):
    global dbconn
    global cursor
    sql = "update batch_census set is_done='1' where batch='" + batch + "'"
    try:
        cursor.execute(sql)
        dbconn.commit()  # 提交到数据库执行
    except Exception as e:
        print('update isdone任务列表失败', e.__str__())


@func_set_timeout(30)
def save_page(batch, driver, handle_page):
    #print(batch, driver, handle_page)
    time_start = time.time()
    recordsaved = int(get_recordsaved(batch))
    id = batch + "_" + str(recordsaved + 1).zfill(6)
    print('正保存 id: ', id)

    driver.switch_to.window(handle_page)

    # start process pic

    pic_ul = driver.find_element_by_xpath('//*[@id="mscroll-list"]/ul')
    pic_lis = pic_ul.find_elements_by_xpath('li')
    pic_num = len(pic_lis)
    #print('found pictures: ', pic_lis)
    if not os.path.exists(os.path.join(os.getcwd(), "pic")):
        os.mkdir(os.path.join(os.getcwd(), "pic"))
    pic_base_dir = os.path.join(os.getcwd(), "pic")
    #print('pic_base_dir:  ', pic_base_dir)
    if not os.path.exists(os.path.join(pic_base_dir, batch)):
        os.mkdir(os.path.join(pic_base_dir, batch))
    pic_batch_dir = os.path.join(pic_base_dir, batch)
    #print('current pic dir: ', pic_batch_dir)

    soup = BeautifulSoup(driver.page_source, "lxml")
    time.sleep(5)
    div_pros = soup.findAll(attrs={'onmouseover': True})
    #print("pic founded! ")
    for p in range(pic_num):
        pic_dir = os.getcwd()
        #print(pic_dir)
        pic_name = pic_dir + '\\pic\\' + batch + '\\' + id + "_" + str(p+1) + '.jpg'
        #print('pic_name: ', pic_name)
        pic_addr = str(div_pros[p]).split('Tip("')[1].split('")')[0]
        #print(pic_addr)
        try:
            urllib.request.urlretrieve(pic_addr, pic_name)
            print('pic : ', p+1, '  has been saved')
        except Exception as e:
            print('图片保存异常 ， ', e)

    # start process data
    #time.sleep(1000)

    t1 = ''
    t2 = ''
    t3 = ''
    t4 = ''
    t5 = ''
    t6 = ''
    t7 = ''
    t8 = ''
    t9 = ''
    t10 = ''
    t11 = ''
    t12 = ''
    t13 = ''
    t14 = ''
    t15 = ''
    t16 = ''
    t17 = ''
    t18 = ''
    t19 = ''
    t20 = ''
    t21 = ''
    t22 = ''
    t23 = ''
    t24 = ''
    t25 = ''
    t26 = ''
    t27 = ''
    t28 = ''
    t29 = ''
    t30 = ''
    t31 = ''
    t32 = ''
    t33 = ''
    t34 = ''
    t35 = ''
    t36 = ''
    t37 = ''
    t38 = ''
    t39 = ''
    t40 = ''
    t41 = ''
    t42 = ''
    t43 = ''
    t44 = ''
    t45 = ''
    t46 = ''
    t47 = ''
    t48 = ''
    t49 = ''
    t50 = ''
    t51 = ''
    t52 = ''
    t53 = ''
    t54 = ''
    t55 = ''
    t56 = ''
    t57 = ''
    t58 = ''
    t59 = ''

    # 生产企业信息
    try:
        t1 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[2]/td[2]/a').text
        t2 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[2]/td[4]/span[1]/a').text
        t3 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[3]/td[2]').text
        t4 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[3]/td[4]/span/a').text
        t5 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[4]/td[2]').text
        t6 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[4]/td[4]').text
        t7 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[5]/td[2]/span').text
        t8 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[5]/td[4]').text
        t9 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[6]/td[2]/a').text
        t10 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[6]/td[4]').text
        t11 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[7]/td[2]').text
        t12 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[7]/td[4]').text
        t13 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[8]/td[2]').text
        t14 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[8]/td[4]').text
        t15 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[9]/td[2]').text
        t16 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[9]/td[4]').text
    except Exception as e:
        print('生产企业信息， 失败', e)

    # 免检说明
    try:
        t17 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[11]/td[2]').text
        t18 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[11]/td[4]').text

    except Exception as e:
        print('免检说明， 失败', e)

    # 公告状态

    try:
        t19 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[13]/td[2]').text
        t20 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[13]/td[4]').text
        t21 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[14]/td[2]').text
        t22 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[14]/td[4]').text
    except Exception as e:
        print('公告状态， 失败', e)
    # 主要技术参数

    try:
        t23 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[16]/td[2]/span').text
        t24 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[16]/td[4]/span').text
        t25 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[17]/td[2]/span').text
        t26 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[17]/td[4]').text
        t27 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[18]/td[2]/span').text
        t28 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[18]/td[4]/span').text
        t29 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[19]/td[2]').text
        t30 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[19]/td[4]').text
        t31 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[20]/td[2]').text
        t32 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[20]/td[4]').text
        t33 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[21]/td[2]').text
        t34 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[21]/td[4]').text
        t35 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[22]/td[2]').text
        t36 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[22]/td[4]/span').text
        t37= driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[23]/td[2]').text
        t38 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[23]/td[4]/span').text
        t39 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[24]/td[2]').text
        t40 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[24]/td[4]').text
        t41 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[25]/td[2]').text
        t42 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[25]/td[4]').text
        t43 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[26]/td[2]').text
        t44 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[26]/td[4]').text
        t45 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[27]/td[2]/span').text
        t46 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[27]/td[4]/span').text
        t47 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[28]/td[2]').text
        t48 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[28]/td[4]').text
        t49 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[29]/td[2]').text
        t50 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[29]/td[4]').text
        t51 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[30]/td[2]').text
        t52 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[30]/td[4]').text
        t53 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[31]/td[2]').text
        t54 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[31]/td[4]').text
        t55 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[32]/td[2]/span[1]').text
        t56 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[37]/td[2]').text
        t57 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[37]/td[4]/span').text
        t58 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[38]/td[2]').text
        if t56 == '是':
            t59 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[43]/td').text
        else:
            t59 = driver.find_element_by_xpath('//*[@id="con_two_p1"]/table/tbody/tr[40]/td').text
    except Exception as e:
        print('其他， 失败', e)

    sql1 = "insert into cartype(id,cheliangmingcheng,cheliangleixing,zhizaodi,paizhaoleixing," \
          "gonggaopici,faburiqi,chanpinhao,muluxuhao,zhongwenpinpai," \
          "yingwenpinpai,gonggaoxinghao,mianzheng,qiyemingcheng,ranyou,qiyedizhi,huanbao," \
          "mianjian,mianjianyouxiaoqizhi,gonggaozhuangtai,gonggaozhuangtaimiaoshu," \
          "gonggaoshengxiaoriqi,biangengjilu,waixingchicun,huoxiangchicun,zongzhiliang," \
          "zaizhiliangliyongxishu,zhengbeizhiliang,edingzaizhiliang,guachezhiliang," \
          "banguaanzuo,jiashishi,qianpaichengke,edingzaike,fangbaosixitong,jiejinjiao," \
          "qianhouxuan,zhouhe,zhouju,zhoushu,zuigaochesu,youhao,tanhuangpianshu," \
          "luntaishu,luntaiguige,qianlunju,houlunju,zhidongqian,zhidonghou,zhicaoqian," \
          "zhicaohou,zhuanxiangxingshi,qidongfangshi,chuandongxingshi,youhao2,vin," \
          "ranliaozhonglei,yijubiaozhun,dipanpaifangbiaozhun,qita) \
        values('" + id + "','" + t1 + "','" + t2 + "','" + t3 + "','" + t4 + "','" + t5 + \
          "','" + t6 + "','" + t7 + "','" + t8 + "','" + t9 + "','" + t10 + "','" + t11 + \
          "','" + t12 + "','" + t13 + "','" + t14 + "','" + t15 + "','" + t16 + "','" + t17 +\
          "','" + t18 + "','" + t19 + "','" + t20 + "','" + t21 + "','" + t22 + "','" + t23 +\
          "','" + t24 + "','" + t25 + "','" + t26 + "','" + t27 + "','" + t28 + "','" + t29 +\
          "','" + t30 + "','" + t31 + "','" + t32 + "','" + t33 + "','" + t34 + "','" + t35 +\
          "','" + t36 + "','" + t37 + "','" + t38 + "','" + t39 + "','" + t40 + "','" + t41 +\
          "','" + t42 + "','" + t43 + "','" + t44 + "','" + t45 + "','" + t46 + "','" + t47 +\
          "','" + t48 + "','" + t49 + "','" + t50 + "','" + t51 + "','" + t52 + "','" + t53 +\
          "','" + t54 + "','" + t55 + "','" + t56 + "','" + t57 + "','" + t58 + "','" + t59 +\
          "')"
    # start process vin code . 17 char seperated by \n
    #print('len of vin : ', len(t55))
    #print('vin: ', t55)
    vin_num = len(t55)//17
    #print('num of vin: ', vin_num)
    vins = []
    leftover = t55
    for i in range(vin_num):
        vin = leftover[0:17]
        leftover = leftover[18:]
        leftover.strip()
        vins.append(vin)
    #print(vins)
    try:
        cursor.execute(sql1)
        for v in vins:
            sql2 = "insert into car_vin(id,vin) values('" + id + "','" + v + "')"
            cursor.execute(sql2)

        sql3 = "update batch_census set recordsaved='" \
              + str(recordsaved+1) + "' where batch='" + batch + "'"
        cursor.execute(sql3)
        dbconn.commit()  # 提交到数据库执行

        print('Record:' + id + '   @  ' +
              datetime.datetime.now().__str__()[5:22])
    except Exception as e:
        print('提交执行失败', e.__str__())
    time_end = time.time()
    time_consume = time_end - time_start
    print('              保存本条消耗时间：    ', round(time_consume, 2), '    秒')


def save_page_test(batch, driver, handle_page):

    time_start = time.time()
    recordsaved = int(get_recordsaved(batch))
    id = batch + "_" + str(recordsaved + 1).zfill(6)
    print('正保存 id: ', id)

    driver.switch_to.window(handle_page)

    # start process data

    t1 = ''
    t2 = ''
    t3 = ''
    t4 = ''
    t5 = ''
    t6 = ''
    t7 = ''
    t8 = ''
    t9 = ''
    t10 = ''
    t11 = ''
    t12 = ''
    t13 = ''
    t14 = ''
    t15 = ''
    t16 = ''
    t17 = ''
    t18 = ''
    t19 = ''
    t20 = ''
    t21 = ''
    t22 = ''
    t23 = ''
    t24 = ''
    t25 = ''
    t26 = ''
    t27 = ''
    t28 = ''
    t29 = ''
    t30 = ''
    t31 = ''
    t32 = ''
    t33 = ''
    t34 = ''
    t35 = ''
    t36 = ''
    t37 = ''
    t38 = ''
    t39 = ''
    t40 = ''
    t41 = ''
    t42 = ''
    t43 = ''
    t44 = ''
    t45 = ''
    t46 = ''
    t47 = ''
    t48 = ''
    t49 = ''
    t50 = ''
    t51 = ''
    t52 = ''
    t53 = ''
    t54 = ''
    t55 = ''
    t56 = ''
    t57 = ''
    t58 = ''
    t59 = ''

    sql1 = "insert into cartype(id,cheliangmingcheng,cheliangleixing,zhizaodi,paizhaoleixing," \
          "gonggaopici,faburiqi,chanpinhao,muluxuhao,zhongwenpinpai," \
          "yingwenpinpai,gonggaoxinghao,mianzheng,qiyemingcheng,ranyou,qiyedizhi,huanbao," \
          "mianjian,mianjianyouxiaoqizhi,gonggaozhuangtai,gonggaozhuangtaimiaoshu," \
          "gonggaoshengxiaoriqi,biangengjilu,waixingchicun,huoxiangchicun,zongzhiliang," \
          "zaizhiliangliyongxishu,zhengbeizhiliang,edingzaizhiliang,guachezhiliang," \
          "banguaanzuo,jiashishi,qianpaichengke,edingzaike,fangbaosixitong,jiejinjiao," \
          "qianhouxuan,zhouhe,zhouju,zhoushu,zuigaochesu,youhao,tanhuangpianshu," \
          "luntaishu,luntaiguige,qianlunju,houlunju,zhidongqian,zhidonghou,zhicaoqian," \
          "zhicaohou,zhuanxiangxingshi,qidongfangshi,chuandongxingshi,youhao2,vin," \
          "ranliaozhonglei,yijubiaozhun,dipanpaifangbiaozhun,qita) \
        values('" + id + "','" + t1 + "','" + t2 + "','" + t3 + "','" + t4 + "','" + t5 + \
          "','" + t6 + "','" + t7 + "','" + t8 + "','" + t9 + "','" + t10 + "','" + t11 + \
          "','" + t12 + "','" + t13 + "','" + t14 + "','" + t15 + "','" + t16 + "','" + t17 +\
          "','" + t18 + "','" + t19 + "','" + t20 + "','" + t21 + "','" + t22 + "','" + t23 +\
          "','" + t24 + "','" + t25 + "','" + t26 + "','" + t27 + "','" + t28 + "','" + t29 +\
          "','" + t30 + "','" + t31 + "','" + t32 + "','" + t33 + "','" + t34 + "','" + t35 +\
          "','" + t36 + "','" + t37 + "','" + t38 + "','" + t39 + "','" + t40 + "','" + t41 +\
          "','" + t42 + "','" + t43 + "','" + t44 + "','" + t45 + "','" + t46 + "','" + t47 +\
          "','" + t48 + "','" + t49 + "','" + t50 + "','" + t51 + "','" + t52 + "','" + t53 +\
          "','" + t54 + "','" + t55 + "','" + t56 + "','" + t57 + "','" + t58 + "','" + t59 +\
          "')"

    try:
        cursor.execute(sql1)
        sql3 = "update batch_census set recordsaved='" \
              + str(recordsaved+1) + "' where batch='" + batch + "'"
        cursor.execute(sql3)
        dbconn.commit()  # 提交到数据库执行

        print('Record:' + id + '   @  ' +
              datetime.datetime.now().__str__()[5:22])
    except Exception as e:
        print('提交执行失败', e.__str__())
    time_end = time.time()
    time_consume = time_end - time_start
    print('              保存本条消耗时间：    ', round(time_consume, 2), '    秒')

