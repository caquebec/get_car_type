from selenium import webdriver
from bs4 import BeautifulSoup
from datetime import date
import time
from selenium.common.exceptions import TimeoutException
import dba
import calendar
import sys
import psutil
import os
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import func_timeout
import logging


pid = ''
driver = ''


def is_element_exist(element):
    global driver
    flag = True
    try:
        driver.find_element_by_xpath(element)
        return flag
    except Exception as e:
        flag = False
        return flag


def retrive():
    global pid
    global driver
    dba.get_connection()

    batch_task = dba.get_batch_task()
    #batch_task.reverse()
    print(batch_task)

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.headless = True

    driver = webdriver.Chrome(options=chrome_options)
    # get pid
    p = psutil.Process(driver.service.process.pid)
    pid_string = str(p.children())
    pid = pid_string.split('pid=')[1].split(',')[0]

    driver.get("http://www.chinacar.com.cn/search.html")
    time.sleep(4)
    if is_element_exist('//*[@id="body_h"]/div[2]/div[3]/div[6]/div/span'):
        driver.find_element_by_xpath('//*[@id="body_h"]/div[2]/div[3]/div[6]/div/span').click()

    handle_base = driver.current_window_handle
    #print('handle_base: ', handle_base)

    #batch_task = ['230']

    for batch in batch_task:
        print("processing " + batch + "  batch 's records")

        batch_input = driver.find_element_by_xpath('//*[@id="s4"]')
        batch_input.clear()
        batch_input.send_keys(batch)

        driver.find_element_by_xpath('//*[@id="searchbtn"]').click()

        time.sleep(3)

        handles = driver.window_handles
        print('handles: ', handles)
        handle_list = ''
        for handle in handles:
            if handle == handle_base:
                continue
            handle_list = handle
        #print('handle_work: ', handle_work)
        driver.switch_to.window(handle_list)
        time.sleep(5)

        recordtotal = int(dba.get_recordtotal(batch))
        print("recordtotal : ", recordtotal)
        recordsaved = int(dba.get_recordsaved(batch))

        # calculate page position
        if int(recordtotal) % 400 == 0:
            page_total = recordtotal // 400
        else:
            page_total = recordtotal // 400 + 1

        if recordsaved % 400 == 0:
            page_pos = recordsaved // 400 + 1
            record_pos = 1
        else:
            page_pos = recordsaved // 400 + 1
            record_pos = recordsaved % 400

        print("total page: ", page_total)
        print('record total: ', recordtotal)
        print('record saved: ', recordsaved)
        print('page_pos: ', page_pos)
        print('record_pos: ', record_pos)

        while page_pos <= page_total:
            # first page or not?
            if page_pos == 1:  # 一页以内
                print(" -----------处理第 ", page_pos, ' 页内容--------------')
                num_base = 1116
                pos_start = recordsaved
                if recordtotal >= 400:
                    pos_end = 400
                else:
                    pos_end = recordtotal

                print('page_pos: ', page_pos)
                print('pos_start: ', pos_start)
                print("pos_end: ", pos_end)

                counter = 1
                for line in range(pos_start, pos_end):
                    #  //*[@id="ext-gen1238"]/div
                    xpath_line = '//*[@id="ext-gen' + str(num_base + line * 19) + '"]/div'
                    element = driver.find_element_by_xpath(xpath_line)

                    print(xpath_line)

                    driver.execute_script("var evt = document.createEvent('MouseEvents');" +
                                          "evt.initMouseEvent('dblclick',true, true, window, 0, 0, 0, 0, 0, false, false, false, false, 0,null);" +
                                          "arguments[0].dispatchEvent(evt);", element)
                    # positioning to page window
                    handles = driver.window_handles
                    # print('handles: ', handles)
                    handle_page = ''
                    for handle in handles:
                        if handle == handle_base or handle == handle_list:
                            continue
                        handle_page = handle
                    # print('handle_page: ', handle_page)
                    driver.switch_to.window(handle_page)

                    # process saving stuff

                    dba.save_page_test(batch, driver, handle_page)

                    #time.sleep(6000)

                    print('已保存第 ', counter , ' 行')

                    # close page window
                    driver.close()
                    # positioning to list window
                    driver.switch_to.window(handle_list)

                    counter += 1
                page_pos = page_pos + 1
            # 需要填数跳页-------------------J U M P....!!!------------------------
            else:
                counter = 1
                recordsaved = int(dba.get_recordsaved(batch))
                driver.refresh()    # refresh before processing
                time.sleep(5)       # waiting until 1116 base is stable

                page_input = driver.find_element_by_xpath('//*[@id="numberfield-1014-inputEl"]')
                page_input.clear()
                page_input.send_keys(page_pos)
                page_input.send_keys(Keys.ENTER)
                time.sleep(5)

                print(" -------正在跳转至目标页面-------------")
                print(" -----------处理第 ", page_pos, ' 页内容--------------')
                tail = int(recordtotal % 400)

                num_base = 8716

                #print('recordsaved: ', recordsaved)
                pos_start = recordsaved % 400
                if page_pos != page_total:
                    pos_end = 400
                else:
                    pos_end = tail

                print('page_pos: ', page_pos)
                print('pos_start: ', pos_start)
                print("pos_end: ", pos_end)

                for line in range(pos_start, pos_end):
                    xpath_line = '//*[@id="ext-gen' + str(num_base + line * 19) + '"]/div'

                    print(xpath_line)

                    element = driver.find_element_by_xpath(xpath_line)
                    driver.execute_script("var evt = document.createEvent('MouseEvents');" +
                                          "evt.initMouseEvent('dblclick',true, true, window, 0, 0, 0, 0, 0, false, false, false, false, 0,null);" +
                                          "arguments[0].dispatchEvent(evt);", element)
                    # positioning to page window
                    handles = driver.window_handles
                    # print('handles: ', handles)
                    handle_page = ''
                    for handle in handles:
                        if handle == handle_base or handle == handle_list:
                            continue
                        handle_page = handle
                    # print('handle_page: ', handle_page)
                    driver.switch_to.window(handle_page)

                    # process saving stuff
                    dba.save_page_test(batch, driver, handle_page)

                    print('已保存第 ', counter, ' 行')

                    # close page window
                    driver.close()
                    # positioning to list window
                    driver.switch_to.window(handle_list)

                    counter += 1
                page_pos = page_pos + 1
                print('存完后，page_pos: ', page_pos)

        print("本批次已存完。")
        dba.set_isdone(batch)
        driver.close()
        driver.switch_to.window(handle_base)

    print("所有批次已处理完")
    driver.close()


if __name__ == '__main__':

    d = os.getcwd().__str__()
    args = d.split('\\')[-1]
    print(args)
    '''
    year = args.split('_')[0]
    month = args.split('_')[1]
    time_interval = args.split('_')[2]
    '''
    is_success = False
    while True:
        try:
            #retrive(year, month)
            retrive()
            is_success = True
        except func_timeout.exceptions.FunctionTimedOut as e:
            print(e)
            print("kill a chrome")
            dos_command = 'taskkill /pid ' + pid + ' -t -f'
            os.system(dos_command)
        except Exception as e:
            print(e)
            print("kill a chrome")
            dos_command = 'taskkill /pid ' + pid + ' -t -f'
            os.system(dos_command)
        if is_success:
            break
