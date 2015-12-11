#!/usr/bin/python
#-*-coding:utf-8-*-

import re,os
import time

def getUrls(path):
    urls = list()
    fp = open(path, 'rb')
    
    for line in fp.readlines():
        url = re.search('url: (.*?) is added', line)
        if url:
            urls.append(url.group(1) + '\n')

    fp.close()
    return urls
    
def addUrls(path, urls):
    urls_set = set(urls)
    fp = open(path, 'rb')
    for line in fp.readlines():
        urls_set.add(line )
    fp.close()
    
    createUrls(path, list(urls_set))

def createUrls(path, urls):
    op = open(path, 'wb')
    op.writelines(urls)
    op.close()

def delSpiderLog(path):
    for filename in os.listdir(path):
        if re.search("spider\_.*?\.log", filename):
            os.remove(path + '/' + filename)

if __name__ == '__main__':
    path = os.getcwd()
    logs_path = path + '/logs'
    log_path = logs_path + '/' + time.strftime("%Y-%m-%d", time.localtime()) + '.log'

    urls = list()

    if (not os.path.exists(logs_path)):
        os.mkdir(logs_path)

    for name in os.listdir(path):
        if re.search('\.log', name):
            urls.extend(getUrls(name))

    if (os.path.exists(log_path)):
        addUrls(log_path, urls)
    else:
        createUrls(log_path, urls)
        delSpiderLog(path)
