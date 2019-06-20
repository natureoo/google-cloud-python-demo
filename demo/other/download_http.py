#!/usr/bin/python
# -*- coding: UTF-8 -*-

import urllib2



def test_download(url):


    #  url = "http://googlehelper.net/download/Ghelper_1.4.6.beta.zip"

    file_name = url.split('/')[-1]
    u = urllib2.urlopen(url)
    f = open(file_name, 'wb')
    meta = u.info()
    file_size = int(meta.getheaders("Content-Length")[0])
    print "Downloading: %s Bytes: %s" % (file_name, file_size)

    file_size_dl = 0
    block_sz = 8192
    while True:
        buffer = u.read(block_sz)
        if not buffer:
            break

        file_size_dl += len(buffer)
        f.write(buffer)
        status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
        status = status + chr(8) * (len(status) + 1)
        print status,

    f.close()



if __name__ == '__main__':
    # url = "http://mirrors.tuna.tsinghua.edu.cn/apache//httpd/httpd-2.4.39.tar.bz2"
    url = "http://mirrors.tuna.tsinghua.edu.cn/apache//httpd/mod_fcgid/mod_fcgid-2.3.9.tar.gz"
    test_download(url)