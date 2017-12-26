#!/usr/bin/env python3
#  -*- coding： utf-8 -*-
# Created by FFJ on 2017-10-11
#
# 依赖 lxml 库: pip install lxml
#
# 多进程广度优先的抓取百度百科纯文本的脚本
# 通过种子网页（主页）抓取正文文本以及所有符合要求的URL进链接库
# 逐一访问并递归以上过程
#
# 修改 get_html() 中的 decode() 编码方式以获取HTML （一般是 utf-8 或 gbk ）
# 修改 get_content() 以匹配需要爬取的网站的正文所在标签
# 修改 get_all_links() 的 matches 以匹配需要的特定URL
#
# 参数
# -n 多进程数量（默认为1）
# --log log文件名


import os
import argparse
from bs4 import BeautifulSoup
from utils import *

# 多进程的锁
m_lock = multiprocessing.Lock


def get_content(html):
    # 获取HTML中需要的正文文本
    # 根据每个网站的源码规则来定制
    try:
        soup = BeautifulSoup(html, 'lxml')
        raw_content = soup.find_all('div', {'class': 'para'})
        content = ''
        for i in raw_content:
            i = remove_html_tag(str(i))
            i = re.sub('\n', '', i)
            i = re.sub('\[[0-9]+-*[0-9]*]', '', i)
            if i:
                content += '\n{}'.format(i.strip())
        if content:
            return content
        else:
            return ''
    except Exception as e:
        logging.error('Get content: {}'.format(e))
        return ''


class Spider(object):

    def __init__(self):
        # 多进程数量
        self.process_num = args.n

        # 种子URL
        self.seed_url = 'https://baike.baidu.com'

        # 链接库文件
        self.links_base_file = args.output + '/temp_links_base.txt'
        # 已经爬过的链接库文件
        self.crawled_link_file = args.output + '/temp_crawled_links.txt'
        # 输出文件
        self.output_file = args.output + '/baidu_baike.txt'

        # 等待爬取的链接队列
        # 因为 multiprocessing.Queue() 最大容量只有三万多条，用 Manager().list() 代替
        self.link_queue = multiprocessing.Manager().list()

        # 爬过的链接列表，用于去重
        self.crawled_links_list = multiprocessing.Manager().list()

    def load_links(self):
        # 载入link到内存
        try:
            if os.path.exists(self.crawled_link_file):
                with open(self.crawled_link_file, 'r') as fr:
                    count = 0
                    for line in fr:
                        count += 1
                        line = line.strip()
                        self.crawled_links_list.append(line)
                        if count % 1000 == 0:
                            logging.warning('已载入已爬链接{}条'.format(count))
        except Exception as e:
            logging.error('Load links to crawled list: {}'.format(e))

        try:
            if os.path.exists(self.links_base_file):
                with open(self.links_base_file, 'r') as fr:
                    count = 0
                    for line in fr:
                        line = line.strip()
                        if line not in self.crawled_links_list:
                            if line not in self.link_queue:
                                count += 1
                                self.link_queue.append(line)
                                if count % 1000 == 0:
                                    logging.warning('已载入待爬链接{}条'.format(count))
        except Exception as e:
            logging.error('Load links to Queue: {}'.format(e))

        if not self.link_queue:
            self.link_queue.append(self.seed_url)
        logging.warning("Load links success")

    def save_content(self, html):
        # 保存正文文本到 ./output_file
        try:
            content = get_content(html)
            if content:
                with open(self.output_file, 'a', encoding='utf-8') as fw:
                    fw.write('{}\n\n'.format(content))
                return True
            return False
        except Exception as e:
            logging.error('Save content: {}'.format(e))
            return False

    def save_crawled_links(self, link):
        # 保存已经抓取的link到 ./crawled_link_file
        try:
            with open(self.crawled_link_file, 'a', encoding='utf-8') as fw:
                fw.write(link + '\n')
        except Exception as e:
            logging.error('Save crawled link：{}'.format(e))

    def save_all_links(self, html):
        # 获取页面所有匹配的link并保存到 ./links_base_file
        try:
            matches = re.findall('/item/[%A-Z0-9/]+', html)
            all_links = []
            for i in matches:
                i = self.seed_url + i
                all_links.append(i)
            with open(self.links_base_file, 'a', encoding='utf-8') as fw:
                for link in all_links:
                    if link not in self.crawled_links_list:
                        if link not in self.link_queue:
                            self.link_queue.append(link)
                            fw.write(link + '\n')
            return True
        except Exception as e:
            logging.error('Save all links：{}'.format(e))
            return False

    def is_special_pattern_url(self, url):
        # 检测是否为指定网站的网页
        if self.seed_url in url[:40]:
            return True
        return False

    def run(self):
        # 多进程主循环
        while True:
            try:
                if not self.link_queue:
                    time.sleep(20 + random.randint(1, 20))
                    if not self.link_queue:
                        with m_lock:
                            self.load_links()
                url = self.link_queue.pop(0)

                if url in self.crawled_links_list and url != self.seed_url:
                    continue

                self.crawled_links_list.append(url)
                self.save_crawled_links(url)

                html = get_html(url)
                if not html:
                    continue

                check_save_content = self.save_content(html)
                check_save_all_links = self.save_all_links(html)

                if check_save_content and check_save_all_links:
                    logging.warning('ok: {}'.format(url))

            except Exception as e:
                logging.critical('尚未预料的错误: {}'.format(e))
                continue

    def start(self):
        # 载入链接到内存并启动多进程
        logging.warning('Start load links')
        self.load_links()
        time.sleep(3)
        processes = []
        for i in range(self.process_num):
            t = multiprocessing.Process(target=self.run, args=())
            t.start()
            processes.append(t)

        for t in processes:
            t.join()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('output', help='输出文件夹路径，末尾不要带斜杠')
    parser.add_argument('-n', help='多进程数量（默认为1）', type=int, default=1)
    parser.add_argument('--log', help='输出log到文件，否则输出到控制台', action='store_true')
    args = parser.parse_args()

    log_flag = args.log
    if log_flag:
        log_flag = args.output + '/log.txt'

    logging.basicConfig(format='%(asctime)s|PID:%(process)d|%(levelname)s: %(message)s',
                        level=logging.WARNING, filename=log_flag)

    spider = Spider()
    spider.start()
