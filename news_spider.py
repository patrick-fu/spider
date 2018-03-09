# -*- coding： utf-8 -*-
# Created by FFJ on 20170825
#
# 多进程广度优先的抓取特定新闻网站的脚本
# 通过种子网页（主页）抓取正文文本以及所有符合要求的URL进链接库
# 逐一访问并递归以上过程
#
#
# 修改 get_html() 中的 decode() 编码方式以获取HTML （一般是 utf-8 或 gbk ）
# 修改 get_content() 以匹配需要爬取的网站的正文所在标签
# 修改 get_all_links() 的 matches 以匹配需要的特定URL
#
# 修改 Spider().__init__() 中的种子URL、链接库文件名、输出文件名
#
#
# 参数 （都在 Spider().__init__() 里使用）
# -n 多进程数量（默认为1）
# -p 网站前缀（默认为 www ）


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
        raw_content = soup.find_all('div', {'class': 'overview'})
        raw_content.append(soup.find_all('div', {'class': 'post_text'}))
        raw_content = re.sub('<!--开始-->[\s\S]+<!--结束-->|<!--\[if[\s\S]+<!\[endif\]-->|' +
                             '<style[\s\S]+</style>|<script[\s\S]+</script>', '', str(raw_content))
        s = '<html><body>{}</body></html>'.format(raw_content)
        soup2 = BeautifulSoup(s, 'lxml')
        content = ''
        for i in soup2.find_all('p'):
            i = remove_html_tag(str(i))
            content += i.strip()
        if content:
            return content
        else:
            return ''
    except Exception as e:
        logging.error('Get content: {}'.format(e))
        return ''


def get_all_links(html):
    # 匹配出HTML中的所有需要的链接
    try:
        # matches = re.findall('"((http|ftp)s?://.*?)"', html)
        matches = re.findall('http://\S+?html', html)
        links = []
        for i in matches:
            links.append(i)
        return links
    except Exception as e:
        logging.error('Get all links: {}'.format(e))
        return []


class Spider(object):

    def __init__(self):
        # 多进程数量
        self.process_num = args.n

        # 网站前缀参数
        self.prefix = args.p

        # 种子URL
        self.seed_url = 'http://{}.163.com/'.format(self.prefix)
        # 链接库文件
        self.links_base_file = args.output + '/temp_{}_163_links_base.txt'.format(self.prefix)
        # 已经爬过的链接库文件
        self.crawled_link_file = args.output + '/temp_{}_163_crawled_links'.format(self.prefix)
        # 输出文件
        self.output_file = args.output + '/{}_163.txt'.format(self.prefix)

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
                    for line in fr:
                        line = line.strip()
                        self.crawled_links_list.append(line)
        except Exception as e:
            logging.error('Load links to crawled list: {}'.format(e))

        try:
            if os.path.exists(self.links_base_file):
                with open(self.links_base_file, 'r') as fr:
                    for line in fr:
                        line = line.strip()
                        if line not in self.crawled_links_list:
                            if line not in self.link_queue:
                                self.link_queue.append(line)
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
                with open(self.output_file, 'a') as fw:
                    fw.write('{}\n'.format(content))
                return True
            return False
        except Exception as e:
            logging.error('Save content: {}'.format(e))
            return False

    def save_crawled_links(self, link):
        # 保存已经抓取的link到 ./crawled_link_file
        try:
            with open(self.crawled_link_file, 'a') as fw:
                fw.write(link + '\n')
        except Exception as e:
            logging.error('Save crawled link：{}'.format(e))

    def save_all_links(self, html):
        # 获取页面所有匹配的link并保存到 ./links_base_file
        try:
            all_links = get_all_links(html)
            with open(self.links_base_file, 'a') as fw:
                for link in all_links:
                    link = re.sub(r'[^\x00-\x7f]', ' ', link)
                    if not self.is_special_pattern_url(link):
                        continue
                    if link not in self.link_queue:
                        if link not in self.crawled_links_list:
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
                        self.load_links()
                url = self.link_queue.pop(0)
                self.save_crawled_links(url)
                if url in self.crawled_links_list and url != self.seed_url:
                    continue

                html = get_html(url)
                self.crawled_links_list.append(url)
                if not html:
                    continue

                check_save_content = self.save_content(html)
                check_save_all_links = self.save_all_links(html)

                if check_save_content and check_save_all_links:
                    logging.warning('ok: {}'.format(url))
                else:
                    continue
            except Exception as e:
                logging.critical('尚未预料的错误: {}'.format(e))
                continue

    def start(self):
        # 载入链接到内存并启动多进程
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
    parser.add_argument('-p', help='网页前缀（默认为主页）', type=str, default='www')
    parser.add_argument('--log', help='输出log到文件，否则输出到控制台', action='store_true')
    args = parser.parse_args()

    log_flag = args.log
    if log_flag:
        log_flag = args.output + '/log.txt'

    logging.basicConfig(format='%(asctime)s|PID:%(process)d|%(levelname)s: %(message)s',
                        level=logging.WARNING, filename=log_flag)

    spider = Spider()
    spider.start()
