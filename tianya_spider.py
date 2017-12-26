#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Created by FFJ on 17-08-22
#
# 依赖 lxml 库: pip install lxml
#
# 按天涯帖子 ID 顺序爬取纯文本数据， 每个帖子保存为一个 ID_帖子标题.txt 文件
# ./ID.txt 存放从哪个 ID 开始爬，不存在则ID默认为 5000000
#
# 输出目录结构
# --output
# ----123
# ------1230001_示例标题一.txt
# ------1230002_示例标题二.txt
# ------ ...
# ----124
# ---- ...
#
# 参数（都在 Spider().__init__() 里使用）
# -n 多进程数量（默认为1）
# -id 初始ID（默认为 5000000, 只在 ID.txt 不存在时起作用）


import os
import argparse
from bs4 import BeautifulSoup

from utils import *

# 多进程锁
m_lock = multiprocessing.Lock()


def get_title(html):
    """
    获取帖子标题
    :param html: 网页源码
    :return: 帖子标题
    """
    try:
        soup = BeautifulSoup(html, 'lxml')
        raw_title = soup.find('span', {'class': 's_title'})

        if not raw_title:
            return ''
        title = remove_html_tag(str(raw_title))
        return title
    except Exception as e:
        logging.error('Get title: {}'.format(e))
        return ''


def get_posts_num(html):
    """
    获取帖子页数
    :param html: 网页源码
    :return: 帖子页数
    """
    try:
        soup = BeautifulSoup(html, 'lxml')
        raw_posts_num = soup.find('div', {'class': 'atl-pages'})
        match = re.findall('/post-.+shtml', str(raw_posts_num))
        if match:
            last_num_url = match.pop(-2)
            last_num = re.findall('-[0-9]+.shtml', str(last_num_url))
            return int(last_num[0][1:-6])
        else:
            return 1
    except Exception as e:
        logging.error('Get posts num: '.format(e))
        return 1


# 暂时不需要
def get_floor(content):
    c_content = '<html><body>' + str(content) + '</html></body>'
    try:
        soup = BeautifulSoup(c_content, 'lxml')
        raw_floor = soup.findAll('span', {'class': 'tail-info'})
        f_floor = re.findall('[0-9]+楼', str(raw_floor))
        if f_floor:
            floor = remove_html_tag(str(f_floor[0]))
            return str(floor)
        else:
            return ''
    except Exception as e:
        logging.error('Get floor: {}'.format(e))
        return ''


def get_content(html):
    """
    获取帖子正文内容
    :param html: 网页源码
    :return: 帖子正文
    """
    try:
        soup = BeautifulSoup(html, 'lxml')
        page = soup.find_all('div', {'class': 'atl-item'})
        s = '<body>{}</body>'.format(page)
        soup2 = BeautifulSoup(s, 'lxml')
        content = ''
        for i in soup2.find_all('div', {'class': 'bbs-content'}):
            i = re.sub('@[\s\S]+?<br/>|-', '', str(i))
            if i:
                i = remove_html_tag(i)
                content = content + i.strip() + '\n\n'
        if content:
            return content
        else:
            return ''
    except Exception as e:
        logging.error('Get content: {}'.format(e))
        return ''


class Spider(object):
    def __init__(self):
        self.list_url_queue = multiprocessing.Manager().list()
        self.seed_url = 'http://bbs.tianya.cn/'
        self.forum_board = args.b
        self.post_id_file = args.output + '/temp_{}_ID.txt'.format(self.forum_board)
        self.output_dir = args.output + '/{}_output/'.format(self.forum_board)
        self.all_output_file = args.output + '/{}_all.txt'.format(self.forum_board)
        self.deduplicate_all_file = args.output + '/{}_dedu.txt'.format(self.forum_board)
        self.post_id = int()

        # 初始ID
        self.start_id = args.id

        # 多进程数量
        self.process_num = args.n
        # 一次放多少条帖子链接队列
        self.queue_put_num = 1000

    def load_post_id(self):
        with open(self.post_id_file, 'r') as fr:
            self.post_id = int(fr.read())

    def save_post_id(self, numb):
        with open(self.post_id_file, 'w') as fw:
            fw.write(str(numb))

    def init_post_id(self):
        with m_lock:
            if not args.no_small_file:
                if not os.path.exists(self.output_dir):
                    os.mkdir(self.output_dir)
            if not os.path.exists(self.post_id_file):
                with open(self.post_id_file, 'w') as fw:
                    fw.write(self.start_id)

            self.load_post_id()
            logging.warning('导入 {} 到队列'.format(str(self.post_id)))
            for post_count in range(self.post_id, self.post_id + self.queue_put_num):
                self.list_url_queue.append(post_count)
            self.save_post_id(int(self.post_id + self.queue_put_num))

    def run(self):
        # 主进程函数
        while True:
            try:
                if not self.list_url_queue:
                    self.init_post_id()

                post_id = self.list_url_queue.pop(0)
                post_id_prefix = str(post_id)[:-4]
                if not post_id_prefix:
                    post_id_prefix = '0'

                post_url_without_suffix = '{0}post-{1}-{2}-'.format(self.seed_url, self.forum_board, str(post_id))
                post_url = post_url_without_suffix + '1.shtml'

            except Exception as e:
                logging.critical('取ID问题: {}'.format(e))
                continue

            try:
                post_html = get_html(post_url)
                if not post_html:
                    continue
                post_title = get_title(post_html)
                if not post_title:
                    logging.error('找不到title: {}'.format(post_url))
                    continue
                first_page_content = get_content(post_html)
                if not first_page_content:
                    logging.error('### 帖子无内容 ###: {}'.format(post_url))
                    continue

                all_content = first_page_content
                page_num = get_posts_num(post_html)

                for i in range(page_num):
                    if i != 0:
                        page_url = post_url_without_suffix + str(i + 1) + '.shtml'
                        other_page = get_html(page_url)
                        other_content = get_content(other_page)
                        all_content = all_content + other_content

                dr = re.compile(r'/|[\\]|[ ]|[|]|[:]|[*]|[<]|[>]|[?]|[\']|["]')
                post_title = dr.sub('_', post_title)

                if not args.no_small_file:
                    output_file_path = self.output_dir + str(post_id_prefix) + '/'
                    if not os.path.exists(output_file_path):
                        os.makedirs(output_file_path, exist_ok=True)
                    output_file = output_file_path + str(post_id) + '_' + post_title + '.txt'
                    save_content(output_file, all_content, mode='w')

                if not args.no_nondedu_file:
                    save_content(self.all_output_file, all_content, mode='a')

                if not args.no_dedu_file:
                    deduplicate_save_content(self.deduplicate_all_file, all_content)
                    
                logging.warning('{0} ---{2}--- {1}'.format(post_id, post_title, str(page_num)))

            except Exception as e:
                logging.critical('尚未预料到的错误: {0} | {1}'.format(e, post_url))
                continue

    def start(self):
        # 启动函数
        self.init_post_id()
        time.sleep(3)
        processes = []
        if args.proxy:
            get_proxies = multiprocessing.Process(target=regularly_get_proxy, args=())
            get_proxies.start()
            processes.append(get_proxies)
            time.sleep(3)
        for i in range(self.process_num):
            t = multiprocessing.Process(target=self.run, args=())
            t.start()
            processes.append(t)

        for t in processes:
            t.join()


def test():
    url = 'http://bbs.tianya.cn/post-free-5787657-1.shtml'
    html = get_html(url)
    print(get_posts_num(html))
    print(get_title(html))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', help='多进程数量（默认为1）', type=int, default=1)
    parser.add_argument('-b', help='论坛版块（默认为free(天涯主板)', type=str, default='free')
    parser.add_argument('-id', help='起始ID（默认为5000000）', type=str, default='5000000')
    parser.add_argument('--no_small_file', help='不输出小文件', action='store_true')
    parser.add_argument('--no_nondedu_file', help='不输出未去重大文件', action='store_true')
    parser.add_argument('--no_dedu_file', help='不输出去重后的大文件', action='store_true')
    parser.add_argument('--proxy', help='使用代理', action='store_true')
    parser.add_argument('--log', help='输出log到文件，否则输出到控制台', action='store_true')
    args = parser.parse_args()

    log_flag = args.log
    if log_flag:
        log_flag = args.output + '/log.txt'

    logging.basicConfig(format='%(asctime)s|PID:%(process)d|%(levelname)s: %(message)s',
                        level=logging.WARNING, filename=log_flag)
    spider = Spider()
    spider.start()
    # test()
