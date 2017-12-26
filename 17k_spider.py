#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Created by FFJ on 17-08-18

import os
import argparse
from bs4 import BeautifulSoup
from utils import *

# 多进程的锁
m_lock = multiprocessing.Lock


def get_books_links(html):
    matches = re.findall('book/[0-9]+?.html', html)
    links = []
    if matches:
        for link in matches:
            links.append(link[5:-5])
    return links


def get_chapters_links(html):
    matches = re.findall('chapter/.+?.html', html)
    links = []
    if matches:
        for link in matches:
            links.append(link)
    return links


def get_title(html):
    try:
        soup = BeautifulSoup(html, 'lxml')
        title = remove_html_tag(str(soup.find('h1')))
        return title
    except Exception as e:
        logging.warning('Get title: {}'.format(e))
        return ''


def get_text(html):
    try:
        soup = BeautifulSoup(html, 'lxml')
        question = soup.findAll('div', {'class': "p"})
        if len(question) == 0:
            return ''

        raw_zw = re.findall('<div class="p">[\s\S]+本书首发来自17K小说网，第一时间看正版内容！<br/>',
                            str(question))
        if raw_zw:
            s = '<html><body>' + str(raw_zw[0]) + '</body></html>'
            soup2 = BeautifulSoup(s, 'lxml')
            text = ''
            for i in soup2.find_all('div'):
                i = remove_html_tag(str(i))
                text += i.strip()
            return text[:-23]
        else:
            return ''
    except Exception as e:
        logging.warning('Get text: {}'.format(e))
        return ''


def is_vip_book(html):
    if 'ellipsis vip' in html:
        return True
    else:
        return False


class Spider(object):

    def __init__(self):
        self.seed_url = 'http://www.17k.com/'
        self.list_url_queue = multiprocessing.Manager().list()
        self.list_number_file = args.output + '/temp_list_number.txt'
        self.list_number = int()
        self.output_path = args.output + '/output/'

        # 多进程数量
        self.process_num = args.n

    def load_list_number(self):
        with open(self.list_number_file, 'r') as fr:
            for line in fr.readlines():
                self.list_url_queue.append(int(line))
        logging.info('读取books number完成, 一共 {} 本'.format(len(self.list_url_queue)))

    def run(self):
        while True:
            if not self.list_url_queue:
                logging.warning('爬虫任务完成')
                break

            book_count = self.list_url_queue.pop(0)
            book_list_url = self.seed_url + "list/" + str(book_count) + ".html"

            try:
                book_list_html = get_html(book_list_url)
                if not book_list_html:
                    logging.warning('Can not get book list html, jump: {}'.format(book_list_url))
                    continue

                book_title = get_title(book_list_html)
                if not book_title:
                    logging.warning('No title: {}'.format(book_list_url))
                    continue

                if is_vip_book(book_list_html):
                    logging.warning('VIP book, jump: {}'.format(book_list_url))
                    continue

                all_chapter_links = get_chapters_links(book_list_html)
                if not all_chapter_links:
                    logging.warning('No chapter links: {}'.format(book_list_url))
                    continue

                logging.info('{0}: {1} | 章节数：{2} | 剩余待爬：{3} 本'.format(str(book_count), book_title,
                                                                      str(len(all_chapter_links)),
                                                                      len(self.list_url_queue)))
                output_file = self.output_path + str(book_count) + '_' + book_title + '.txt'

                for chapter_suffix in all_chapter_links:
                    chapter_url = self.seed_url + chapter_suffix
                    chapter_html = get_html(chapter_url)
                    chapter_text = get_text(chapter_html)

                    if not chapter_text:
                        logging.warning('No text: {0}: {1}'.format(book_title, chapter_url))
                        continue

                    with open(output_file, 'a') as fw:
                        fw.write(chapter_text + '\n')

            except Exception as e:
                logging.warning('Run: {0}: {1}'.format(e, book_list_url))
                continue

    def init_special_books_links(self):
        check_repeat = []
        with open(self.list_number_file, 'w') as fw:
            for i in range(1, 51):
                if i % 10 == 0:
                    logging.info('已载入{}页'.format(str(i)))
                page_url = 'http://all.17k.com/lib/book/2_14_0_0_0_1_1_0_{}.html?'.format(str(i))
                page_html = get_html(page_url)
                one_page_books_links = get_books_links(page_html)
                if one_page_books_links:
                    for books_num in one_page_books_links:
                        if books_num not in check_repeat:
                            fw.write(books_num + '\n')
                            check_repeat.append(books_num)
        logging.info('获取完成')
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)

    def start(self):
        self.load_list_number()
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
    spider.init_special_books_links()
    spider.start()
