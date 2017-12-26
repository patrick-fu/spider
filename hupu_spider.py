# -*- coding： utf-8 -*-
# Created by FFJ on 20170822
#
# 依赖 lxml 库: pip install lxml
#
# 按虎扑帖子 ID 顺序爬取纯文本数据， 每个帖子保存为一个 ID_帖子标题.txt 文件
# ./ID.txt 存放从哪个 ID 开始爬，不存在则ID默认为 5000000
#
# 输出目录结构
# --output
# ----1234
# ------12340001_示例标题一.txt
# ------12340002_示例标题二.txt
# ------ ...
# ----1235
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
    try:
        soup = BeautifulSoup(html, 'lxml')
        raw_title = soup.find('h1')

        if not raw_title:
            return ''
        title = remove_html_tag(str(raw_title))
        return title
    except Exception as e:
        logging.error('Get title: {}'.format(e))
        return ''


def get_posts_num(html):
    try:
        soup = BeautifulSoup(html, 'lxml')
        raw_posts_num = soup.find('div', {'class': 'page'})
        match = re.findall('/[0-9]+-[0-9]+.html', str(raw_posts_num))
        if match:
            last_num_url = match.pop(-2)
            last_num = re.findall('-[0-9]+.html', str(last_num_url))
            return int(last_num[0][1:-5])
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
    try:
        soup = BeautifulSoup(html, 'lxml')
        extract = soup.find_all('blockquote')
        extract.extend(soup.find_all('small'))
        [i.extract() for i in extract]
        raw = soup.find_all('div', {'class': 'quote-content'})
        all_content = '\n\n'.join([re.sub('@.+?[\s]', '', remove_html_tag(str(c)).strip()) for c in raw])
        return all_content
    except Exception as e:
        logging.error('Get content: {}'.format(e))
        return ''


class Spider(object):
    def __init__(self):
        self.list_url_queue = multiprocessing.Manager().list()
        self.seed_url = 'https://bbs.hupu.com/'
        self.post_id_file = args.output + '/temp_ID.txt'
        self.single_output_dir = args.output + '/'
        self.all_output_file = args.output + '.txt'
        self.deduplicate_all_file = args.output + '_deduplication.txt'
        self.proxies_file = 'all_proxies.txt'
        self.post_id = int()

        # 初始ID
        self.start_id = args.id

        # 多进程数量
        self.process_num = args.n
        # 一次放多少条帖子链接队列
        self.queue_put_num = 10000

    def init_post_id(self):
        with m_lock:
            if not os.path.exists(self.single_output_dir):
                os.mkdir(self.single_output_dir)
            if not os.path.exists(self.post_id_file):
                with open(self.post_id_file, 'w') as fw:
                    fw.write(self.start_id)

            with open(self.post_id_file, 'r') as fr:
                self.post_id = int(fr.read())

            logging.warning('导入 {} 到队列'.format(str(self.post_id)))
            for post_count in range(self.post_id, self.post_id + self.queue_put_num):
                self.list_url_queue.append(post_count)

            with open(self.post_id_file, 'w') as fw:
                fw.write(str(self.post_id + self.queue_put_num))

    def run(self):
        while True:
            try:
                if not self.list_url_queue:
                    self.init_post_id()

                post_id = self.list_url_queue.pop(0)
                post_id_prefix = str(post_id)[:-4]
                if not post_id_prefix:
                    post_id_prefix = '0'

                post_url_without_suffix = '{0}{1}'.format(self.seed_url, str(post_id))
                post_url = post_url_without_suffix + '.html'

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
                        page_url = '{0}-{1}.html'.format(post_url_without_suffix, str(i + 1))
                        other_page = get_html(page_url)
                        other_content = get_content(other_page)
                        all_content = all_content + '\n\n' + other_content

                dr = re.compile(r'/|[\\]|[ ]|[|]|[:]|[*]|[<]|[>]|[?]|[\']|["]')
                post_title = dr.sub('_', post_title)
                
                if not args.no_small_file:
                    output_file_path = self.single_output_dir + str(post_id_prefix) + '/'
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('output', help='输出文件夹路径（同时生成同名合并大文件，末尾不要带斜杠）', type=str)
    parser.add_argument('--no_small_file', help='不输出小文件', action='store_true')
    parser.add_argument('--no_nondedu_file', help='不输出未去重大文件', action='store_true')
    parser.add_argument('--no_dedu_file', help='不输出去重后的大文件', action='store_true')
    parser.add_argument('-n', help='多进程数量（默认为1）', type=int, default=1)
    parser.add_argument('--id', help='起始ID（默认为0）', type=str, default='0')
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
