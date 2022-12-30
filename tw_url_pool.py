# coding=utf-8
import redis
import time
import json
import logging
import urllib.parse as urlparse

logging.basicConfig(level="INFO", datefmt='%y-%m-%d %H:%M:%S', format="[%(levelname)s] %(asctime)s  %(message)s")


class UrlDB:
    status_failure = b'0'
    status_success = b'1'

    def __init__(self, db_id=0, host="127.0.0.1"):
        # self.connPool = redis.ConnectionPool(host="127.0.0.1", port=6379, decode_responses=True)
        self.db = redis.StrictRedis(host=host, port=6379, db=db_id, decode_responses=True)

    def set_success(self, url):
        try:
            self.db.set(url, self.status_success)
            flag = True
        except:
            flag = False
        return flag

    def set_failure(self, url):
        try:
            self.db.set(url, self.status_failure)
            flag = True
        except:
            flag = False
        return flag

    def has(self, url):
        if isinstance(url, str) or isinstance(url, bytes):
            try:
                result = self.db.get(url)
                return result
            except:
                pass
        return False

    def set(self, key, value):
        """
        外层记得try
        """
        self.db.set(key, value)

    def get(self, key, default):
        # 这个也记得try
        result = self.db.get(key)
        return result


class UrlPool:
    def __init__(self, pool_name, db_id=0, host="127.0.01"):
        self.name = pool_name
        self.db = UrlDB(db_id, host)

        self.hub_pool = {}  # {url: last_query_time, }  存放hub url
        self.waiting = {}  # {host: set([urls]),} 等待下载的url按host分组
        self.pending = {}  # {url: pended_time,} 记录已被取出（self.pop()）但还未被更新状态（正在下载）的URL，pended_time为取出时间
        self.failure = {}  # {url: times,} 记录失败的URL的次数

        self.pending_key = self.name + "_pending"
        self.waiting_key = self.name + "_waiting"
        self.hub_key = self.name + "_hub"

        self.failure_threshold = 3  # 最大失败次数
        self.pending_threshold = 10  # pending最大时间，过期会重新放入waitiog
        self.waiting_count = 0  # self.waiting中总url数量
        self.hub_refresh_span = 60
        self.load_cache()

    def __del__(self):

        flag = self.dump_schedule_list()
        if flag:
            logging.info("进度已保存")

    def load_cache(self):
        try:

            # 读waiting
            waiting_str = self.db.get(self.waiting_key, {})
            count = 0
            if waiting_str:
                # waiting的结构是{host:set()}，不能用json.load()
                waiting = eval(waiting_str)
                self.waiting = waiting
                for key in self.waiting:
                    count += len(self.waiting[key])
                self.waiting_count = count

            # 读pending
            pending_str = self.db.get(self.pending_key, {})
            pending_urls = []
            if pending_str:
                pending_str = pending_str.replace("'", '"')
                pending_urls = json.loads(pending_str)
                self.add_urls(list(pending_urls.keys()))

            # 读hub
            hub_str = self.db.get(self.hub_key, {})

            if hub_str:
                hub_str = hub_str.replace("'", '"')
                self.hub_pool = json.loads(hub_str)

            logging.info("读取到库中hub {} 条，waiting {} 条，pending {} 条，pending已重新加入waiting中".format(
                len(self.hub_pool), count, len(pending_urls), ))
        except:
            logging.warning("读取上次记录的过程出现问题")

    def dump_schedule_list(self):
        try:
            self.db.set(self.pending_key, str(self.pending))
            self.db.set(self.waiting_key, str(self.waiting))
            self.db.set(self.hub_key, str(self.hub_pool))
            logging.debug("进度保存成功")
            return True
        except:
            logging.warning("pending/waiting 保存失败")
            return False

    def set_hubs(self, urls, append=0, hub_refresh_span=60):

        self.hub_refresh_span = hub_refresh_span
        if not append:
            self.hub_pool = {}
        for url in urls:
            self.hub_pool[url] = 0
        try:
            self.db.set(self.hub_key, str(self.hub_pool))
            logging.info("hub 保存完成")
        except:
            logging.warning("hub 保存失败")

    def set_status(self, url, state_code):
        if url in self.pending:
            self.pending.pop(url)
        # 状态码200或404，确认成功/不可获取
        if state_code == 200:
            self.db.set_success(url)
            logging.debug(" 状态码 {}  {}".format(state_code, url))
        elif state_code == 404:
            self.db.set_failure(url)
            logging.debug("状态码 {} {}  设置完成".format(state_code, url))
        else:
            # 其他状态码，塞到wating里重下，超过设定次数确定为失败
            if url not in self.failure:
                self.failure[url] = 1
                self.add_url(url)
                logging.info("{} 状态码异常{}，重新下载，次数 1".format(url, state_code))
            else:
                self.failure[url] += 1
                if self.failure[url] > self.failure_threshold:
                    self.db.set_failure(url)
                    self.failure.pop(url)
                    logging.warning("{} 状态码异常{}，次数 {}，设为失败".format(url, state_code))

                else:
                    self.add_url(url)
                    logging.info("{} 状态码异常 {}，次数 {}，重新下载".format(url, state_code, self.failure[url]))

        self.dump_schedule_list()

    def __push_to_pool(self, url, host):
        if host not in self.waiting:
            self.waiting[host] = set()
        self.waiting[host].add(url)
        self.waiting_count += 1

    def url_verdict(self, url, not_filter=False):
        """
        判断url是否需要放入waiting
        :param url:
        :param not_filter: 是否过滤
        :return: 是否放入waiting，host，message
        """
        # url格式是否合法
        host = urlparse.urlparse(url).netloc
        if not host or "." not in host:
            return False, "0.0.0.0", "url {} 格式错误 ".format(url)

        # 是否已在waiting中
        if host in self.waiting and url in self.waiting[host]:
            return False, host, "{} 已在waiting中".format(url)

        if not_filter:
            return True, host, "{} 未过滤放入waiting".format(url)

        # 是否下载过
        if self.db.has(url):
            return False, host, "{} 已下载过".format(url)

        # 是否在预备列表
        pended_time = self.pending.get(url, 0)
        if pended_time:
            if time.time() - pended_time < self.pending_threshold:
                return False, host, "url {} 正等待下载，不放入".format(url)
            else:
                self.pending.pop(url)
                return True, host, "{} 下载超时，重新放入".format(url)

        return True, host, "{} 正常加入waiting".format(url)

    def add_urls(self, urls, not_filter=False):
        """
        添加url到waiting
        :param urls:
        :param not_filter:
        :return:
        """
        append_count = 0
        if isinstance(urls, str):
            urls = [urls]
        for url in urls:
            flag, host, message = self.url_verdict(url, not_filter)
            if flag:
                logging.debug(message)
                self.__push_to_pool(url, host)
                append_count += 1
            else:
                logging.debug(message)
        logging.info("收到url {} 个，{} 个添加到waiting，池中总url数：{}".format(len(urls), append_count, self.waiting_count))

    add_url = add_urls

    def pop(self, count, hub_percent=50):
        """
        :param count:
        :param hub_percent:
        :return:{url:url_level(hub=1，普通=0)}
        """
        url_attr_url = 0
        url_attr_hub = 1
        pop_hubs = {}
        hub_count = count * hub_percent // 100

        # 取hub url
        for hub in self.hub_pool:
            # 如果该hub上次爬取时间超过设定的间隔则再次放入到pended中
            span = time.time() - self.hub_pool[hub]
            if span > self.hub_refresh_span:
                pop_hubs[hub] = url_attr_hub
                self.hub_pool[hub] = time.time()
            else:
                continue
            if len(pop_hubs) > hub_count:
                break

        # 取普通url
        left_count = count - len(pop_hubs)
        urls = {}
        cycle_count = 0
        while len(urls) < left_count and cycle_count < 5:
            cycle_count += 1
            for host in self.waiting:
                if not self.waiting[host]:
                    continue
                url = self.waiting[host].pop()
                urls[url] = url_attr_url
                self.pending[url] = time.time()
                if len(urls) > left_count:
                    break
        self.waiting_count -= len(urls)
        logging.info(
            "请求{} 实际pop {} hubs：{}  urls：{}，池中剩余 {}".format(count, len(urls) + len(pop_hubs), len(pop_hubs), len(urls),
                                                            self.size()))
        urls.update(pop_hubs)
        return urls

    def size(self):
        return self.waiting_count

    def empty(self):
        return self.waiting_count == 0
