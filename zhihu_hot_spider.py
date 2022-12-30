# coding=utf-8
import aiohttp
import asyncio
import requests
import json
import re
import traceback
import cchardet
import logging
import time
import hashlib
import execjs
from scrapy import Selector
from urllib.parse import unquote

logging.basicConfig(level="INFO", datefmt='%y-%m-%d %H:%M:%S', format="[%(levelname)s] %(asctime)s  %(message)s")


class ZhihuHotSpider:
    def __init__(self, server_host, server_port):
        self.server_host = server_host
        self.server_port = server_port
        self.loop = asyncio.get_event_loop()
        # 队列
        self.queue = asyncio.Queue()
        self.worker = 0
        self.max_worker = 10

        # session
        self.default_headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
        }
        self.loop.run_until_complete(self.make_session(self.loop))
        self.dc0 = ""
        self.dc0_update_time = 0

    def __del__(self):
        self.loop.run_until_complete(self.close_session())

    async def close_session(self):
        await self.session.close()

    async def update_dc0(self):
        """
        不带dc0获取不到正文页，随便请求一个问题就能拿到dc0；隔段时间要更新
        """
        async with self.session.get("https://www.zhihu.com/question/19996225") as response:
            cookies_dc0 = str(response.cookies.get("d_c0"))
            re_dc0 = re.search('d_c0=\"(.*?)\";', cookies_dc0)
            try:
                self.dc0 = re_dc0.group(1)
                self.dc0_update_time = time.time()
            except:
                traceback.print_exc()
                logging.warning("dc0更新失败")

    async def make_session(self, loop):
        self.session = aiohttp.ClientSession(loop=loop, headers=self.default_headers)
        await self.update_dc0()

    async def loop_get_urls(self):
        """
        保证queue中一直有urls，间隔5秒跑一次
        :return:
        """
        while True:
            count = self.max_worker - self.worker
            try:
                url = "http://{}:{}/task?count={}".format(self.server_host, self.server_port, count)
                async with self.session.get(url) as response:
                    text = await response.text()
                    jtext = json.loads(text, encoding="utf-8")
                    urls = jtext.items()

                for kv in urls:
                    await self.queue.put(kv)
                logging.info("{} 条url添加到队列，当前队列长度 {}".format(len(urls), self.queue.qsize()))

            except:
                traceback.print_exc()
            await asyncio.sleep(5)

    async def append_urls(self, urls):
        """
        :param urls: 是个list
        :return:
        """
        if not isinstance(urls, list):
            urls = [urls]

        try:
            server_url = "http://{}:{}/append_urls".format(self.server_host, self.server_port)
            async with self.session.post(server_url, json=urls) as response:
                return response.status
        except:
            traceback.extract_stack()
            return 0

    async def download(self, method, url, *args, **kwargs):
        """
        下载网页
        """
        try:
            async with self.session.request(method, url, **kwargs) as response:
                # print(self.session.cookie_jar.filter_cookies("https://www.zhihu.com/"))
                lxml = await response.read()
                status_code = response.status
                text = lxml.decode(cchardet.detect(lxml)["encoding"])
                real_url = str(response.url)
                if status_code == 200:
                    logging.debug("{}下载完成".format(url))
                else:
                    logging.warning("下载失败 {} - {}".format(status_code, url))
        except Exception as e:
            logging.warning("{} 下载失败 {} {}".format(url, type(e), str(e)))
            text = ""
            status_code = 0
            real_url = url
        return status_code, text, real_url

    def request_downlaad(self, method, url, *args, **kwargs):
        """
        结构跟download是一样的
        aiohttp的cookies解析有问题，cookies里有引号的话会被再加两引号，导致验证失败，有些还是得用同步请求
        """
        try:
            response = requests.request(method, url, **kwargs)
            text = response.content.decode("utf-8")
            status_code = response.status_code
            real_url = str(response.url)
            if status_code == 200:
                logging.debug("下载完成 {}".format(url))
            else:
                logging.warning("下载失败 {} - {}".format(status_code, url))
        except Exception as e:
            logging.warning("下载失败 {} {} {}".format(url, type(e), str(e)))
            text = ""
            status_code = 0
            real_url = url
        return status_code, text, real_url

    async def set_statue(self, url, status_code):
        """
        url状态提交
        :param url:
        :param status_code:
        :return:
        """
        server_url = "http://{}:{}/set_status".format(self.server_host, self.server_port)
        if not isinstance(url, str):
            url = str(url)
        result = {"url": url, "status": status_code}
        try:
            async with self.session.post(server_url, json=result) as response:
                if response.status == 200:
                    logging.debug("{} 状态保存".format(url))
                    return response.status
        except:
            traceback.print_exc()

        logging.warning("{} 状态提交失败".format(url))

    async def set_hubs(self, urls, append=-1, hub_refresh_span=-1):
        data = {"hubs": urls}
        if append != -1:
            data["append"] = append
        if hub_refresh_span != -1:
            data["hub_refresh_span"] = hub_refresh_span
        url = "http://{}:{}/set_hubs".format(self.server_host, self.server_port)
        try:
            async with self.session.post(url, json=data) as response:
                status = response.status
                if status == 200:
                    logging.info("hub 设置成功")
                else:
                    logging.warning("hub设置异常 {}".format(status))
        except:
            logging.warning("hub设置失败")
            traceback.print_exc()

    async def save_to_db(self, table_name: str, kvs: list):
        """
        :param table_name: 表名
        :param kvs: list中为dict，dict的key为字段名，values为值
        :return:
        """
        data = {"table_name": table_name, "kvs": kvs}
        try:
            url = "http://{}:{}/save_to_db".format(self.server_host, self.server_port)
            async with self.session.post(url, json=data) as response:
                status_code = response.status
                if status_code == 200:
                    logging.debug("{} - {} 提交到库".format(table_name, len(kvs)))
                else:
                    logging.warning("statue_code：{} {} - {} 提交".format(status_code, table_name, kvs))

                return response.status
        except:
            traceback.print_exc()
            logging.warning("{} - {} 提交到库失败".format(table_name, kvs))
            return 0

    async def get_hot_rank(self, url):
        """
        :param url:
        :return:
        """
        d_timestamp = time.time()
        # 验证登录状态的cookies，可以是定值
        cookies = {
            "z_c0": "2|1:0|10:1636985798|4:z_c0|92:Mi4xZkRReENnQUFBQUFBTUoxT3NoQUpGQ1lBQUFCZ0FsVk54cmxfWWdDZlZiX0ExX1VWcW85RUJZYVhDSkk4WjlJbVBB|2413507dd083c91c776a937e776543c37c0723035b66803cb57539a1f213fd63"
        }
        status_code, text, real_url = await self.download("GET", url, cookies=cookies)
        selectro = Selector(text=text)
        hot_list = selectro.css("div[class~=HotList-list]>section")
        kvs = []
        question_ids = []
        for question in hot_list:
            # 热度排名，标题，链接，提问详情
            hot_ranking = question.css("div[class~=HotItem-rank]::text").get()
            title = question.css("h2[class~=HotItem-title]::text").get() \
                .replace("\n", "").replace(" ", "").replace("'", "‘")
            hot_metrics = question.css("div[class~=HotItem-metrics]::text").get()
            link = question.css("div.HotItem-content>a::attr(href)").get()
            question_id = link.split("/")[-1]
            question_ids.append(question_id)
            excerpt = question.css("p.HotItem-excerpt::text").get()
            if excerpt:
                excerpt = excerpt.replace("'", "‘")
            field_key = ["hot_ranking", "d_timestamp", "hot_metrics", "title", "link", "excerpt"]
            values = [hot_ranking, d_timestamp, hot_metrics, title, link, excerpt]
            kv = dict(list(zip(field_key, values)))
            kvs.append(kv)
        logging.info("热榜获取成功 - {}".format(question_ids))
        await self.save_to_db("hot_rank", kvs)
        await self.set_statue(url, status_code)
        if real_url != url:
            await self.set_statue(real_url, status_code)
        return question_ids

    def parse_answer(self, answer_info):
        """
        解析回答
        :param line: 一条回答的数据
        :return: 解析出的内容
        """
        html_text = answer_info["content"]
        selector = Selector(text=html_text)

        # 回答正文
        # answer_content = "\n".join(selector.xpath("//p//text()").extract())
        answer_content = "\n".join(selector.css("p ::text,b ::text").extract())
        # 回答中插的图片
        img_list = selector.xpath("//noscript/img/@src").extract()
        img_list_str = "；".join(img_list)
        external_links = selector.xpath("//a/@href").extract()

        # 赞同数
        voteup_count = answer_info["voteup_count"]
        # 回答发表/更新时间
        update_time = answer_info["updated_time"]
        created_time = answer_info["created_time"]
        # 该回答链接
        question_id = answer_info["question"]["id"]
        base_answer_url = "http://www.zhihu.com/question/{}/answer/{}"
        answer_id = answer_info["url"].split("/")[-1]
        answer_url = base_answer_url.format(question_id, answer_id)

        # 外链
        patten = "https://link.zhihu.com/\?target=(.*)"
        external_links_text = selector.css("a[href]::text").extract()
        read_external_links = []
        for external_link in external_links:
            re_search = re.search(patten, external_link)
            if re_search:
                read_external_links.append(unquote(re_search.group(1), "utf-8"))
            else:
                read_external_links.append(unquote(external_link, "utf-8"))

        external_links_str = ""
        for link_info in zip(external_links_text, read_external_links):
            external_links_str += "{} - {}；".format(link_info[0], link_info[1])

        # 评论数
        comment_count = answer_info["comment_count"]
        author_r_info = answer_info["author"]
        # 作者名字
        author_name = author_r_info["name"]
        # 作者简介
        introduction = author_r_info["headline"]
        # 作者主页链接
        user_type = author_r_info["user_type"]
        url_token = author_r_info["url_token"]
        user_url = "https://www.zhihu.com/{}/{}".format(user_type, url_token)
        autho_gender = author_r_info["gender"]
        # 是否广告
        is_advertiser = author_r_info["is_advertiser"]
        # 回答者信息
        autho_info = {
            # "author_name": author_name,
            # "introduction": introduction,
            # "user_url": user_url,
        }

        answer_info = {
            "answer_url": answer_url,
            "content": answer_content,
            "voteup_count": voteup_count,
            "external_links": external_links_str,
            "img_list": img_list_str,
            "created_time": created_time,
            "update_time": update_time,
            "comment_count": comment_count,
            "author_name": author_name,
            "is_advertiser": is_advertiser,
            "author_introduction": introduction,
            "auhtor_user_url": user_url,
            "autho_gender": autho_gender

        }
        return answer_info

    async def save_question(self, url):
        """

        :param url:
        :return:
        """
        c_dc0 = self.session.cookie_jar.filter_cookies("https://www.zhihu.com").get("d_c0")
        re_dc0 = re.search('d_c0="(.*?)"', str(c_dc0))
        try:
            dc0 = re_dc0.group(1)
        except:
            logging.warning("{} 获取dc0失败".format(url))
            await self.set_statue(url, 0)
            return
        headers = self.get_headers(dc0, url)
        r_session = requests.session()
        r_session.headers = headers
        # status_code, text, real_url = await self.download("get", url, headers=headers)
        status_code, text, real_url = self.request_downlaad("get", url, headers=headers)
        download_time = time.time()
        re_1 = re.search("questions/(.*?)/answers.*?(limit=\d+&offset=\d+)", url)
        try:
            question_id = re_1.group(1)
            answer_count = re_1.group(2)
        except:
            question_id = "0"
            answer_count = "limit=0&offset=0"
        logging.debug("{} - {} 返回值长度：{}".format(question_id, answer_count, len(text)))
        try:
            j_text = json.loads(text)
        except:
            logging.info("问题 {} - {} 未获取到数据".format(question_id, answer_count))
            await self.set_statue(url, status_code)
            return
        answers = j_text.get("data", {})

        # 处理回答

        answer_info_dicts = []
        for answer in answers:
            answer_info_dict = self.parse_answer(answer)
            answer_info_dict["question_id"] = question_id
            answer_info_dict["download_time"] = download_time

            answer_info_dicts.append(answer_info_dict)
        await self.save_to_db("answers", answer_info_dicts)

        paging_info = j_text["paging"]
        if not paging_info.get("is_end", True):
            next_url = paging_info["next"]
            await self.append_urls([next_url])

        logging.info("问题 {} - {} 解析完成，{} 条回答".format(question_id, answer_count, len(answers)))

        await self.set_statue(url, status_code)
        if real_url != url:
            await self.set_statue(real_url, status_code)

    def get_headers(self, dc0, url):
        """
        执行js加密，获得加密过的请求头
        :param dc0:
        :param url:
        :return:
        """
        x81 = "3_2.0aR_sn77yn6O92wOB8hPZnQr0EMYxc4f18wNBUgpTk7tu6L2qK6P0ET9y-LS9-hp1DufI-we8gGHPgJO1xuPZ0GxCTJHR7820XM20cLRGDJXfgGCBxupMuD_Ie8FL7AtqM6O1VDQyQ6nxrRPCHukMoCXBEgOsiRP0XL2ZUBXmDDV9qhnyTXFMnXcTF_ntRueTh_29ZrL96Tes_bNMggpmtbSYobSYIuw9_BYBkXOK8gYBCqSm-C20ZueBtGLPvM38TBHKCrXm6HSX8BHm17X1SLH0SrLV6QOKKLH9JQ9Z-JOpxDCmiCLZkJVmegL1LJL1t9YKOh3LbDpV9hcGqUxmOC30wqoKABVZgBVfJ4o9sJx18JN95qfzWUL8pB3xHGSMbgYLeg90X9V8QU2qNUoVcH3qPBCBSLY_18F_OcV_zwpOfrxKwweCUgV1swxmVBCZECV8xcw1DC2xbiVChwC1TvwLkCLBOcHMuwLCe8eC"
        re_rehref = re.search("https://www.zhihu.com/api/v4/questions/(.*?)/answers", url)
        try:
            question_id = re_rehref.group(1)
        except:
            question_id = "https://www.zhihu.com"

        dc0 = '"' + dc0 + '"'
        url = url.replace("https://www.zhihu.com", "")
        code1 = "+".join(["101_3_2.0", url, dc0, x81])
        md5_code = hashlib.md5(bytes(code1, encoding="utf8")).hexdigest()
        ctx2 = execjs.compile(open('zh_96.js', 'r', encoding="utf-8").read())
        encrypt_str = "2.0_" + ctx2.call('b', md5_code)
        headers = {'Host': 'www.zhihu.com', 'Connection': 'keep-alive', 'Pragma': 'no-cache',
                   'Cache-Control': 'no-cache',
                   'x-zse-93': '101_3_2.0', 'sec-ch-ua-mobile': '?0',
                   'User-Agent': 'Mozilla/5.0(WindowsNT10.0;Win64;x64)AppleWebKit/537.36(KHTML,likeGecko)Chrome/98.0.4758.82Safari/537.36',
                   'x-zst-81': x81,
                   'x-requested-with': 'fetch', 'x-zse-96': encrypt_str,
                   'sec-ch-ua': '"NotA;Brand";v="99","Chromium";v="98","GoogleChrome";v="98"',
                   'sec-ch-ua-platform': '"Windows"',
                   'Accept': '*/*',
                   'Sec-Fetch-Site': 'same-origin',
                   'Sec-Fetch-Mode': 'cors',
                   'Sec-Fetch-Dest': 'empty',
                   'Referer https': '//www.zhihu.com/question/{}'.format(question_id),
                   'Accept-Encoding': 'gzip,deflate',
                   'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7',
                   'Cookie': 'd_c0={};'.format(dc0)}
        return headers

    async def process(self, url, url_level):
        if url_level == 1:
            question_ids = await self.get_hot_rank(url)
            question_urls = []
            get_num = 20
            base_url = 'https://www.zhihu.com/api/v4/questions/{}/answers?include=data%5B%2A%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cattachment%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time%2Cupdated_time%2Creview_info%2Crelevant_info%2Cquestion%2Cexcerpt%2Cis_labeled%2Cpaid_info%2Cpaid_info_content%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cis_recognized%3Bdata%5B%2A%5D.mark_infos%5B%2A%5D.url%3Bdata%5B%2A%5D.author.follower_count%2Cvip_info%2Cbadge%5B%2A%5D.topics%3Bdata%5B%2A%5D.settings.table_of_content.enabled&limit={}&offset=0&platform=desktop&sort_by=default'
            for question_id in question_ids:
                question_url = base_url.format(question_id, get_num)
                question_urls.append(question_url)
            await self.append_urls(question_urls)
        elif url_level == 0:
            await self.save_question(url)
        self.worker -= 1

    async def _run(self):
        asyncio.ensure_future(self.loop_get_urls())
        await self.set_hubs(["https://www.zhihu.com/hot"], 60)
        while True:
            url, url_level = await self.queue.get()
            asyncio.ensure_future(self.process(url, url_level))
            self.worker += 1
            if self.worker >= self.max_worker:
                logging.info("worker满，暂停3秒")
                await asyncio.sleep(3)

    def run(self):
        self.loop.run_until_complete(self._run())


if __name__ == '__main__':
    spider = ZhihuHotSpider("localhost", 8080)
    spider.run()
