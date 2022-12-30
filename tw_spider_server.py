# coding=utf-8
from sanic import Sanic, response
from tw_url_pool import UrlPool
import logging
import twPymysql
import traceback
import sanic

app = Sanic(__name__)
urlpool = UrlPool("zhihu_hot", 1, "127.0.0.1")
mysql_conn = twPymysql.Connection("127.0.0.1", "root", "root", "zhihu")
logging.basicConfig(level="INFO", datefmt='%y-%m-%d %H:%M:%S', format="[%(levelname)s] %(asctime)s  %(message)s")
print("初始化完成")


@app.listener("after_server_stop")
async def cache_urlpool(app, loop):
    global urlpool
    logging.info("url 缓存")
    del urlpool  # 删除会自动存到redis里
    logging.info("exit!")


@app.route("/task")
async def get_urls(request):
    """
    从url池里获取url
    """
    count = request.args.get("count", 10)
    try:
        count = int(count)
    except:
        count = 10
    urls = urlpool.pop(count)
    return response.json(urls)


@app.route("/set_status", methods=["POST", ])
async def set_url_status(request):
    """
    设置url状态
    """
    result = request.json
    urlpool.set_status(result["url"], result["status"])

    return response.text("ok")


@app.route("/set_hubs", methods=["POST", ])
async def set_hubs(request):
    """
    设置需重复爬的url
    """
    args = []
    data = request.json
    hubs = data.get("hubs", "[]")
    if not hubs:
        return response.text("无hubs")
    args.append(hubs)

    is_append = data.get("append", -1)
    if is_append != -1:
        args.append(is_append)

    hub_refresh_span = data.get("hub_refresh_span", 0)
    if hub_refresh_span:
        args.append(hub_refresh_span)

    urlpool.set_hubs(*args)
    logging.info("hub更新完成")
    return response.text("ok")


@app.route("/append_urls", methods=["POST", ])
async def append_to_pool(request):
    """
    向url池添加url
    """
    urls = request.json
    urlpool.add_urls(urls)
    return response.text("append succes")


@app.route("/append_urls_without_filter", methods=["POST", ])
async def append_urls_without_filter(request):
    """
    向url池添加，但不过滤
    """
    urls = request.json
    urlpool.add_urls(urls, True)
    return response.text("append succes")


@app.route("/save_to_db", methods=["POST", ])
async def save_to_db(request):
    """
    存数据库
    """
    j_text = request.json  # {"table_name": table_name, "kvs": kvs}

    table_name = j_text["table_name"]
    kvs = j_text["kvs"]

    logging.debug("准被插入库，表名：{} 条数 ：{}".format(table_name, len(kvs)))
    for kv in kvs:
        url = kv.get("link", kv)
        try:
            mysql_conn.table_insert(table_name, kv)
        except:
            logging.warning("{} 插入数据库失败".format(url))
            traceback.print_exc()
            return response.text("fail")
    logging.info("插入数据库成功，表名：{} 条数 ：{}".format(table_name, len(kvs)))
    return response.text("done")


@app.route("/test", methods=["get", ])
async def test(request: sanic.request.Request):
    print(request.headers)
    print(request.headers.get("cookie"))
    print("-------------")
    return response.text("ok")


if __name__ == '__main__':
    app.run(
        host="0.0.0.0",
        port=8080,
        debug=False,
        access_log=False,
        workers=1)
