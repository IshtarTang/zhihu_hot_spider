知乎热榜的爬虫，默认间隔一分钟爬一次，会接着爬热榜上问题的回答

进度用redis储存，结果存到mysql

#### 启动

分布式的，先启动服务器端`tw_spider_server`，然后启动客户端`zhihu_hot_spider`即可

因为我只有一台电脑，所以我也不知道同时启动多个客户端效果咋样



#### 设置

##### server端

redis和mysql设置

UrlPool的三个参数分别是：在redis中key的前缀；用redis的几号库；host。端口是默认的6379，要改的话在`tw_url_poo`l改

![1644918252807](%E7%AC%94%E8%AE%B0%E5%9B%BE/readme/1644918252807.png)

##### client端

默认连接本地服务器端

![1644918288328](%E7%AC%94%E8%AE%B0%E5%9B%BE/readme/1644918288328.png)



间隔多久爬一次热榜，默认60秒

![1644918449290](%E7%AC%94%E8%AE%B0%E5%9B%BE/readme/1644918449290.png)



默认在爬了热榜之后会接着爬热榜上问题的回答，不需要的话把420行注释掉就行

![1644918532282](%E7%AC%94%E8%AE%B0%E5%9B%BE/readme/1644918532282.png)