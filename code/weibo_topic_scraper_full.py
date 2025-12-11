import csv
import os
import random
import requests
from lxml import etree
from urllib.parse import quote
import datetime
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------- 核心优化1：会话保持+重试机制 ----------------------
session = requests.Session()
# 重试策略（解决请求偶尔失败）
retry_strategy = Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[403, 404, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

# ---------------------- 核心优化2：补全请求头（模拟真实浏览器） ----------------------
headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'sec-ch-ua': '"Chromium";v="142", "Not_A Brand";v="99", "Google Chrome";v="142"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
    # 新增关键字段：Referer+Origin（微博验证来源）
    'Referer': 'https://s.weibo.com/',
    'Origin': 'https://s.weibo.com',
    # 替换成你最新的Cookie（必须是登录后30分钟内的）
    'cookie': 'SCF=Ao2btD0Fdevtu967FRq79M1arAK1oh2oEPJyTvhwZzCPo00QX5uTE9TEQisrqd8v5Cxu57_maEh1yyegioFqIxs.; SINAGLOBAL=412063297926.81903.1763117938433; ULV=1763378530830:3:3:2:4764579778546.668.1763378530829:1763280982202; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWiWNH2bN.e7c9O7mdvo6vW5JpX5KMhUgL.Fo-fe0epSheEeo52dJLoIpjLxK-LBo5L12BLxK-L1K2LBoeLxKBLB.BLBK5t; ALF=1767794406; SUB=_2A25EMqm2DeRhGeNL6FEQ9C3OyTyIHXVnMaN-rDV8PUJbkNANLUHWkW1NSQCHeU-U1lUutceUlWWW-daZHdkpUGGr; WBPSESS=WijKwpBt0lPglW3tdNkGFLrIF1zJqOssyuCb_jHkusuZKAYTU5hbCK7qcKagCNc764XRrLHzcrf6hkMIHEfXkqITC1YF9dpQU99aZtZ8X9n1ddYsg73sh-mSwjpKldM0AzliOCm8okgQkMUm3zGhfQ==; XSRF-TOKEN=BW6nXV74Gg__VKUy6EN1YLnd',
}
session.headers.update(headers)

# ---------------------- 配置参数（调整关键参数） ----------------------
topic = '罗永浩西贝'
start_time = '2025-09-10-00'
end_time = '2025-12-01-00'
hour_interval = 12  # 缩小时间粒度，提升匹配率
max_pages_per_period = 50  # 微博分页超过50页必空，降低上限
# 随机延迟（模拟人工操作）
delay_range = (3, 8)

# 有写权限的保存目录
SAVE_DIR = os.path.join(os.path.expanduser("~"), "weibo_results")
if not os.path.exists(SAVE_DIR):
    os.makedirs(SAVE_DIR)

def get_hour_periods(start_datetime, end_datetime, interval_hours):
    """分割时间段（缩小粒度）"""
    periods = []
    start_dt = datetime.datetime.strptime(start_datetime, "%Y-%m-%d-%H")
    end_dt = datetime.datetime.strptime(end_datetime, "%Y-%m-%d-%H")
    current_dt = start_dt
    while current_dt < end_dt:
        next_dt = current_dt + datetime.timedelta(hours=interval_hours)
        period_end_dt = min(next_dt, end_dt)
        periods.append((
            current_dt.strftime("%Y-%m-%d-%H"),
            period_end_dt.strftime("%Y-%m-%d-%H")
        ))
        current_dt = next_dt
    return periods

def crawl_weibo_by_period(topic, start_date, end_date, max_pages):
    weibo_count = 0
    fileName = topic.replace('#', '').replace(' ', '_')
    file_path = f"{SAVE_DIR}/{fileName}_全量_{start_date}_{end_date}.csv"

    # 表头（保留完整字段）
    header = ["user_name", "content", "tag", "location", "created_at", 
              "repost_counts", "comment_counts", "like_counts", "weibo_link"]
    file_exists = os.path.isfile(file_path)
    f = open(file_path, "a", encoding="utf-8-sig", newline="")
    writer = csv.DictWriter(f, header)
    if not file_exists:
        writer.writeheader()

    # ---------------------- 核心优化3：调整搜索参数（去掉原创限制） ----------------------
    timescope = f"custom:{start_date}:{end_date}"
    # 去掉scope=ori（原创限制），保留suball=1（全量内容）
    base_url_template = 'https://s.weibo.com/weibo?q={}&suball=1&timescope={}&Refer=g&page={}'
    print(f"开始爬取: {topic} | 时间: {start_date} - {end_date}")

    page = 0
    last_weibo_ids = []  # 检测重复内容（微博分页返回重复则停止）
    while True:
        page += 1
        if page > max_pages:
            print(f"达到最大页数 {max_pages}，停止爬取")
            break
        
        # 构造URL（编码关键词）
        encoded_topic = quote(topic)
        encoded_timescope = quote(timescope)
        url = base_url_template.format(encoded_topic, encoded_timescope, page)
        print(f"爬取第 {page} 页: {url}")

        try:
            # 随机延迟（模拟人工）
            time.sleep(random.uniform(*delay_range))
            # 发送请求（用session保持登录态）
            response = session.get(url, timeout=15)
            response.raise_for_status()  # 触发HTTP错误
            html = etree.HTML(response.text, parser=etree.HTMLParser(encoding='utf-8'))

            # 检测无结果
            no_result = html.xpath('//img[@class="no-result"]')
            if no_result:
                print(f"该时间段无更多结果 | 已爬 {weibo_count} 条")
                break

            # 提取微博卡片（兼容新版微博DOM）
            articles = html.xpath('//div[contains(@class, "card-wrap") and @action-type="feed_list_item"]')
            if not articles:
                print(f"第 {page} 页无有效内容，停止")
                break

            current_weibo_ids = []
            for article in articles:
                try:
                    # 提取微博ID（去重）
                    weibo_id = article.xpath('./@mid') or article.xpath('./@data-mid')
                    if not weibo_id:
                        continue
                    weibo_id = weibo_id[0]
                    current_weibo_ids.append(weibo_id)

                    # 去重：如果ID已存在，说明微博返回重复内容
                    if weibo_id in last_weibo_ids:
                        print(f"检测到重复内容，停止分页")
                        break

                    # 解析用户名称（兼容新版样式）
                    user_name = article.xpath('.//a[@class="name" or @nick-name]/@nick-name') or \
                                article.xpath('.//span[@class="nick-name"]/text()')
                    user_name = user_name[0].strip() if user_name else "无用户名"

                    # 解析内容（兼容展开/未展开）
                    content = article.xpath('.//p[@node-type="feed_list_content"]//text()') or \
                              article.xpath('.//p[@node-type="feed_list_content_full"]//text()')
                    content = "".join([c.strip() for c in content]).replace('\n', '').replace(' ', '')

                    # 解析话题标签
                    tags = article.xpath('.//a[contains(@href, "weibo?q=%23")]/text()')
                    tags = "，".join(list(set(tags))) if tags else ""

                    # 解析位置
                    location = article.xpath('.//a[contains(@href, "location") or contains(@href, "pages/place")]/text()')
                    location = location[0].strip().lstrip('·') if location else ""

                    # 解析发布时间
                    created_at = article.xpath('.//a[@class="time" or @node-type="feed_list_item_date"]/text()')
                    created_at = created_at[0].strip() if created_at else ""

                    # 解析互动数据（兼容新版样式）
                    repost = article.xpath('.//span[@class="woo-box-flex woo-retweet-count"]/text()') or \
                             article.xpath('.//li[contains(@class, "retweet")]/a/text()')
                    repost = repost[0].strip().replace('转发', '0') if repost else "0"

                    comment = article.xpath('.//span[@class="woo-box-flex woo-comment-count"]/text()') or \
                              article.xpath('.//li[contains(@class, "comment")]/a/text()')
                    comment = comment[0].strip().replace('评论', '0') if comment else "0"

                    like = article.xpath('.//span[@class="woo-like-count"]/text()') or \
                           article.xpath('.//li[contains(@class, "like")]/a/text()')
                    like = like[-1].strip().replace('赞', '0') if like else "0"

                    # 解析微博链接
                    weibo_link = f"https://weibo.com/{weibo_id}" if weibo_id else ""

                    # 写入数据
                    writer.writerow({
                        "user_name": user_name,
                        "content": content,
                        "tag": tags,
                        "location": location,
                        "created_at": created_at,
                        "repost_counts": repost,
                        "comment_counts": comment,
                        "like_counts": like,
                        "weibo_link": weibo_link
                    })
                    weibo_count += 1

                except Exception as e:
                    print(f"解析单条微博出错: {str(e)}")
                    continue

            # 更新去重ID列表
            last_weibo_ids.extend(current_weibo_ids)
            # 随机延迟（避免高频）
            time.sleep(random.uniform(*delay_range))

        except requests.exceptions.RequestException as e:
            print(f"请求出错: {str(e)} | 重试下一页")
            continue

    f.close()
    print(f"时间段 {start_date}-{end_date} 爬取完成 | 共 {weibo_count} 条")
    return weibo_count

def save_statistics(period_stats):
    """保存统计结果"""
    stats_file = f"{SAVE_DIR}/{topic.replace(' ', '_')}_爬取统计_全量.csv"
    with open(stats_file, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["起始时间", "结束时间", "爬取数量"])
        writer.writeheader()
        for (start, end), count in period_stats.items():
            writer.writerow({"起始时间": start, "结束时间": end, "爬取数量": count})
    print(f"统计文件已保存: {stats_file}")

def main():
    total = 0
    period_stats = {}
    periods = get_hour_periods(start_time, end_time, hour_interval)
    print(f"共分割为 {len(periods)} 个时间段，开始全量爬取...")

    for i, (start, end) in enumerate(periods):
        print(f"\n========== 第 {i+1}/{len(periods)} 个时间段 ==========")
        count = crawl_weibo_by_period(topic, start, end, max_pages_per_period)
        period_stats[(start, end)] = count
        total += count
        # 大间隔休息（每爬5个时间段休10秒）
        if (i+1) % 5 == 0:
            print(f"爬取5个时间段，休息10秒...")
            time.sleep(10)

    print(f"\n========== 爬取完成 ==========")
    print(f"总计爬取: {total} 条")
    print("各时间段详情:")
    for (start, end), count in period_stats.items():
        print(f"{start} - {end}: {count} 条")
    save_statistics(period_stats)

if __name__ == "__main__":
    # 预热请求（模拟人工先打开微博首页）
    session.get("https://s.weibo.com/", timeout=10)
    time.sleep(2)
    main()