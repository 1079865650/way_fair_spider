import queue
import threading
import json
import time
import arrow
import requests
import re
import difflib
import psycopg2
from db_helper import db_helper

# 172.31.6.162 5432    dbspider   Xr6!g9I@p5
PG_INFO = {
    "host": "172.31.6.162",
    "port": 5432,
    "user": "dbspider",
    "passwd": "Xr6!g9I@p5",
    "db": "bidata",
}

def init_pg(pg_info):
    connect = psycopg2.connect(
        host = pg_info["host"],
        database = pg_info["db"],
        user = pg_info["user"],
        password = pg_info["passwd"],
        port = pg_info["port"]
    )
    connect.autocommit = True
    # 创建一个cursor来执行数据库的操作
    cur = connect.cursor()
    return connect, cur

#pg_conn, pg_cur = init_pg(PG_INFO)

def re_reconnect_pg():
    global pg_conn, pg_cur
    try:
        pg_cur.execute("select 1")
    except Exception as e:
        pg_conn, pg_cur = init_pg(PG_INFO)

def getHtml(url):  # url从哪里拿
    resp = getResp(url)
    html = resp.content.decode("utf-8") if resp else ""
    return html

def getResp(url, retry_times = 0):
    headers = {
        "Connection": "keep-alive",
        "sec-ch-ua": '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Sec-Fetch-Site": "none",
        "Accept-Encoding": "gzip, deflate",
        "accept-language": "zh-CN,zh;q=0.9",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
    }
    resp = None
    try:
        resp = requests.get(
            url=url,
            headers=headers,
            timeout=15,
        )
    except Exception as e:
        print("get resp error " + str(e))
        retry_times = retry_times + 1
        if retry_times < 3:
            time.sleep(10)
            return getResp(url, retry_times)
    return resp

def getJson(url, jsonobj, otherHeaders = {}, retry_times = 0):
    headers = {
        "sec-ch-ua": '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36",
        "Accept": "application/json",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "use-web-hash": "true",
        "Accept-Encoding": "gzip, deflate",
        "accept-language": "zh-CN,zh;q=0.9",
        "content-type": "application/json",
        "apollographql-client-name": "@wayfair/sf-ui-product-details",
        "apollographql-client-version": "721593e1e1135c1350664130d0cb0d8746976114",
        "x-parent-txid": "I/bHHmNaS3yOTQCmb9tVAg==",
    }
    headers.update(otherHeaders)
    json = None
    try:
        req = requests.post(
            url=url,
            headers=headers,
            timeout=15,
            json=jsonobj
            #cookies=cookies
        )
        json = req.content.decode("utf-8")
    except Exception as e:
        print("get json error " + str(e))
        retry_times = retry_times + 1
        if retry_times < 3:
            time.sleep(10)
            return getJson(url, jsonobj, otherHeaders, retry_times)
    return json

# 清空排名
def clearRank(url):
    #re_reconnect_pg()
    with db_helper.get_resource() as (cursor, _):
        cursor.execute( """update spider.wayfair_listing set rank=0 where category_url=%s""", [ url ])

# 获取listing列表粗略信息
def spiderWayfair(site, category_cn, url, page):
    category_url = url
    if page > 1:
        url = url + "&curpage=" + str(page) if "?" in category_url else url + "?curpage=" + str(page)
    html = getHtml(url)
    if html == "":
        print("get html empty")
        return
    # 正则表达式 re.findall
    strs = re.findall(r"\"WEBPACK_ENTRY_DATA\"]=(.*?);<\/script>", html)
    obj = json.loads(strs[0])
    list = obj["application"]["props"]["browse"]["browse_grid_objects"]
    category_id = obj["application"]["props"]["browse"]["category_id"]
    skus = []
    for item in list:
        skus.append(item["sku"])
    opts_url = getHost(url) + "/v/option/get_color_option_icons_for_skus?page=Superbrowse&format=true&"
    url_skus = "&skus%5B%5D=".join(skus)
    opts_url = opts_url + "skus%5B%5D=" + url_skus
    html = getHtml(opts_url)
    if html == "":
        print("get html empty")
        return 
    opts = json.loads(html)
    opt_mapper = {}
    for opt in opts:
        opt_mapper[opt["sku"]] = opt
    i = 1
    for item in list:
        item["options"] = opt_mapper[item["sku"]]
        item["category_url"] = category_url
        item["category_id"] = category_id
        item["category_cn"] = category_cn
        item["rank"] = (page - 1) * 48 + i
        item["site"] = site
        try:
            save_listing(item)
        except Exception as e:
            print(str(e))
        i = i + 1

# 保存列表信息
def save_listing(item):
    #re_reconnect_pg()
    curTime = arrow.now().format("YYYY-MM-DD HH:mm:ss")
    sql = """
    UPDATE spider.wayfair_listing set 
    sku=%s,rank=%s,url=%s,product_name=%s,price=%s,currency=%s,img_url=%s,category_id=%s,update_time=%s,sys_category=%s,sku_info=%s,status=%s,category_url=%s,color_option_count=%s,size_option_count=%s,other_option_count=%s,ori_price=%s,average_overall_rating=%s,review_count=%s,manufacturer=%s
    where sku=%s and category_url=%s and rank=0;

    INSERT INTO spider.wayfair_listing(sku,rank,url,product_name,price,currency,img_url,category_id,update_time,sys_category,sku_info,status,category_url,color_option_count,size_option_count,other_option_count,ori_price,average_overall_rating,review_count,manufacturer,site)
    select %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
    where not EXISTS (select 1 from spider.wayfair_listing where sku=%s and category_url=%s);
    """
    pricing = item["raw_pricing_data"]["pricing"]
    display = pricing["customerPrice"]["display"]
    price = display["min"]["value"] if "min" in display else display["value"]
    currency = display["min"]["currency"] if "min" in display else display["currency"]
    arr = [
        item["sku"],
        item["rank"],
        item["url"],
        item["product_name"],
        price, # price
        currency, # currency
        str(item["image_data"]["ireid"]), # img_url
        item["category_id"], # category_id
        curTime, # update_time
        item["category_cn"], # sys_category
        json.dumps(item["options"]), # sku_info
        "init", # status
        item["category_url"],
        item["options"]["color_option_count"],
        item["options"]["size_option_count"],
        item["options"]["other_option_count"],
        pricing["listPrice"]["display"]["value"] if pricing["listPrice"] else price, # ori_price
        item["average_overall_rating"] if "average_overall_rating" in item else "0",
        item["review_count"] if "review_count" in item else 0,
        item["manufacturer"] if "manufacturer" in item else "",
        item["sku"],
        item["category_url"],

        item["sku"],
        item["rank"],
        item["url"],
        item["product_name"],
        price, # price
        currency, # currency
        str(item["image_data"]["ireid"]), # img_url
        item["category_id"], # category_id
        curTime, # update_time
        item["category_cn"], # sys_category
        json.dumps(item["options"]), # sku_info
        "init", # status
        item["category_url"],
        item["options"]["color_option_count"],
        item["options"]["size_option_count"],
        item["options"]["other_option_count"],
        pricing["listPrice"]["display"]["value"] if pricing["listPrice"] else price, # ori_price
        item["average_overall_rating"] if "average_overall_rating" in item else "0",
        item["review_count"] if "review_count" in item else 0,
        item["manufacturer"] if "manufacturer" in item else "",
        item["site"],
        item["sku"],
        item["category_url"],
    ]
    try:
        # print("insert:" + str(arr))
        with db_helper.get_resource() as (cursor, _):
            cursor.execute(
                sql,
                arr,
            )
        print("rank " + str(item["rank"]) + " " + item["sku"])
        # print("insert success")
    except Exception as e:
        print(str(e))
        return None

def get_or_default(obj, key, default):
    return obj[key] if key in obj else default

def save_listing_detail(item):
    re_reconnect_pg()
    curTime = arrow.now().format("YYYY-MM-DD HH:mm:ss")
    sql = """UPDATE spider.wayfair_listing_detail set
    update_time=%s,status=%s,piname_1=%s,piname_2=%s,piname_3=%s,tag_1=%s,tag_2=%s,tag_3=%s
    where sku=%s and piid_1=%s and piid_2=%s and piid_3=%s;

    INSERT INTO spider.wayfair_listing_detail(sku,piid_1,piid_2,piid_3,url,product_name,price,currency,update_time,tag_1,tag_2,tag_3,status,piname_1,piname_2,piname_3,category_url,site)
    select %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
    where not EXISTS (select 1 from spider.wayfair_listing_detail where sku=%s and piid_1=%s and piid_2=%s and piid_3=%s);
    """
    piids = []
    for i in range(1, 4):
        if f"piid_{i}" in item:
            piids.append(str(item[f"piid_{i}"]))
    if len(piids) > 0:
        item["url"] = item["url"].replace("piid=", "_=")
        item["url"] = item["url"] + ("&" if item["url"].find("?") > 0 else "?") +"piid=" + "%2C".join(piids)
    arr = [
        curTime,
        "update",
        get_or_default(item, "piname_1", ""),
        get_or_default(item, "piname_2", ""),
        get_or_default(item, "piname_3", ""),
        get_or_default(item, "tag_1", ""),
        get_or_default(item, "tag_2", ""),
        get_or_default(item, "tag_3", ""),
        item["sku"],
        get_or_default(item, "piid_1", 0),
        get_or_default(item, "piid_2", 0),
        get_or_default(item, "piid_3", 0),

        item["sku"],
        get_or_default(item, "piid_1", 0),
        get_or_default(item, "piid_2", 0),
        get_or_default(item, "piid_3", 0),
        item["url"],
        item["product_name"],
        item["price"],
        item["currency"],
        curTime,
        get_or_default(item, "tag_1", ""),
        get_or_default(item, "tag_2", ""),
        get_or_default(item, "tag_3", ""),
        "init",
        get_or_default(item, "piname_1", ""),
        get_or_default(item, "piname_2", ""),
        get_or_default(item, "piname_3", ""),
        item["category_url"],
        item["site"],
        item["sku"],
        get_or_default(item, "piid_1", 0),
        get_or_default(item, "piid_2", 0),
        get_or_default(item, "piid_3", 0),
    ]
    try:
        # print("insert:" + str(arr))
        # re_reconnect_pg()
        with db_helper.get_resource() as (cursor, _):
            cursor.execute(
                sql,
                arr,
            )
        # print("insert success")
    except Exception as e:
        print(str(e))
        return None

global_map = {}

def save_attribute(category_name, site, key, val):
    mkey = f"{category_name}@{site}@{key}@{val}"
    if mkey in global_map:
        print(f"save_attribute skip {mkey}")
        return 
    global_map[mkey] = True
    sql = """
    INSERT INTO spider.wy_site_attribute(eya_category_name,platform,site,platform_feature_key,platform_feature_value)
    select %s,%s,%s,%s,%s
    where not EXISTS (select 1 from spider.wy_site_attribute where eya_category_name=%s and site=%s and platform_feature_key=%s and platform_feature_value=%s);
    """
    arr = [
        category_name,
        "wayfair",
        site,
        key,
        val,

        category_name,
        site,
        key,
        val
    ]
    try:
        with db_helper.get_resource() as (cursor, _):
            cursor.execute(
                sql,
                arr,
            )
        print(f"save_attribute {key},{val} success")
    except Exception as e:
        print(str(e))
        return None

def save_comment(comment, detailOptions):
    #re_reconnect_pg()
    sku = comment["sku"]
    category_url = comment["category_url"]
    review_id = comment["reviewId"]
    # 评论是否全部读取完过
    if comment["comments_status"] == "done":
        sql = f"select id from spider.wayfair_listing_comment where sku='{sku}' and review_id={review_id}"
        with db_helper.get_resource() as (cursor, _):
            cursor.execute(sql)
            result = cursor.fetchall()
            if len(result) > 0:
                return False

    curTime = arrow.now().format("YYYY-MM-DD HH:mm:ss")
    
    sql = """UPDATE spider.wayfair_listing_comment set
    update_time=%s
    where sku=%s and review_id=%s;
    
    INSERT INTO spider.wayfair_listing_comment(sku,img_url,update_time,status,piname_1,piname_2,piname_3,content,review_id,reviewer_location,reviewer_name,rating_stars,reviewer_badge_text,date,review_helpful,language_code,opid,rat,category_url,site)
    select %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
    where not EXISTS (select 1 from spider.wayfair_listing_comment where sku=%s and review_id=%s);
    """
    imgids = []
    for item in comment["customerPhotos"]:
        imgids.append(str(item["ire_id"]))
    date = comment["date"].split("/")
    if len(date) == 1:
        date = comment["date"].split(".")
    if ".com" in comment["host"]:
        cdate = date[2] + "-" + date[0] + "-" + date[1]
    elif ".ca" in comment["host"]:
        date = comment["date"]
        date = date.replace("Jan", "01,")
        date = date.replace("Feb", "02,")
        date = date.replace("Mar", "03,")
        date = date.replace("Apr", "04,")
        date = date.replace("May", "05,")
        date = date.replace("Jun", "06,")
        date = date.replace("Jul", "07,")
        date = date.replace("Aug", "08,")
        date = date.replace("Sep", "09,")
        date = date.replace("Oct", "10,")
        date = date.replace("Nov", "11,")
        date = date.replace("Dec", "12,")
        date = date.split(",")
        cdate = date[2].strip() + "-" + date[0].strip() + "-" + date[1].strip()
    else:
        cdate = date[2] + "-" + date[1] + "-" + date[0]
    last_month = arrow.now().shift(months=-1).format("YYYY-MM") + "-01"
    # 只获取最近1个月的评论
    if cdate < last_month:
        return None
    options = comment["options"]
    oplen = len(options)
    piname1 = options[0]["value"] if oplen > 0 else ""
    piname2 = options[1]["value"] if oplen > 1 else ""
    piname3 = options[2]["value"] if oplen > 2 else ""
    optid = getOptionId(comment, detailOptions)
    arr = [
        curTime,
        comment["sku"],
        comment["reviewId"],
        comment["sku"],
        ",".join(imgids),
        curTime,
        "done",
        piname1,
        piname2,
        piname3,
        comment["productComments"],
        comment["reviewId"],
        comment["reviewerLocation"],
        comment["reviewerName"],
        comment["ratingStars"],
        comment["reviewerBadgeText"],
        cdate,
        comment["reviewHelpful"],
        comment["languageCode"],
        optid["opid"],
        optid["rat"],
        comment["category_url"],
        comment["site"],
        comment["sku"],
        comment["reviewId"],
    ]
    try:
        # print("insert:" + str(arr))
        # re_reconnect_pg()
        with db_helper.get_resource() as (cursor, _):
            cursor.execute(
                sql,
                arr,
            )
        # print("insert success")
    except Exception as e:
        print(str(e))
    
    return True

def getOptionId(comment, detailOptions):
    commentOpts = []
    if len(comment["options"]) == 0:
        if len(detailOptions) == 1:
            return {"opid": detailOptions[0]["id"], "rat": 1}
        else:
            return {"opid": 0, "rat": 0}
    for opt in comment["options"]:
        commentOpts.append(opt["value"])
    commentOptStr = " ".join(commentOpts).strip()
    if commentOptStr == "":
        return {"opid": 0, "rat": 0}
    like = 0
    opid = 0
    for opt in detailOptions:
        optStr = " ".join([opt["piname_1"], opt["piname_2"], opt["piname_3"]]).strip()
        rat = difflib.SequenceMatcher(None, commentOptStr, optStr).quick_ratio()
        if rat > like:
            opid = opt["id"]
            like = rat
    return {"opid": opid, "rat": like}

# 获取listing链接下拉取options的任务
def load_options_tasks(url):
    #re_reconnect_pg()
    sql = f"select id,sku,url,color_option_count,size_option_count,other_option_count,product_name,price,currency,category_url,site,sys_category from spider.wayfair_listing where rank>0 and status='init' and category_url='{url}'"
    result = None
    with db_helper.get_resource() as (cursor, _):
        cursor.execute(sql)
        result = cursor.fetchall()
    tasks = []
    for item in result:
        task_info = {
            "id": item["id"],
            "sku": item["sku"],
            "url": item["url"],
            "color_option_count": item["color_option_count"],
            "size_option_count": item["size_option_count"],
            "other_option_count": item["other_option_count"],
            "product_name": item["product_name"],
            "price": item["price"],
            "currency": item["currency"],
            "category_url": item["category_url"],
            "site": item["site"],
            "sys_category": item["sys_category"],
        }
        tasks.append(task_info)
    return tasks

# listing任务：拉取评论、价格、特性
def load_listing_tasks(url):
    #re_reconnect_pg()
    sql = f"select id,sku,url,comments_status,last_comments_page,category_url,review_count,last_review_count,site,sys_category from spider.wayfair_listing where rank>0 and status='succ' and category_url='{url}' limit 9000"
    result = None
    with db_helper.get_resource() as (cursor, _):
        cursor.execute(sql)
        result = cursor.fetchall()
    tasks = []
    for item in result:
        task_info = {
            "id": item["id"],
            "sku": item["sku"],
            "url": item["url"],
            "comments_status": item["comments_status"],
            "last_comments_page": item["last_comments_page"],
            "category_url": item["category_url"],
            "review_count": item["review_count"],
            "last_review_count": item["last_review_count"],
            "site": item["site"],
            "sys_category": item["sys_category"],
        }
        tasks.append(task_info)
    return tasks

def load_options_by_sku(sku, url):
    #re_reconnect_pg()
    sql = f"select id,piname_1,piname_2,piname_3,piid_1,piid_2,piid_3,tag_1,tag_2,tag_3 from spider.wayfair_listing_detail where sku='{sku}' and category_url='{url}'"
    result = None
    with db_helper.get_resource() as (cursor, _):
        cursor.execute(sql)
        result = cursor.fetchall()
    options = []
    for item in result:
        opt = {
            "id": item["id"],
            "piname_1": item["piname_1"],
            "piname_2": item["piname_2"],
            "piname_3": item["piname_3"],
            "piid_1": item["piid_1"],
            "piid_2": item["piid_2"],
            "piid_3": item["piid_3"],
            "tag_1": item["tag_1"],
            "tag_2": item["tag_2"],
            "tag_3": item["tag_3"]
        }
        options.append(opt)
    return options

def update_listing_status(sku, category_url, status):
    #re_reconnect_pg()
    sql = f"update spider.wayfair_listing set status='{status}' where sku='{sku}' and category_url='{category_url}'"
    with db_helper.get_resource() as (cursor, _):
        cursor.execute(sql)

def update_listing_last_reviews(sku, category_url, reviews):
    #re_reconnect_pg()
    sql = f"update spider.wayfair_listing set last_review_count='{reviews}' where sku='{sku}' and category_url='{category_url}'"
    with db_helper.get_resource() as (cursor, _):
        cursor.execute(sql)

def update_listing_comments_status(sku, category_url, status, page):
    #re_reconnect_pg()
    sql = f"update spider.wayfair_listing set comments_status='{status}',last_comments_page={page} where sku='{sku}' and category_url='{category_url}'"
    with db_helper.get_resource() as (cursor, _):
        cursor.execute(sql)

def loadWayfairListing(task):
    url = task["url"]
    category_cn = task["name"]
    site = task["site"]
    print(f"clear rank of {url}")
    clearRank(url)
    for i in range(1, 21):
        print(f"start loading page {i} of {url}")
        spiderWayfair(site, category_cn, url, i)
        print(f"end loading page {i} of {url}, sleeping 10s")
        time.sleep(10)

# 根据链接获取商品对应的options
def runOptionTasks(url, try_times = 0):
    tasks = load_options_tasks(url)
    fails = 0
    for task in tasks:
        if task["color_option_count"] == 0 and task["size_option_count"] == 0 and task["other_option_count"] == 0:
            save_listing_detail(task)
            update_listing_status(task["sku"], task["category_url"], "succ")
        else:
            # options = load_options_by_url(task["url"])
            options = loadOptionsByPriceApi(task)
            if len(options) > 0:
                for opt in options:
                    detail = task.copy()
                    detail.update(opt)
                    save_listing_detail(detail)
                update_listing_status(task["sku"], task["category_url"], "succ")
                print(task["sku"] + " save options succ, sleep 3s...")
            else:
                fails = fails + 1
                print(task["sku"] + " save options fail, sleep 3s...")
            time.sleep(3)
    if fails > 0 and try_times < 5:
        try_times = try_times + 1
        print(f"fails {fails}, retry {try_times} times..")
        return runOptionTasks(url, try_times)
    elif fails > 0:
        return False
    print("done..")
    return True

def getHost(url):
    return "/".join(url.split("/")[0:3])

# 读取评论
def loadComments(options, task):
    host = getHost(task["url"])
    url = host + "/graphql?hash=a636f23a2ad15b342db756fb5e0ea093"
    curpage = task["last_comments_page"] if task["last_comments_page"] and task["last_comments_page"] > 0 else 1
    perPage = 30
    params = {
        "variables": {
            "filter_rating": "",
            "language_code": "en",
            "page_number": 1,
            "reviews_per_page": perPage,
            "search_query": "",
            "sku": task["sku"],
            "sort_order": "DATE_DESCENDING"
        }
    }
    curNum = 100
    needBreak = False
    while curNum >= perPage:
        params["variables"]["page_number"] = curpage
        print("load comments " + task["sku"] + ", page " + str(curpage))
        res = getJson(url, params, {
            "origin": host,
            "reffer": task["url"],
            "apollographql-client-version": "27c53b9320e4c8fa9ad400d80e35da75a69de445",
            "x-parent-txid": "I/bHHmNbRK6fjNLEIvcoAg=="
        })
        if not res:
            break
        try:
            jsonObj = json.loads(res)
            comments = jsonObj["data"]["product"]["customerReviews"]["reviews"]
        except Exception as e:
            print("load comments error " + str(e))
            break
        curNum = len(comments)
        for comment in comments:
            comment["sku"] = task["sku"]
            comment["category_url"] = task["category_url"]
            comment["comments_status"] = task["comments_status"]
            comment["host"] = host
            comment["site"] = task["site"]
            saveRes = save_comment(comment, options)
            # 2个月前数据不保存
            if saveRes is None:
                needBreak = True
                print("comment has expried, stop loading...")
                update_listing_comments_status(task["sku"], task["category_url"], "done", 0)
                time.sleep(6)
                break
            # 已经保存的数据不再重复拉取
            elif not saveRes:
                needBreak = True
                print("comment has saved, stop loading...")
                time.sleep(6)
                break
        if needBreak:
            break
        update_listing_comments_status(task["sku"], task["category_url"], "processing", curpage)
        curpage = curpage + 1
        time.sleep(6)
    update_listing_comments_status(task["sku"], task["category_url"], "done", 0)

# 读取特性
def loadFeatures(task):
    host = getHost(task["url"])
    url = host + "/graphql"
    params = {
        "operationName": "specs",
        "variables": {
            "sku": task["sku"]
        },
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "731f41b9572fefb3f47cddc6ab143d198903c8475f753210b4fb044c89d912a4"
            }
        }
    }
    res = getJson(url, params, {
        "origin": host,
        "reffer": task["url"],
        "x-wf-way": "true",
        "apollographql-client-version": "27c53b9320e4c8fa9ad400d80e35da75a69de445"
    })
    if not res:
        print(str(task["sku"]) + " load features error, skip..")
        return 
    jsonObj = json.loads(res)
    try:
        obj = jsonObj["data"]["productSpecificationSections"]
        res = {}
        for type in obj:
            name = type["name"]
            res[name] = {}
            for item in type["specifications"]["edges"]:
                childs = item["node"]["children"]["edges"]
                key = item["node"]["specification"]["label"]
                values = item["node"]["specification"]["value"]
                if "selectedChoices" in values:
                    val = []
                    for v in values["selectedChoices"]:
                        val.append(v["name"])
                        save_attribute(task["sys_category"], task["site"], key, v["name"])
                elif "value" in values:
                    val = [values["value"]]
                    save_attribute(task["sys_category"], task["site"], key, values["value"])
                res[name][key] = val
                for c in childs:
                    key = c["node"]["label"]
                    values = c["node"]["value"]
                    if "selectedChoices" in values:
                        val = []
                        for v in values["selectedChoices"]:
                            val.append(v["name"])
                            save_attribute(task["sys_category"], task["site"], key, v["name"])
                    elif "value" in values:
                        val = [values["value"]]
                        save_attribute(task["sys_category"], task["site"], key, values["value"])
                    res[name][key] = val
        save_features(task["sku"], json.dumps(res))
        # time.sleep(2)
    except Exception as e:
        print("save features failed, skip...")

def set_opt_val(m, index, opt):
    index = index + 1
    m[f"piid_{index}"] = opt["id"] if "id" in opt else opt["option_id"]
    m[f"tag_{index}"] = opt["category"]
    m[f"piname_{index}"] = opt["name"]
    return m

def load_options_by_url(url, try_times = 0):
    html = getHtml(url)
    if html == "":
        print("get html empty")
        return []
    strs = re.findall(r"\"WEBPACK_ENTRY_DATA\"]=(.*);<\/script>", html)
    if not strs:
        if try_times > 3:
            return []
        print("error for loading options, sleep 15s retry...")
        time.sleep(15)
        return load_options_by_url(url, try_times + 1)
    obj = json.loads(strs[0])
    options = obj["application"]["props"]["options"]["standardOptions"]
    opt_len = len(options)
    arr = []
    if opt_len == 1:
        for opt in options[0]["options"]:
            task = {}
            task = set_opt_val(task, 0, opt)
            arr.append(task)
    elif opt_len == 2:
        for opt in options[0]["options"]:
            task = {}
            task = set_opt_val(task, 0, opt)
            for opt in options[1]["options"]:
                task = set_opt_val(task, 1, opt)
                arr.append(task.copy())
    elif opt_len >= 3:
        for opt in options[0]["options"]:
            task = {}
            task = set_opt_val(task, 0, opt)
            for opt in options[1]["options"]:
                task = set_opt_val(task, 1, opt)
                for opt in options[2]["options"]:
                    task = set_opt_val(task, 2, opt)
                    arr.append(task.copy())
    return arr

def loadOptionsByPriceApi(mainTask):
    url = mainTask["url"]
    sku = mainTask["sku"]
    host = getHost(url)
    url = host + "/graphql?hash=351b9eb2d99ac83076f18b55ca83ce24%2351e9525cea39fb7df417ec0cf3622f24%2331d0c6755477e794473315f559edc41a"
    params = {
        "variables": {
            "sku": sku,
            "selectedOptions": []
        }
    }
    header = {
        "origin": host,
        "reffer": url,
    }
    if ".uk" in host:
        header["x-wayfair-locale"] = "en-GB"
    res = getJson(url, params, header)
    if not res:
        print(sku + " load price error, skip..")
        return []
    jsonObj = json.loads(res)
    try:
        prices = jsonObj["data"]["product"]["options"]["optionCategories"]
        opt_len = len(prices)
        arr = []
        if opt_len == 1:
            for opt in prices[0]["options"]:
                opt["category"] = prices[0]["name"]
                task = {}
                task = set_opt_val(task, 0, opt)
                arr.append(task)
                save_attribute(mainTask["sys_category"], mainTask["site"], opt["category"], opt["name"])
        elif opt_len == 2:
            for opt in prices[0]["options"]:
                opt["category"] = prices[0]["name"]
                task = {}
                task = set_opt_val(task, 0, opt)
                for opt in prices[1]["options"]:
                    opt["category"] = prices[1]["name"]
                    task = set_opt_val(task, 1, opt)
                    arr.append(task.copy())
                    save_attribute(mainTask["sys_category"], mainTask["site"], opt["category"], opt["name"])
        elif opt_len >= 3:
            for opt in prices[0]["options"]:
                opt["category"] = prices[0]["name"]
                task = {}
                task = set_opt_val(task, 0, opt)
                for opt in prices[1]["options"]:
                    opt["category"] = prices[1]["name"]
                    task = set_opt_val(task, 1, opt)
                    for opt in prices[2]["options"]:
                        opt["category"] = prices[2]["name"]
                        task = set_opt_val(task, 2, opt)
                        arr.append(task.copy())
                        save_attribute(mainTask["sys_category"], mainTask["site"], opt["category"], opt["name"])
        return arr
    except Exception as e:
        print(str(e))
        return []

# 读取价格
def loadPrices(options, task):
    host = getHost(task["url"])
    url = host + "/graphql?hash=351b9eb2d99ac83076f18b55ca83ce24%2351e9525cea39fb7df417ec0cf3622f24%2331d0c6755477e794473315f559edc41a"
    params = {
        "variables": {
            "sku": task["sku"],
            "selectedOptions": []
        }
    }
    allOptions = []
    for opt in options:
        opts = []
        if opt["piid_1"] > 0:
            opts.append(opt["piid_1"])
        if opt["piid_2"] > 0:
            opts.append(opt["piid_2"])
        if opt["piid_3"] > 0:
            opts.append(opt["piid_3"])
        if len(opts) == 0:
            continue
        allOptions.append({
            "id": opt["id"],
            "opts": opts
        })
    # 没有options，跳过
    if len(allOptions) == 0:
        print(task["sku"] + " no options, skips..")
        return 
    optionsOrder = {
        options[0]["tag_1"]: 0
    }
    if options[0]["tag_2"] != "":
        optionsOrder[options[0]["tag_2"]] = 1
    if options[0]["tag_3"] != "":
        optionsOrder[options[0]["tag_3"]] = 2

    try:
        loadedPrice = {}
        for opts in allOptions:
            opts_key = ",".join(str(i) for i in opts["opts"])
            if opts_key in loadedPrice:
                save_price(opts["id"], loadedPrice[opts_key])
                continue
            params["variables"]["selectedOptions"] = opts["opts"]
            header = {
                "origin": host,
                "reffer": task["url"],
            }
            if ".uk" in host:
                header["x-wayfair-locale"] = "en-GB"
            res = getJson(url, params, header)
            if not res:
                print(str(opts["id"]) + " load price error, skip..")
                continue
            jsonObj = json.loads(res)
            prices = jsonObj["data"]["product"]["options"]["optionCategories"]
            newArr = [{} for _ in range(len(prices))]
            for price in prices:
                newArr[optionsOrder[price["name"]]] = price
            index = 0
            for price in newArr:
                opids = opts["opts"][:]
                for op in price["options"]:
                    opids[index] = op["id"]
                    copts_key = ",".join(str(i) for i in opids)
                    if op["pricing"]:
                        cp = op["pricing"]["customerPrice"]["display"]
                        if "value" in cp:
                            loadedPrice[copts_key] = cp["value"]
                    if copts_key == opts_key:
                        save_price(opts["id"], loadedPrice[copts_key] if copts_key in loadedPrice else 0)
                index = index + 1
            print("done sleep 3s...")
            time.sleep(3)
                
    except Exception as e:
        print("load price error " + str(e))
        return

def save_price(id, price_num):
    #re_reconnect_pg()
    sql = f"update spider.wayfair_listing_detail set price={price_num} where id={id}"
    with db_helper.get_resource() as (cursor, _):
        cursor.execute(sql)
    print(f"save price {id}, {price_num}")

def save_features(sku, features):
    #re_reconnect_pg()
    sql = """update spider.wayfair_listing set features=%s where sku=%s"""
    with db_helper.get_resource() as (cursor, _):
        cursor.execute(sql, [
            features,
            sku
        ])
    print(f"save features {sku} success")

def runListingTasks(url):
    tasks = load_listing_tasks(url)
    for task in tasks:
        options = load_options_by_sku(task["sku"], url)
        if task["review_count"] > 0 and task["last_review_count"] != task["review_count"]:
            loadComments(options, task)
            update_listing_last_reviews(task["sku"], task["category_url"], task["review_count"])
        loadPrices(options, task)
        loadFeatures(task)
        update_listing_status(task["sku"], task["category_url"], "success")

def loadBatchTask(batch_no, task):
    #re_reconnect_pg()
    sql = f"select id,batch_no,date,process_info,task_status from spider.wayfair_batch_task where batch_no='{batch_no}'"
    result = None
    with db_helper.get_resource() as (cursor, _):
        cursor.execute(sql)
        result = cursor.fetchall()
    if result and len(result) > 0:
        return {
            "id": result[0]["id"],
            "batch_no": result[0]["batch_no"],
            "date": result[0]["date"],
            "process_info": result[0]["process_info"],
            "task_status": result[0]["task_status"]
        }
    else:
        for item in task:
            item["status"] = "init"
        sql = """INSERT INTO spider.wayfair_batch_task(batch_no,date,process_info,task_status) values (%s,%s,%s,%s)
        """
        curTime = arrow.now().format("YYYY-MM-DD HH:mm:ss")
        arr = [
            batch_no,
            curTime,
            json.dumps(task),
            "init",
        ]
        try:
            #re_reconnect_pg()
            with db_helper.get_resource() as (cursor, _):
                cursor.execute(
                    sql,
                    arr,
                )
        except Exception as e:
            return None
        
        return {
            "batch_no": batch_no,
            "date": curTime,
            "process_info": json.dumps(task),
            "task_status": "init"
        }
    
def saveBatchProcess(batch_no, tasks):
    #re_reconnect_pg()
    sql = """update spider.wayfair_batch_task set process_info=%s where batch_no=%s"""
    with db_helper.get_resource() as (cursor, _):
        cursor.execute(sql, [json.dumps(tasks), batch_no])
    print(f"save batch process {batch_no} success")

def saveBatchStatus(batch_no, status):
    #re_reconnect_pg()
    sql = """update spider.wayfair_batch_task set task_status=%s where batch_no=%s"""
    with db_helper.get_resource() as (cursor, _):
        cursor.execute(sql, [json.dumps(tasks), status])
    print(f"save batch status {batch_no} success")

class ThreadNum(threading.Thread):
  def __init__(self, queue):
    threading.Thread.__init__(self, daemon = True)
    self.queue = queue
  
  def run(self):
    while True:
        #消费者端，从队列中获取num
        args = self.queue.get()
        doRunTask(args["task"], args["batch_no"], args["tasks"])
        #在完成这项工作之后，使用 queue.task_done() 函数向任务已经完成的队列发送一个信号
        self.queue.task_done()
    print("Consumer Finished")

def runBatch(batch_no, tasks):
    batch_task = loadBatchTask(batch_no, tasks)
    if not batch_task:
        return None
    tasks = json.loads(batch_task["process_info"])
    tasksQueue = queue.Queue()
    for i in range(5):
        t = ThreadNum(tasksQueue)
        # t.setDaemon(True)
        t.start()
    for task in tasks:
        tasksQueue.put({"task": task, "batch_no": batch_no, "tasks": tasks})
    tasksQueue.join()

def doRunTask(task, batch_no, tasks):
    if task["status"] == "done":
        return
    if task["status"] == "init":
        loadWayfairListing(task)
        with db_helper.get_resource() as (cursor, _):
            cursor.execute("update spider.wayfair_listing set img_url=concat('https://secure.img1-fg.wfcdn.com/im/123/resize-h800-w800%5Ecompr-r85/' , LEFT ( img_url, 4 ) , '/' , img_url , '/default.jpg') WHERE LENGTH ( img_url ) < 10")
        task["status"] = "loading"
        saveBatchProcess(batch_no, tasks)
    runOptionTasks(task["url"])
    runListingTasks(task["url"])
    task["status"] = "done"
    saveBatchProcess(batch_no, tasks)
    saveBatchStatus(batch_no, "succ")
    insertSummary(batch_no, task["url"], task["site"])

def loadTasks():
    #re_reconnect_pg()
    sql = f"select site,eya_category_name,category_link from spider.wy_category_task where platform='wayfair' and enable_status=0"
    result = None
    with db_helper.get_resource() as (cursor, _):
        cursor.execute(sql)
        result = cursor.fetchall()
    tasks = []
    for item in result:
        task_info = {
            "site": item["site"],
            "name": item["eya_category_name"],
            "url": item["category_link"],
        }
        tasks.append(task_info)
    return tasks

# 计算留评率
def computeSaleCommentRat(category_url, site):
    #re_reconnect_pg()
    sql = """
    select 
        sum(units) as sales,sum(sales_amount) as sales_count 
    from 
        spider.wy_sales_result
    where
        site='%(site)s'
        and month_id=TO_CHAR((DATE_TRUNC('MONTH', CURRENT_DATE) + INTERVAL '-1 MONTH')::DATE,'YYYY-MM')
        and wy_sku in (select sku from spider.wayfair_listing where category_url='%(category_url)s' and rank>0)
    """
    sql = sql % dict(category_url=category_url, site=site)
    result = None
    with db_helper.get_resource() as (cursor, _):
        cursor.execute(sql)
        result = cursor.fetchall()
    sales = result[0]["sales"] if result and len(result) > 0 else 0
    sql = """
    select 
        count(*) as total
    from 
        spider.wayfair_listing_comment
    where
        category_url='%(category_url)s'
        and date >= date_trunc('month', current_date - interval '1' month)
        and date < date_trunc('month', current_date)
        and sku in (select wy_sku from spider.wy_sales_result where site='%(site)s')
    """
    sql = sql % dict(category_url=category_url, site=site)
    result = None
    with db_helper.get_resource() as (cursor, _):
        cursor.execute(sql)
        result = cursor.fetchall()
    comments = result[0]["total"] if result and len(result) > 0 else 0
    sales = sales if sales else 0
    return round(sales / comments) if comments > 0 else 0

def insertSummary(batch_no, category_url, site):
    sales_review_rat = computeSaleCommentRat(category_url, site)
    sql = """
    delete from spider.wayfair_listing_summary where batch_no='%(batch_no)s' and category_url='%(category_url)s';
    
    INSERT INTO spider.wayfair_listing_summary(sku,site,sys_category,sponsored,opt_num,rank,product_name,manufacturer,price,currency,review_count,average_overall_rating,sales_count_30d,sales_30d,date,url,features,tag_1,tag_2,tag_3,piname_1,piname_2,piname_3,review_count_30d,batch_no,opt_review_count,opt_rating_stars,platform_category,sales_review_rat,opt_review_count_30d,img_url,category_url,is_ziel,month)
    SELECT
        wy_listing.sku,
        wy_listing.site,
        wy_listing.sys_category,
        POSITION ( 'sponsoredid' IN wy_listing.url ) AS sponsored,
        0 AS opt_num,
        wy_listing.rank,
        wy_listing.product_name,
        wy_listing.manufacturer,
        wy_detail.price,
        wy_detail.currency,
        wy_listing.review_count,
        wy_listing.average_overall_rating,
        0 AS sales_count_30d,
        0 AS sales_30d,
        CURRENT_DATE AS date,
        wy_detail.url,
        wy_listing.features,
        wy_detail.tag_1,
        wy_detail.tag_2,
        wy_detail.tag_3,
        wy_detail.piname_1,
        wy_detail.piname_2,
        wy_detail.piname_3,
        (
            SELECT COUNT( * ) 
            FROM
                spider.wayfair_listing_comment as w_2
            WHERE
                w_2.sku = wy_detail.sku
                and w_2.date >= date_trunc('month', current_date - interval '1' month)
                and w_2.date < date_trunc('month', current_date)
        ) AS review_count_30d,
        '%(batch_no)s' AS batch_no,
        (
            SELECT COUNT( * ) 
            FROM
                spider.wayfair_listing_comment as w_3
            WHERE
                w_3.sku = wy_detail.sku 
                AND w_3.opid = wy_detail.ID 
                AND w_3.rat >= 0.8 
        ) AS opt_review_count,
        (
            SELECT AVG( w_4.rating_stars ) / 2 
            FROM
                spider.wayfair_listing_comment as w_4
            WHERE
                w_4.sku = wy_detail.sku 
                AND w_4.opid = wy_detail.ID 
                AND w_4.rat >= 0.8 
        ) AS opt_rating_stars,
        wy_task.platform_category,
        wy_task.sales_review_rat,
        (
            SELECT COUNT( * ) 
            FROM
                spider.wayfair_listing_comment as w_5
            WHERE
                w_5.sku = wy_detail.sku 
                AND w_5.opid = wy_detail.id 
                AND w_5.rat >= 0.8 
                and w_5.date >= date_trunc('month', current_date - interval '1' month)
                and w_5.date < date_trunc('month', current_date)
        ) AS opt_review_count_30d,
        wy_listing.img_url,
        wy_detail.category_url,
        (
            select count(*) 
            from 
                spider.wy_sales_result as w_6
            where
                w_6.wy_sku=wy_detail.sku
                and w_6.site=wy_detail.site
        ) as is_ziel,
        (DATE_TRUNC('MONTH', CURRENT_DATE) + INTERVAL '-1 MONTH')::DATE as month
    FROM
        spider.wayfair_listing_detail as wy_detail
        LEFT JOIN spider.wayfair_listing as wy_listing ON wy_detail.sku = wy_listing.sku
        LEFT JOIN spider.wy_category_task as wy_task ON wy_detail.category_url = wy_task.category_link 
    where wy_listing.status='success' and wy_listing.rank>0 and wy_listing.category_url='%(category_url)s'
    ORDER BY wy_listing.rank ASC;

    update 
        spider.wayfair_listing_summary as wy_summary
    set
        opt_num=(
            select count(*) 
            from
                spider.wayfair_listing_summary as w_1 
            where 
                w_1.sku=wy_summary.sku
                and (
                    w_1.price>0 
                    or w_1.opt_review_count_30d>0
                )
        )
    where 
        wy_summary.batch_no='%(batch_no)s' 
        and wy_summary.category_url='%(category_url)s';

    update
        spider.wayfair_listing_summary
    set
        sales_review_rat=%(sales_review_rat)d
    where 
        batch_no='%(batch_no)s' 
        and category_url='%(category_url)s'
        and sales_review_rat is null;

    UPDATE 
        spider.wayfair_listing_summary AS wy_summary 
    SET sales_count_30d = (
        wy_summary.opt_review_count_30d + (
            wy_summary.review_count_30d - (SELECT 
                SUM( w_1.opt_review_count_30d ) 
            FROM
                spider.wayfair_listing_summary AS w_1 
            WHERE
                w_1.sku = wy_summary.sku 
            )) / wy_summary.opt_num 
        ) * wy_summary.sales_review_rat 
    WHERE
        wy_summary.batch_no = '%(batch_no)s' 
        and wy_summary.opt_num>0
        AND wy_summary.category_url = '%(category_url)s';
    
    update 
        spider.wayfair_listing_summary
    set
        sales_30d=sales_count_30d * sales_review_rat * price
    where 
        batch_no='%(batch_no)s' 
        and category_url='%(category_url)s';
    """
    sql = sql % dict(
        batch_no = batch_no, 
        category_url = category_url,
        sales_review_rat = sales_review_rat)
    #re_reconnect_pg()
    with db_helper.get_resource() as (cursor, _):
        cursor.execute(sql)
    print(f"save batch summary {batch_no} {category_url} success")

if __name__ == "__main__":
    batch_no = "2023-01-03"
    tasks = loadTasks()
    runBatch(batch_no, tasks)