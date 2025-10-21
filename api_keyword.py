from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from collections import defaultdict
from dotenv import load_dotenv
import os
from fuzzywuzzy import fuzz
import asyncio
import time
from datetime import datetime
import redis
from redis.exceptions import RedisError

import json
# import matplotlib.pyplot as plt
import logging
load_dotenv()

api_logger = logging.getLogger("api_logger")
api_logger.setLevel(logging.INFO)
# file_handler = logging.FileHandler("/usr/app/src/api.log")
file_handler = logging.FileHandler("api.log")

file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
api_logger.addHandler(file_handler)


REDIS_HOST = os.getenv("REDIS_HOST")  # Default to 'localhost' if not set
REDIS_PORT = int(os.getenv("REDIS_PORT"))    # Default to 6379 if not set
CACHE_EXPIRATION_SECONDS = 2400  # 40 phút
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, socket_timeout=1.4, socket_connect_timeout=1.4  )

app = FastAPI()
def read_white_list(file_path):
    """Read whitelist from file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            white_list = set(line.strip() for line in f)
            api_logger.info(f"Loaded whitelist from {file_path}. Total items: {len(white_list)}")
            return white_list
    except Exception as e:
        api_logger.error(f"Error loading whitelist from {file_path}: {e}")
        return set()

# Đọc white_list từ file khi khởi động ứng dụng
white_list = read_white_list('white_list.txt')
black_list = read_white_list('black_list.txt')
black_list2 = [k.replace('_', ' ') for k in black_list]

elasticsearch_db_url = os.getenv("ELASTICSEARCH_DB_URL")
# elasticsearch_db_url = "http://10.11.32.21:30100"
# Kết nối tới Elasticsearch
if not elasticsearch_db_url:
    api_logger.error("Environment variable ELASTICSEARCH_DB_URL is not set.")
es_db = Elasticsearch([elasticsearch_db_url], request_timeout=100)

api_logger.info("Connected to Elasticsearch.")

# Mô hình dữ liệu yêu cầu từ người dùng
class KeywordRequest(BaseModel):
    type: List[str]
    start_date: str
    end_date: str
    type_top: str
    page: int
    page_size: int
    topic_ids: List[str]
class HashtagRequest(BaseModel):
    type: List[str]
    start_date: str
    end_date: str
    page: int
    page_size: int
    topic_ids: List[str]
class SearchRequest(BaseModel):
    keyword: str
    type: List[str]
    start_date: str
    end_date: str
    type_top: str
    page: int
    page_size: int
    topic_ids: Optional[List[str]] = []
    tenancy_ids: Optional[List[str]] = None

    
class SearchRequesthastag(BaseModel):
    hashtag: str
    type: List[str]
    start_date: str
    end_date: str
    page: int
    page_size: int
    topic_ids: Optional[List[str]] = []
    tenancy_ids: Optional[List[str]] = None

# Hàm lưu danh sách vào Redis
def save_list_to_cache(cache_key, data_list):
    try:
        for item in data_list:
            r.rpush(cache_key, json.dumps(item))  # Lưu từng item dưới dạng JSON
        r.expire(cache_key, CACHE_EXPIRATION_SECONDS)  # Thiết lập thời gian hết hạn
        api_logger.info(f"Saved list to cache with key: {cache_key}")
    except RedisError as e:
        api_logger.error(f"Failed to save list to cache: {e}")


# Hàm lấy danh sách từ Redis
def get_list_from_cache(cache_key, page, page_size):
    try:
        # Xác định range cần lấy
        start_index = (page - 1) * page_size
        end_index = start_index + page_size - 1

        # Lấy giá trị trong khoảng đó
        cached_data = r.lrange(cache_key, start_index, end_index)
        if not cached_data:
            return None

        # Chuyển từ JSON thành object Python
        return [json.loads(item) for item in cached_data]
    except RedisError as e:
        api_logger.error(f"Redis error when fetching list: {e}")
        return None

def is_request_cacheable(request, type_top=None):
    try:
        today = datetime.today()
        end_date = datetime.strptime(request.end_date, "%m/%d/%Y")
        start_date = datetime.strptime(request.start_date, "%m/%d/%Y")
        if end_date.day != today.day or end_date.month!= today.month or end_date.year != today.year:
            return False

        if start_date.day not in [(today - timedelta(days=i)).day for i in [0, 7, 30]] or start_date.month not in [(today - timedelta(days=i)).month for i in [0, 7, 30]] or start_date.year not in [(today - timedelta(days=i)).year for i in [0, 7, 30]] :
            return False

        if request.topic_ids != []:
            return False
        if not set(["media", "facebook", "tiktok", "youtube", "voz", "xamvn", "otofun"]).issubset(set(request.type)):
            return False

        # if request.type != ["media", "facebook", "tiktok", "youtube", "voz", "xamvn", "otofun"]:
        #     return False

        if type_top and request.type_top != type_top:
            return False

        return True
    except Exception:
        return False
# Hàm lấy dữ liệu từ Redis
def get_data_from_cache(cache_key, page, page_size):
    try:
        # Thử lấy dữ liệu từ Redis
        cached_data = r.get(cache_key)
        if not cached_data:
            return None
        # if not cached_data.get("data") or len(cached_data["data"]) == 0:
        #     api_logger.info(f"Cache data is empty for key: {cache_key}. Fetching data from Elasticsearch.")
        #     return None

        # Xử lý dữ liệu cache
        cached_data = json.loads(cached_data)
        start_index = (page - 1) * page_size
        end_index = start_index + page_size
        cached_data["data"] = cached_data["data"][start_index:end_index]
        if not cached_data["data"]: 
            return None
        return cached_data
    except RedisError as e:
        # Log lỗi và trả về None để fallback sang Elasticsearch
        api_logger.error(f"Redis error when fetching cache: {e}")
        return None


async def call_with_retry(func, request, response, max_retries=5, timeout=1.5):
    for attempt in range(max_retries):
        try:
            if attempt < max_retries - 2:
                print(attempt)
                return await asyncio.wait_for(func(request, response), timeout)
            
            else:
                return await func(request, response)
        except asyncio.TimeoutError:
            api_logger.warning(f"Timeout on attempt {attempt + 1} for {func.__name__}.")
        except Exception as e:
            api_logger.error(f"Error on attempt {attempt + 1} for {func.__name__}: {e}")

        await asyncio.sleep(0.5)

    raise Exception("Không thể kết nối sau tối đa số lần thử lại.")


# @app.post("/search_keywords/")
# async def search_keywords_api(request: SearchRequest, response: Response, bypass_cache: bool = False):
#     if  not bypass_cache and is_request_cacheable(request, type_top=request.type_top):
#         try:
#             api_logger.info("Query from cache search_keywords")

#             period_mapping = {
#                 "0": "today",
#                 "7": "last_7_days",
#                 "30": "last_30_days"
#             }
#             days_diff = (datetime.strptime(request.end_date, "%m/%d/%Y") - datetime.strptime(request.start_date, "%m/%d/%Y")).days
#             cache_period = period_mapping.get(str(days_diff), "unknown")
#             cache_key = f"search_keywords:{request.type_top}:{cache_period}"
#             cached_result = get_data_from_cache(cache_key, request.page, request.page_size)
#             if cached_result:
#                 return cached_result
#         except Exception as e:
#             api_logger.error(f"Loi API cache: {e}")
#             pass

            
#     start_time = time.time()
#     api_logger.info(f"/search_keywords endpoint called with request: {request.dict()}")

#     # Nếu không lấy từ Redis, chọc vào Elasticsearch
#     try:
#         result = await call_with_retry(search_keywords, request, response)
#         api_logger.info(f"/search_keywords completed successfully in {time.time() - start_time:.2f} seconds.")
#         return result
#     except Exception as e:
#         api_logger.error(f"/search_keywords failed: {e}")
#         raise HTTPException(status_code=500, detail="Internal server error.")
async def save_to_cache(cache_key, result):
    """Hàm lưu kết quả vào cache một cách bất đồng bộ."""
    try:
        r.setex(cache_key, CACHE_EXPIRATION_SECONDS, json.dumps(result))
        api_logger.info(f"Saved result to cache asynchronously: {cache_key}")
    except RedisError as e:
        api_logger.error(f"Error saving to cache asynchronously: {e}")

@app.post("/search_keywords/")
async def search_keywords_api(request: SearchRequest, response: Response, bypass_cache: bool = False):
    # start_date = datetime.strptime(request.start_date, "%m/%d/%Y")
    # end_date = datetime.strptime(request.end_date, "%m/%d/%Y")
    # if (end_date - start_date).days > 64:
    #     start_date = end_date - timedelta(days=64)
    #     request.start_date = start_date.strftime("%m/%d/%Y")

    if not bypass_cache and is_request_cacheable(request, type_top=request.type_top):
        try:
            api_logger.info("Query from cache search_keywords (special logic)")

            # Xác định giai đoạn thời gian cache
            period_mapping = {
                "0": "today",
                "7": "last_7_days",
                "30": "last_30_days"
            }
            days_diff = (
                datetime.strptime(request.end_date, "%m/%d/%Y") -
                datetime.strptime(request.start_date, "%m/%d/%Y")
            ).days
            cache_period = period_mapping.get(str(days_diff), "unknown")

            tenancy_part = request.tenancy_ids[0] if request.tenancy_ids else "none"

            cache_key = f"search_keywords:{request.type_top}:{cache_period}:{tenancy_part}"

            cached_result = get_data_from_cache(cache_key, request.page, request.page_size)
            if cached_result:
                api_logger.info(f"Cache hit: {cache_key}")
                return cached_result

        except Exception as e:
            api_logger.error(f"Error checking special cache: {e}")
    
    # Kiểm tra cache chung dựa trên cache_key_general
    # cache_key_general = f"search_keywords:{request.type}:{request.start_date}:{request.end_date}:{','.join(sorted(request.topic_ids))}:{request.type_top}:{request.keyword}"
    # cache_key_general = f"search_keywords:{request.type}:{request.start_date}:{request.end_date}:{','.join(sorted(request.topic_ids))}:{request.type_top}:{request.page}:{request.page_size}:{request.keyword}"
    tenancy_part = ','.join(sorted(request.tenancy_ids)) if request.tenancy_ids else 'none'

    cache_key_general = (
        f"search_keywords:"
        f"{request.type}:"
        f"{request.start_date}:"
        f"{request.end_date}:"
        f"{','.join(sorted(request.topic_ids))}:"
        f"{request.type_top}:"
        f"{request.page}:"
        f"{request.page_size}:"
        f"{request.keyword}:" 
        f"{tenancy_part}"
    )

    try:
        cached_general_result = r.get(cache_key_general)
        if not bypass_cache and cached_general_result:
            api_logger.info(f"Query from general cache: {cache_key_general}")
            cached_general_result = json.loads(cached_general_result)
            if cached_general_result["data"]:
                return cached_general_result
    except RedisError as e:
        api_logger.error(f"Error checking general cache: {e}")

    # Thực hiện truy vấn Elasticsearch nếu không có dữ liệu trong cache
    start_time = time.time()
    api_logger.info(f"/search_keywords endpoint called with request: {request.dict()}")

    try:
        result = await call_with_retry(search_keywords, request, response)
        api_logger.info(f"/search_keywords completed successfully in {time.time() - start_time:.2f} seconds.")
        if not bypass_cache:
            asyncio.create_task(save_to_cache(cache_key_general, result))
        return result

        # Lưu kết quả vào cache
        # try:
        #     r.setex(cache_key_general, CACHE_EXPIRATION_SECONDS, json.dumps(result))
        #     api_logger.info(f"Saved result to general cache: {cache_key_general}")
        # except RedisError as e:
        #     api_logger.error(f"Error saving to general cache: {e}")

        # return result
    except Exception as e:
        api_logger.error(f"/search_keywords failed: {e}")
        raise HTTPException(status_code=500, detail="Internal server error.")

async def search_keywords(request: SearchRequest, response: Response):
    try:
        if request.type_top not in ["popular", "trend"]:
            response.status_code = 400
            api_logger.warning(f"Invalid type_top value: {request.type_top}")

            return {"status_code": 400, "message": "Invalid 'type_top' value provided. Choose 'popular' or 'trend'.", "sum_records": 0, "data": []}

        try:
            start_date = datetime.strptime(request.start_date, "%m/%d/%Y")
            end_date = datetime.strptime(request.end_date, "%m/%d/%Y")
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        except ValueError:
            response.status_code = 400
            return {"status_code": 400, "message": "Invalid date format, please use MM/DD/YYYY", "sum_records": 0, "data": []}
        #test ở đây
        
        index_name = "key-osint1-current" if request.type_top == "popular" else "key-trend-current"
        if (end_date - start_date).days > 80:
            api_logger.info("Handling large time range (over 80 days). Splitting into smaller chunks.")

            # Chia khoảng thời gian thành các đoạn nhỏ (40 ngày mỗi đoạn)
            step = timedelta(days=20)
            current_start_date = start_date
            if request.type_top == "popular":
                # Logic riêng cho popular
                aggregated_result = defaultdict(int)  # Chỉ cần lưu tổng số lượng `record`
            else:
                # Logic ban đầu
                aggregated_result = defaultdict(lambda: {"record": 0, "top_20_days": 0, "trends": 0})

            while current_start_date < end_date:
                current_end_date = min(current_start_date + step - timedelta(days=1), end_date)
                api_logger.info(f"Querying data from {current_start_date.strftime('%m/%d/%Y')} to {current_end_date.strftime('%m/%d/%Y')}")

                # Tạo yêu cầu con cho khoảng thời gian nhỏ hơn
                sub_request = SearchRequest(
                    keyword=request.keyword,
                    type=request.type,
                    start_date=current_start_date.strftime("%m/%d/%Y"),
                    end_date=current_end_date.strftime("%m/%d/%Y"),
                    type_top=request.type_top,
                    page=1,  # Không phân trang cho sub-request
                    page_size=10000,  # Lấy toàn bộ kết quả để tổng hợp
                    topic_ids=request.topic_ids,
                    tenancy_ids=request.tenancy_ids
                )

                # Gọi hàm `search_keywords` để lấy dữ liệu
                sub_result = await search_keywords(sub_request, response)

                if sub_result["status_code"] == 200:
                    for item in sub_result["data"]:
                        keyword = item["keyword"]
                        if request.type_top == "popular":
                            # Cộng dồn chỉ số `record` cho popular
                            aggregated_result[keyword] += item["total_record"]
                        else:
                            # Xử lý toàn bộ trường hợp cho các type_top khác
                            aggregated_result[keyword]["record"] += item["total_record"]
                            if "top_20_days" in item:
                                aggregated_result[keyword]["top_20_days"] += item["top_20_days"]
                            if "trends" in item:
                                aggregated_result[keyword]["trends"] += item["trends"]

                # Chuyển sang khoảng thời gian tiếp theo
                current_start_date += step

            # Xử lý sắp xếp và trả về kết quả
            if request.type_top == "popular":
                # Sắp xếp riêng cho popular theo record
                sorted_keywords = sorted(
                    aggregated_result.items(),
                    key=lambda x: x[1],  # Sắp xếp theo số lượng record
                    reverse=True
                )
            else:
                # Sắp xếp logic ban đầu
                sorted_keywords = sorted(
                    aggregated_result.items(),
                    key=lambda x: (
                        x[1]['top_20_days'],
                        x[1]['trends'],
                        x[1]['record']
                    ),
                    reverse=True
                )

            # Xử lý logic danh sách trắng/đen nếu không phải popular
            if request.type_top != "popular":
                final_list = []
                non_white_list = []
                is_black_list = []

                for k, v in sorted_keywords:
                    if " " not in k and k not in white_list:
                        non_white_list.append((k, v))  # Từ khóa không thỏa mãn điều kiện
                    elif k in black_list2:
                        is_black_list.append((k, v))  # Từ khóa thuộc danh sách đen
                    else:
                        final_list.append((k, v))  # Từ khóa thỏa mãn điều kiện

                final_list.extend(non_white_list)
                final_list.extend(is_black_list)
                final_list = [(k.replace('_', ' '), v) for k, v in final_list]
                sorted_keywords = final_list

            # Phân trang kết quả
            start_index = (request.page - 1) * request.page_size
            end_index = start_index + request.page_size
            paged_keywords = sorted_keywords[start_index:end_index]

            # Tạo dữ liệu trả về
            if request.type_top == "popular":
                # Chỉ trả về keyword và record cho popular
                data = [
                    {
                        "keyword": k,
                        "total_record": v
                    }
                    for k, v in paged_keywords
                ]
            else:
                # Trả về đầy đủ dữ liệu cho các trường hợp khác
                data = [
                    {
                        "keyword": k,
                        "total_record": v["record"],
                        "top_20_days": v["top_20_days"],
                        "trends": v["trends"]
                    }
                    for k, v in paged_keywords
                ]

            result = {
                "status_code": 200,
                "message": "OK",
                "sum_records": len(sorted_keywords),
                "data": data
            }

            return result

        else:
            if not request.tenancy_ids:
                api_logger.warning("No tenancy_ids provided. Returning empty result.")
                return {
                    "status_code": 200,
                    "message": "No tenancy_ids specified",
                    "sum_records": 0,
                    "data": []
                }

            keyword_aggregate = defaultdict(int)

            query = {
                "bool": {
                    "must": [
                        {"terms": {"type": request.type}},
                        {"terms": {"topic_id": request.topic_ids}},
                        {"terms": {"tenant_id": request.tenancy_ids}},
                        {
                            "range": {
                                "date": {
                                    "gte": start_date.strftime("%m_%d_%Y"),
                                    "lte": end_date.strftime("%m_%d_%Y")
                                }
                            }
                        }
                    ]
                }
            }


            start_query_time = time.time()

            es_response = es_db.search(index=index_name, body={"query": query, "size": 10000},  timeout="60s" )
            end_query_time = time.time()
            query_duration = end_query_time - start_query_time
            api_logger.info(f"Elasticsearch query to {index_name} completed in {query_duration:.2f} seconds.")

            hits = es_response['hits']['hits']
            start_calc_time = time.time()

            if request.type_top == "popular":
                keyword_aggregate = defaultdict(int)
                for doc_source in hits:
                    doc = doc_source['_source']
                    if "keywords_top" not in doc:
                        continue
                    for kw in doc["keywords_top"]:
                        keyword = kw["keyword"]
                        record = kw["record"]
                        keyword_aggregate[keyword] += record
                        
            else:
                    keyword_aggregate = defaultdict(lambda: {"record": 0, "top_20_days": 0, "trends": 0})
                    for hit in hits:
                        hit_types = hit['_source'].get('type', [])
                        keywords_list = hit['_source']
                        if "keywords_trend" not in keywords_list:
                            continue
                        keywords_list = keywords_list["keywords_trend"]
                        for i, keyword_data in enumerate(keywords_list):
                            keyword = keyword_data.get("keyword")
                            if not keyword:
                                continue

                            keyword_aggregate[keyword]["record"] += keyword_data.get("record", 0)

                            if hit_types in {"voz", "otofun", "xamvn", "reddit"}:
                                continue  # forum: không tính top/trend

                            if hit_types in {"facebook", "media", "youtube"}:
                                if i < 10:
                                    keyword_aggregate[keyword]["top_20_days"] += 1
                                if keyword_data.get("isTrend", False):
                                    keyword_aggregate[keyword]["trends"] += 1
                            else:
                                if i < 10:
                                    keyword_aggregate[keyword]["top_20_days"] += 0.5
                                if keyword_data.get("isTrend", False):
                                    keyword_aggregate[keyword]["trends"] += 0.5
                    # Sắp xếp
                    sorted_keywords = sorted(
                        keyword_aggregate.items(),
                        key=lambda x: (
                            x[1]['top_20_days'],
                            x[1]['trends'],
                            x[1]['record']
                        ),
                        reverse=True
                    )

                    # Lọc whitelist/blacklist
                    final_list = []
                    non_white_list = []
                    is_black_list = []
                    for k, v in sorted_keywords:
                        if "_" not in k and k not in white_list:
                            non_white_list.append((k, v))
                        elif k in black_list:
                            is_black_list.append((k, v))
                        else:
                            final_list.append((k, v))

                    final_list.extend(non_white_list)
                    final_list.extend(is_black_list)
                    final_list = [(k.replace('_', ' '), v) for k, v in final_list]

                    sorted_keywords = final_list


            if request.type_top == "popular":
                # sorted_keywords = sorted(keyword_aggregate.items(), key=lambda x: x[1], reverse=True)
                sorted_keywords = sorted(
                                            {k.replace('_', ' '): v for k, v in keyword_aggregate.items()}.items(),
                                            key=lambda x: x[1], 
                                            reverse=True
                                        )


            if request.keyword:
                similar_keywords = [(k, v) for k, v in sorted_keywords if fuzz.ratio(request.keyword, k) > 90]
            else:
                similar_keywords = sorted_keywords
                # similar_keywords = [(k, v) for k, v in sorted_keywords]

            start_index = (request.page - 1) * request.page_size
            end_index = start_index + request.page_size
            paged_keywords = similar_keywords[start_index:end_index]

            # data = [{"keyword": k.replace('_', ' '), "total_record": v["record"] if isinstance(v, dict) else v} for k, v in paged_keywords]
            if request.type_top == "popular":
                data = [
                        {
                            # "keyword": k,
                            "keyword": k.replace('_', ' '),
                            "total_record": v["record"] if isinstance(v, dict) else v,
                            # "top_20_days": v["top_20_days"],
                            # "trends": v["trends"]
                        }
                        for k, v in paged_keywords]
            else:
                data = [
                    {
                        # "keyword": k,
                        "keyword": k.replace('_', ' '),
                        "total_record": v["record"] if isinstance(v, dict) else v,
                        "top_20_days": v["top_20_days"],
                        "trends": v["trends"]
                    }
                    for k, v in paged_keywords]

            result = {
                "status_code": 200,
                "message": "OK",
                # "sum_records": sum(v["record"] if isinstance(v, dict) else v for _, v in similar_keywords),
                "sum_records": len(similar_keywords),

                "data": data
            }
            end_calc_time = time.time()
            calc_duration = end_calc_time - start_calc_time
            print(f"Thời gian tính toán: {calc_duration:.2f} giây")

            return result

    except Exception as e:
        api_logger.error(f"Error in search_keywords: {e}")

        response.status_code = 500
        return {"status_code": 500, "message": "Internal server error", "error": str(e)}



@app.post("/search_hashtag/")
async def search_hashtag_api(request: SearchRequesthastag, response: Response,bypass_cache: bool = False ):
    # start_date = datetime.strptime(request.start_date, "%m/%d/%Y")
    # end_date = datetime.strptime(request.end_date, "%m/%d/%Y")
    # if (end_date - start_date).days > 64:
    #     start_date = end_date - timedelta(days=64)
    #     request.start_date = start_date.strftime("%m/%d/%Y")

    if not bypass_cache and is_request_cacheable(request):
        try:
            api_logger.info("Query from cache search_hashtag")

            period_mapping = {
                "0": "today",
                "7": "last_7_days",
                "30": "last_30_days"
            }
            days_diff = (
                datetime.strptime(request.end_date, "%m/%d/%Y") -
                datetime.strptime(request.start_date, "%m/%d/%Y")
            ).days
            cache_period = period_mapping.get(str(days_diff), "unknown")

            # ✅ Lấy tenancy_id đầu tiên làm phần cache_key
            tenancy_part = request.tenancy_ids[0] if request.tenancy_ids else "none"

            # ✅ Cache key đầy đủ
            cache_key = f"search_hashtag:{cache_period}:{tenancy_part}"

            cached_result = get_data_from_cache(cache_key, request.page, request.page_size)
            if cached_result:
                api_logger.info(f"Cache hit: {cache_key}")
                return cached_result

        except Exception as e:
            api_logger.error(f"Lỗi API cache search_hashtag: {e}")

    tenancy_part = ','.join(sorted(request.tenancy_ids)) if request.tenancy_ids else 'none'

    cache_key_general = (
        f"search_hashtag:"
        f"{request.type}:"
        f"{request.start_date}:"
        f"{request.end_date}:"
        f"{','.join(sorted(request.topic_ids))}:"
        f"{request.page}:"
        f"{request.page_size}:"
        f"{request.hashtag or 'none'}:"
        f"{tenancy_part}"
    )
    # cache_key_general = f"search_hashtag:{request.type}:{request.start_date}:{request.end_date}:{','.join(sorted(request.topic_ids))}:{request.page}:{request.page_size}:{request.hashtag}"
    # Nếu không lấy từ Redis, chọc vào Elasticsearch
    # cache_key_general = f"search_hashtag:{request.type}:{request.start_date}:{request.end_date}:{','.join(sorted(request.topic_ids))}:{request.page}:{request.page_size}:{request.hashtag}"
    try:
        cached_general_result = r.get(cache_key_general)
        if cached_general_result:
            api_logger.info(f"Query from general cache: {cache_key_general}")
            cached_general_result = json.loads(cached_general_result)
            return cached_general_result
    except RedisError as e:
        api_logger.error(f"Error checking general cache for search_hashtag: {e}")

    # Nếu không có dữ liệu trong cache, chọc vào Elasticsearch
    start_time = time.time()
    api_logger.info(f"/search_hashtag endpoint called with request: {request.dict()}")

    try:
        # Gọi Elasticsearch để lấy dữ liệu
        result = await call_with_retry(search_hashtag, request, response)
        api_logger.info(f"/search_hashtag completed successfully in {time.time() - start_time:.2f} seconds.")
        if not bypass_cache:

        # Trả về kết quả cho người dùng ngay lập tức
            asyncio.create_task(save_to_cache(cache_key_general, result))
        return result

    except Exception as e:
        api_logger.error(f"/search_hashtag failed: {e}")
        raise HTTPException(status_code=500, detail="Internal server error.")

async def search_hashtag(request: SearchRequesthastag, response: Response):
    try:
        try:
            start_date = datetime.strptime(request.start_date, "%m/%d/%Y")
            end_date = datetime.strptime(request.end_date, "%m/%d/%Y")
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        except ValueError:
            response.status_code = 400
            return {"status_code": 400, "message": "Invalid date format, please use MM/DD/YYYY", "sum_records": 0, "data": []}
        if (end_date - start_date).days > 80:
            api_logger.info("Handling large time range (over 80 days) for hashtags. Splitting into smaller chunks.")

            step = timedelta(days=20)
            current_start_date = start_date
            hashtag_aggregate = defaultdict(int)

            while current_start_date < end_date:
                current_end_date = min(current_start_date + step - timedelta(days=1), end_date)
                api_logger.info(f"Querying data from {current_start_date.strftime('%m/%d/%Y')} to {current_end_date.strftime('%m/%d/%Y')}")

                # Tạo một yêu cầu nhỏ hơn với khoảng thời gian giới hạn
                sub_request = SearchRequesthastag(
                    hashtag=request.hashtag,
                    type=request.type,
                    start_date=current_start_date.strftime("%m/%d/%Y"),
                    end_date=current_end_date.strftime("%m/%d/%Y"),
                    page=1,
                    page_size=10000,
                    topic_ids=request.topic_ids,
                    tenancy_ids=request.tenancy_ids
                )

                # Gọi hàm `search_hashtag` để lấy dữ liệu
                sub_result = await search_hashtag(sub_request, response)

                if sub_result["status_code"] == 200:
                    for item in sub_result["data"]:
                        hashtag = item["hashtag"]
                        hashtag_aggregate[hashtag] += item["total_record"]

                # Chuyển sang khoảng thời gian tiếp theo
                current_start_date += step

            # Sắp xếp kết quả theo tổng số lượng
            sorted_hashtags = sorted(
                hashtag_aggregate.items(),
                key=lambda x: x[1],  # Sắp xếp theo tổng số lượng record
                reverse=True,
            )

            # Phân trang kết quả
            start_index = (request.page - 1) * request.page_size
            end_index = start_index + request.page_size
            paged_hashtags = sorted_hashtags[start_index:end_index]

            # Tạo dữ liệu trả về
            data = [{"hashtag": k, "total_record": v} for k, v in paged_hashtags]

            result = {
                "status_code": 200,
                "message": "OK",
                "sum_records": len(sorted_hashtags),  # Tổng số hashtag
                "data": data,
            }

            return result
        if not request.tenancy_ids:
            api_logger.warning("No tenancy_ids provided. Returning empty result.")
            return {
                "status_code": 200,
                "message": "No tenancy_ids specified",
                "sum_records": 0,
                "data": []
            }

        query = {
                "bool": {
                    "must": [
                        {"terms": {"type": request.type}},
                        {"terms": {"topic_id": request.topic_ids}},
                        {"terms": {"tenant_id": request.tenancy_ids}},
                        {
                            "range": {
                                "date": {
                                    "gte": start_date.strftime("%m_%d_%Y"),
                                    "lte": end_date.strftime("%m_%d_%Y")
                                }
                            }
                        }
                    ]
                }
            }
        start_query_time = time.time()

        es_response = es_db.search(index="key-osint1-current", body={"query": query, "size": 10000})
        hits = es_response['hits']['hits']
        start_calc_time = time.time()

        end_query_time = time.time()
        query_duration = end_query_time - start_query_time
        api_logger.info(f"Elasticsearch query to key-osint1-current - hashtag completed in {query_duration:.2f} seconds.")

        hashtag_aggregate = defaultdict(int)
        for doc_source in hits:
            doc = doc_source['_source']
            if "hashtags_top" not in doc:
                continue
            for kw in doc["hashtags_top"]:
                keyword = kw["hashtag"]
                record = kw["record"]
                hashtag_aggregate[keyword] += record
            # else:
            #     # fallback nếu không có tenancy_ids
            #     tenancy_topics = tenancy_data.get("all", {})
            #     if request.topic_ids:
            #         for topic_id in request.topic_ids:
            #             hashtags_top = tenancy_topics.get(topic_id, {}).get("hashtags_top", [])
            #             for hashtag_data in hashtags_top:
            #                 hashtag_aggregate[hashtag_data["hashtag"]] += hashtag_data["record"]
            #     else:
            #         for topic in tenancy_topics.values():
            #             hashtags_top = topic.get("hashtags_top", [])
            #             for hashtag_data in hashtags_top:
            #                 hashtag_aggregate[hashtag_data["hashtag"]] += hashtag_data["record"]

        sorted_hashtags = sorted(hashtag_aggregate.items(), key=lambda x: x[1], reverse=True)
        if request.hashtag:
            similar_hashtags = [(k, v) for k, v in sorted_hashtags if fuzz.ratio(request.hashtag, k) > 65]
        else:
            similar_hashtags = sorted_hashtags


        start_index = (request.page - 1) * request.page_size
        end_index = start_index + request.page_size
        paged_hashtags = similar_hashtags[start_index:end_index]

        data = [{"hashtag": k, "total_record": v["record"] if isinstance(v, dict) else v} for k, v in paged_hashtags]

        result = {
            "status_code": 200,
            "message": "OK",
            # "sum_records": sum(v["record"] if isinstance(v, dict) else v for _, v in similar_hashtags),
            "sum_records": len(similar_hashtags),

            "data": data
        }

        end_calc_time = time.time()
        calc_duration = end_calc_time - start_calc_time
        print(f"Thời gian tính toán: {calc_duration:.2f} giây")



        return result

    except Exception as e:
        api_logger.error(f"Error in search_hashtag: {e}")

        response.status_code = 500
        return {"status_code": 500, "message": str(e), "sum_records": 0, "data": []}


if __name__ == "__main__":
    import uvicorn
    # uvicorn.run(app, host="0.0.0.0", port=55555)
    uvicorn.run(app, host="localhost", port=55555)
