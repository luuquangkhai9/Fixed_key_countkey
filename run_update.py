import redis
import requests
from datetime import datetime, timedelta
import json
import time
from elasticsearch import Elasticsearch
import logging
from dotenv import load_dotenv
import os
from main_keyword_top import calculate_top_keywords_with_topic_2_es, calculate_top_keywords_with_trend_logic_topic
from main_query_es import  query_keyword_with_topic

# Load environment variables
load_dotenv()

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")  # Default to 'localhost' if not set
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))    # Default to 6379 if not set
CACHE_EXPIRATION_SECONDS = 30000  # 40 minutes

# API Configuration
API_HOST = os.getenv("API_HOST", "localhost")      # Default to 'localhost' if not set
API_PORT = os.getenv("API_PORT", 5601)             # Default to 8000 if not set
API_BASE_URL = f"http://{API_HOST}:{API_PORT}"

# Elasticsearch Configuration
elasticsearch_url = os.getenv("ELASTICSEARCH_URL")
elasticsearch_db_url = os.getenv("ELASTICSEARCH_DB_URL")
es = Elasticsearch([elasticsearch_url], request_timeout=100)
es_db = Elasticsearch([elasticsearch_db_url], request_timeout=100)

# Logging Configuration
# logging.basicConfig(filename='/usr/app/src/cache_update.log', level=logging.INFO, 
#                     format='%(asctime)s:%(levelname)s:%(message)s')
logging.basicConfig(filename='cache_update.log', level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Redis Connection
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
historical_data_index = "key-osint1-current"
historical_data_index_trends = "key-trend-current"
keyword_top_file = 'keyword_percentages_main_title.json'
keyword_extract_file = 'keyword_test_27.1_filter_new_3.json'
keyword_today_file = 'keyword_percentages_main_title_noun_phase.json'
query_data_file_fb = 'content_test_newquery_filter_fb.json'
query_data_file_tik = 'content_test_newquery_filter_tik.json'
query_data_file_ytb = 'content_test_newquery_filter_ytb.json'
query_data_file_voz = 'content_test_newquery_filter_voz.json'
query_data_file_xamvn = 'content_test_newquery_filter_xam.json'
query_data_file_oto = 'content_test_newquery_filter_oto.json'
query_data_file_media = 'content_test_newquery_filter_media.json'

interval_hours = 2

# Helper function to calculate date range
def calculate_date_range(days_back):
    end_date = datetime.today() 
    start_date = end_date - timedelta(days=days_back)
    return start_date.strftime("%m/%d/%Y"), end_date.strftime("%m/%d/%Y")

# Function to call API and cache the result
def call_api_and_update_cache(endpoint, request_data, cache_key):
    try:
        response = requests.post(f"{API_BASE_URL}/{endpoint}?bypass_cache=true", json=request_data)
        if response.status_code == 200:
            new_data = response.json()

            # Check if key already exists in Redis
            cached_data = r.get(cache_key)
            if cached_data:
                # Merge new data with existing data
                cached_data = json.loads(cached_data)
                cached_data["data"] = new_data["data"]  # Update the "data" field
                cached_data["sum_records"] = new_data["sum_records"]  # Update the record count
            else:
                # If the key doesn't exist, use new data
                cached_data = new_data

            # Save updated data back to Redis
            r.set(cache_key, json.dumps(cached_data), ex=CACHE_EXPIRATION_SECONDS)
            logging.info(f"Updated cache for key: {cache_key}")
        else:
            logging.error(f"Failed to call API {endpoint}. Status code: {response.status_code}")
    except Exception as e:
        logging.error(f"Error calling API {endpoint}: {e}")

# # Clear old cache
# def clear_old_cache():
#     keys = r.keys("search_*")
#     if keys:
#         r.delete(*keys)
#         logging.info(f"Cleared old cache: {len(keys)} keys deleted")

# Update all cache
# def update_all_cache():
#     date_ranges = {
#         "today": calculate_date_range(0),
#         "last_7_days": calculate_date_range(7),
#         "last_30_days": calculate_date_range(30),
#     }

#     type_values = ["media", "facebook", "tiktok", "youtube", "voz", "xamvn", "otofun", "reddit"]

#     for key, (start_date, end_date) in date_ranges.items():
#         request_data_popular = {
#             "keyword": "",
#             "type": type_values,
#             "start_date": start_date,
#             "end_date": end_date,
#             "type_top": "popular",
#             "page": 1,
#             "page_size": 1000000,
#             "topic_ids": [],
#             "tenancy_ids": [tenancy_id]

#         }
#         call_api_and_update_cache("search_keywords", request_data_popular, f"search_keywords:popular:{key}")
#         time.sleep(0.5)
#         request_data_trend = {
#             "keyword": "",
#             "type": type_values,
#             "start_date": start_date,
#             "end_date": end_date,
#             "type_top": "trend",
#             "page": 1,
#             "page_size": 1000000,
#             "topic_ids": []
#         }
#         call_api_and_update_cache("search_keywords", request_data_trend, f"search_keywords:trend:{key}")
#         time.sleep(0.5)

#         request_data_hashtag = {
#             "hashtag": "",
#             "type": type_values,
#             "start_date": start_date,
#             "end_date": end_date,
#             "page": 1,
#             "page_size": 1000000,
#             "topic_ids": []
#         }
#         call_api_and_update_cache("search_hashtag", request_data_hashtag, f"search_hashtag:{key}")
#         time.sleep(0.5)
def update_all_cache():
    date_ranges = {
        "today": calculate_date_range(0),
        "last_7_days": calculate_date_range(7),
        "last_30_days": calculate_date_range(30),
    }

    type_values = ["media", "facebook", "tiktok", "youtube", "voz", "xamvn", "otofun", "reddit"]

    # Danh sách tenancy_id bạn muốn cập nhật (có thể lấy từ DB hoặc config)
    tenancy_ids = [ "A05"]  # <-- tùy bạn thay đổi

    for tenancy_id in tenancy_ids:
        for key, (start_date, end_date) in date_ranges.items():
            tenancy_part = tenancy_id

            request_data_popular = {
                "keyword": "",
                "type": type_values,
                "start_date": start_date,
                "end_date": end_date,
                "type_top": "popular",
                "page": 1,
                "page_size": 1000000,
                "topic_ids": ["all"],
                "tenancy_ids": [tenancy_id]
            }
            call_api_and_update_cache(
                "search_keywords",
                request_data_popular,
                f"search_keywords:popular:{key}:{tenancy_part}"
            )
            time.sleep(0.5)

            request_data_trend = {
                "keyword": "",
                "type": type_values,
                "start_date": start_date,
                "end_date": end_date,
                "type_top": "trend",
                "page": 1,
                "page_size": 1000000,
                "topic_ids": ["all"],
                "tenancy_ids": [tenancy_id]
            }
            call_api_and_update_cache(
                "search_keywords",
                request_data_trend,
                f"search_keywords:trend:{key}:{tenancy_part}"
            )
            time.sleep(0.5)

            request_data_hashtag = {
                "hashtag": "",
                "type": type_values,
                "start_date": start_date,
                "end_date": end_date,
                "page": 1,
                "page_size": 1000000,
                "topic_ids": ["all"],
                "tenancy_ids": [tenancy_id]
            }
            call_api_and_update_cache(
                "search_hashtag",
                request_data_hashtag,
                f"search_hashtag:{key}:{tenancy_part}"
            )
            time.sleep(0.5)

# Query and extract keywords from Elasticsearch
def query_and_extract_keywords(es, start_time_str, end_time_str , type ):
    logging.info(f"Start querying keywords for type '{type}' from {start_time_str} to {end_time_str}")

    try:
        dataFramse_Log = query_keyword_with_topic(es, start_time_str, end_time_str, type)
        logging.info(f"Query successful for type '{type}'")
        return dataFramse_Log
    except Exception as e:
        logging.error(f"Error querying keywords for type '{type}': {e}")
        return []

# Process and update data for a specific day and platform
def process_and_update_day(es_db, current_day_str, extracted_keywords, platform_type):
    # Tính toán top từ khóa cho nền tảng và ngày hiện tại
    logging.info(f"Processing and updating keywords for platform '{platform_type}' on {current_day_str}")

    try:

        top_keywords = calculate_top_keywords_with_topic_2_es(es_db, current_day_str, extracted_keywords, historical_data_index, platform_type)
        del top_keywords  # Giải phóng bộ nhớ sau khi xử lý

        top_keywords_trends = calculate_top_keywords_with_trend_logic_topic(current_day_str, es_db, historical_data_index, platform_type)
        del top_keywords_trends  # Giải phóng bộ nhớ sau khi xử lý
        logging.info(f"Successfully processed and updated keywords for platform '{platform_type}' on {current_day_str}")
    except Exception as e:
        logging.error(f"Error processing and updating keywords for platform '{platform_type}' on {current_day_str}: {e}")

# Run update for Elasticsearch
def run_update():
    today = datetime.today()
    start_day = today - timedelta(days=8)
    current_day = start_day
    end_day = today

    while current_day <= end_day:
        next_day = current_day + timedelta(days=4)
        next_day_str = next_day.strftime("%m/%d/%Y 23:59:59")

        for platform in ["media", "facebook", "tiktok", "youtube", "voz", "xamvn", "otofun","reddit"]:
            extracted_keywords = query_and_extract_keywords(es, current_day.strftime("%m/%d/%Y 00:00:00"), next_day_str, platform)
            for day_offset in range(5):
                day_to_process = current_day + timedelta(days=day_offset)
                day_to_process_str = day_to_process.strftime("%m/%d/%Y")
                process_and_update_day(es_db, day_to_process_str, extracted_keywords, platform)
            del extracted_keywords

        current_day += timedelta(days=4)

    logging.info("Update complete.")

# Periodic cache update
def periodic_cache_update():
    while True:
        logging.info("Starting cache update...")
        update_all_cache()
        logging.info("Cache update complete. Waiting for 40 minutes...")
        time.sleep(2700)

# Schedule Elasticsearch update
def schedule_update():
    while True:
        logging.info("Luong update khoi dong")

        current_time = datetime.now()
        if current_time.hour == 14 :
            logging.info("Bat dau  update !")
            run_update()
        
        time.sleep(3600)

if __name__ == "__main__":
    from multiprocessing import Process
#testdebug
#   cache_process = Process(target=periodic_cache_update)
#    update_process = Process(target=schedule_update)



    #periodic_cache_update()



    
#    cache_process.start()
#    update_process.start()

#    cache_process.join()
#    update_process.join() 
