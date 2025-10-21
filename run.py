import numpy as np
import re
# from vncorenlp import VnCoreNLP
import json
# from langdetect import detect 
# from langdetect import detect as detect2
from main_query_es import  query_keyword_with_topic
from collections import defaultdict
from main_keyword_top import  calculate_top_keywords_with_topic_2_es, calculate_top_keywords_with_trend_logic_topic
import time
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
from time import sleep
from keyword_save_es import  load_data_to_elasticsearch_kw_a
import logging
import urllib3
import redis
from dotenv import load_dotenv
import os
load_dotenv()

logging.basicConfig(
    #filename="D:/ChienND/Team AI Security/TuDH/hastag/run.log",
     filename="run.log",

    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s"
)
logger = logging.getLogger("run")

print(logging.getLogger("elasticsearch").handlers)
print(logging.getLogger("urllib3").handlers)

# Giảm mức log cho `urllib3` (sử dụng bởi Elasticsearch)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.getLogger("urllib3").propagate = False

# Giảm log cho Elasticsearch (nếu cần thiết)
logging.getLogger("elasticsearch").setLevel(logging.CRITICAL)
# Remove all handlers from urllib3
class ExcludeHttpLogsFilter(logging.Filter):
    def filter(self, record):
        # Lọc bỏ các log chứa "POST" hoặc "HEAD"
        return "POST" not in record.getMessage() and "HEAD" not in record.getMessage()

# Thêm bộ lọc vào logger

logging.getLogger("elasticsearch").addFilter(ExcludeHttpLogsFilter())
logging.getLogger("urllib3").addFilter(ExcludeHttpLogsFilter())
logging.getLogger("elasticsearch").handlers.clear()
logging.getLogger("urllib3").handlers.clear()

# Lấy URL Elasticsearch từ biến môi trường
elasticsearch_url = os.getenv("ELASTICSEARCH_URL")
elasticsearch_db_url = os.getenv("ELASTICSEARCH_DB_URL")


restart_needed = False
#es = Elasticsearch([elasticsearch_url], request_timeout=100)

es_db = Elasticsearch([elasticsearch_db_url], request_timeout=100)

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

def query_and_extract_keywords(es, start_time_str, end_time_str , type ):
    logging.info(f"Start querying keywords for type '{type}' from {start_time_str} to {end_time_str}")
    print(f"Start querying keywords for type '{type}' from {start_time_str} to {end_time_str}")

    try:
        dataFramse_Log = query_keyword_with_topic(es, start_time_str, end_time_str, type)
        # logging.info(f"Query successful for type '{type}'")
        return dataFramse_Log
    except Exception as e:
        # logging.error(f"Error querying keywords for type '{type}': {e}")
        return []

def process_and_update_day(es_db, current_day_str, extracted_keywords, platform_type):
    # Tính toán top từ khóa cho nền tảng và ngày hiện tại
    logging.info(f"Processing and updating keywords for platform '{platform_type}' on {current_day_str}")
    print(f"Processing and updating keywords for platform '{platform_type}' on {current_day_str}")

    try:

        calculate_top_keywords_with_topic_2_es(es_db, current_day_str, extracted_keywords, historical_data_index, platform_type)
        # Giải phóng bộ nhớ sau khi xử lý

        top_keywords_trends = calculate_top_keywords_with_trend_logic_topic(current_day_str, es_db, historical_data_index, platform_type)
        del top_keywords_trends  # Giải phóng bộ nhớ sau khi xử lý
        logging.info(f"Successfully processed and updated keywords for platform '{platform_type}' on {current_day_str}")
    except Exception as e:
        logging.error(f"Error processing and updating keywords for platform '{platform_type}' on {current_day_str}: {e}")

def run_keyword_all_day():
    input_day = datetime.today()-timedelta(days=1)
    historical_data = []
    seven_days_before_input = input_day - timedelta(days=16)
    last_day = seven_days_before_input
    current_day = last_day
    
    while current_day <= input_day:
        current_day_str = current_day.strftime("%m/%d/%Y")
        next_day = current_day + timedelta(days=8)
        if next_day > input_day:
            next_day = input_day  # Đảm bảo không vượt quá ngày kết thúc

        next_day_str = next_day.strftime("%m/%d/%Y 23:59:59")

        if not any(record['date'] == current_day_str for record in historical_data):
            sleep(2)
            '''
            extracted_keywords_fb = query_and_extract_keywords(es_db, current_day.strftime("%m/%d/%Y 00:00:00"), next_day_str, "facebook")
            for day_offset in range((next_day - current_day).days+1):
                day_to_process = current_day + timedelta(days=day_offset)
                day_to_process_str = day_to_process.strftime("%m/%d/%Y")
                process_and_update_day(es_db, day_to_process_str, extracted_keywords_fb, "facebook")
            del extracted_keywords_fb  # Giải phóng bộ nhớ sau khi xử lý
            print('done facebook!')
            # TikTok
            extracted_keywords_tik = query_and_extract_keywords(es_db, current_day.strftime("%m/%d/%Y 00:00:00"), next_day_str, "tiktok")
            for day_offset in range((next_day - current_day).days+1):
                day_to_process = current_day + timedelta(days=day_offset)
                day_to_process_str = day_to_process.strftime("%m/%d/%Y")
                process_and_update_day(es_db, day_to_process_str, extracted_keywords_tik, "tiktok")
            del extracted_keywords_tik  # Giải phóng bộ nhớ sau khi xử lý
            print('done tiktok!')

            # YouTube
            extracted_keywords_ytb = query_and_extract_keywords(es_db, current_day.strftime("%m/%d/%Y 00:00:00"), next_day_str, "youtube")
            for day_offset in range((next_day - current_day).days+1):
                day_to_process = current_day + timedelta(days=day_offset)
                day_to_process_str = day_to_process.strftime("%m/%d/%Y")
                process_and_update_day(es_db, day_to_process_str, extracted_keywords_ytb, "youtube")
            del extracted_keywords_ytb  # Giải phóng bộ nhớ sau khi xử lý
            print('done youtube!')

            # VOZ
            extracted_keywords_voz = query_and_extract_keywords(es_db, current_day.strftime("%m/%d/%Y 00:00:00"), next_day_str, "voz")
            for day_offset in range((next_day - current_day).days+1):
                day_to_process = current_day + timedelta(days=day_offset)
                day_to_process_str = day_to_process.strftime("%m/%d/%Y")
                process_and_update_day(es_db, day_to_process_str, extracted_keywords_voz, "voz")
            del extracted_keywords_voz  # Giải phóng bộ nhớ sau khi xử lý
            print('done voz!')

            # XAMVN
            extracted_keywords_xamvn = query_and_extract_keywords(es_db, current_day.strftime("%m/%d/%Y 00:00:00"), next_day_str, "xamvn")
            for day_offset in range((next_day - current_day).days+1):
                day_to_process = current_day + timedelta(days=day_offset)
                day_to_process_str = day_to_process.strftime("%m/%d/%Y")
                process_and_update_day(es_db, day_to_process_str, extracted_keywords_xamvn, "xamvn")
            del extracted_keywords_xamvn  # Giải phóng bộ nhớ sau khi xử lý
            print('done xamvn!')

            # OTOFUN
            extracted_keywords_oto = query_and_extract_keywords(es_db, current_day.strftime("%m/%d/%Y 00:00:00"), next_day_str, "otofun")
            for day_offset in range((next_day - current_day).days+1):
                day_to_process = current_day + timedelta(days=day_offset)
                day_to_process_str = day_to_process.strftime("%m/%d/%Y")
                process_and_update_day(es_db, day_to_process_str, extracted_keywords_oto, "otofun")
            del extracted_keywords_oto  # Giải phóng bộ nhớ sau khi xử lý
            print('done otofun!')

            # Reddit
            extracted_keywords_reddit = query_and_extract_keywords(es_db, current_day.strftime("%m/%d/%Y 00:00:00"), next_day_str, "reddit")
            for day_offset in range((next_day - current_day).days+1):
                day_to_process = current_day + timedelta(days=day_offset)
                day_to_process_str = day_to_process.strftime("%m/%d/%Y")
                process_and_update_day(es_db, day_to_process_str, extracted_keywords_reddit, "reddit")
            del extracted_keywords_reddit  # Giải phóng bộ nhớ sau khi xử lý
            print('done reddit!')

            '''
            # MEDIA
            #testdebug
            extracted_keywords_media = query_and_extract_keywords(es_db, current_day.strftime("%m/%d/%Y 00:00:00"), next_day_str, "media")
#            extracted_keywords_media = []
            for day_offset in range((next_day - current_day).days+1):
                day_to_process = current_day + timedelta(days=day_offset)
                day_to_process_str = day_to_process.strftime("%m/%d/%Y")
                #testdebug
                process_and_update_day(es_db, day_to_process_str, extracted_keywords_media, "media")
            del extracted_keywords_media  # Giải phóng bộ nhớ sau khi xử lý
            print('done media!')



            # Chuyển sang khoảng thời gian tiếp theo (4 ngày)
            current_day += timedelta(days=8)
            print('done all!')


def get_latest_hour_from_data(data):
    latest_datetime = None

    for entry in data:
        created_time_str = entry.get('_source', {}).get('created_time', '')
        
        if created_time_str:
            created_time = datetime.strptime(created_time_str, "%m/%d/%Y %H:%M:%S")
            
            if not latest_datetime or created_time > latest_datetime:
                latest_datetime = created_time

    if latest_datetime:
        return latest_datetime.hour
    else:
        return 0
def get_latest_datetime_from_data(data):
    latest_datetime = None

    for entry in data:
        created_time_str = entry.get('_source', {}).get('created_time', '')
        
        if created_time_str:
            created_time = datetime.strptime(created_time_str, "%m/%d/%Y %H:%M:%S")
            
            if not latest_datetime or created_time > latest_datetime:
                latest_datetime = created_time

    return latest_datetime


def merge_extracted_keywords(old_data, new_data): 
    for key, value in new_data.items():
        if key not in old_data:
            old_data[key] = value
        else:
            old_keywords = set(old_data[key]['hashtag'])
            new_keywords = set(value['hashtag'])
            combined_keywords = list(old_keywords.union(new_keywords))
            old_data[key]['hashtag'] = combined_keywords
            if old_data[key]['created_time'] < value['created_time']:
                old_data[key]['title'] = value['title']
                old_data[key]['created_time'] = value['created_time']
    return old_data

# def summarize_keywords_in_intervals(collection , type , query_data_file):
def summarize_keywords_in_intervals(type ,  old_extracted_keywords):
    logging.info(f"Starting summarize_keywords_in_intervals for type '{type}'")    
    start_bool = False
    top_keywords = {}
    if old_extracted_keywords is None:
        old_extracted_keywords = []  # Khởi tạo danh sách rỗng nếu giá trị là None

    # try:
    #     with open(query_data_file, 'r', encoding='utf-8') as file:
    #         old_extracted_keywords =  json.load(file)
    # except Exception as e:
    #     print(e)
    #     old_extracted_keywords =  []
    latest_datetime = get_latest_datetime_from_data(old_extracted_keywords)
    print(latest_datetime)

    # now = datetime.now()- timedelta(days=114)
    now = datetime.now()

    current_time = now.replace(minute=0, second=0, microsecond=0)
    if latest_datetime:
        start_of_day = latest_datetime.replace(minute=0, second=0, microsecond=0)
        additional_hours = (interval_hours - start_of_day.hour % interval_hours) % interval_hours
        if additional_hours > 0:
            start_of_day += timedelta(hours=additional_hours)

    else:
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    top_keywords_summary = {}
    
    while start_of_day < current_time:
        end_of_interval = start_of_day + timedelta(hours=interval_hours)
        if end_of_interval > current_time:
            end_of_interval = current_time
            
        print(start_of_day.strftime("%m/%d/%Y %H:%M:%S"))
        print(end_of_interval.strftime("%m/%d/%Y %H:%M:%S"))
        logging.info(f"Processing interval from {start_of_day.strftime('%m/%d/%Y %H:%M:%S')} to {end_of_interval.strftime('%m/%d/%Y %H:%M:%S')}")
        print(f"Processing interval from {start_of_day.strftime('%m/%d/%Y %H:%M:%S')} to {end_of_interval.strftime('%m/%d/%Y %H:%M:%S')}")
        extracted_keywords = query_and_extract_keywords(es_db , start_of_day.strftime("%m/%d/%Y %H:%M:%S"), end_of_interval.strftime("%m/%d/%Y %H:%M:%S") , type )
        current_day_str = start_of_day.strftime("%m/%d/%Y")
        old_extracted_keywords = old_extracted_keywords + extracted_keywords
        top_keywords = calculate_top_keywords_with_topic_2_es(es_db , current_day_str, old_extracted_keywords, historical_data_index, type)
        sleep(1.6)
        top_keywords_trends = calculate_top_keywords_with_trend_logic_topic(current_day_str, es_db, historical_data_index,type )
        start_of_day = end_of_interval
        if top_keywords["topic_ids"].get("all", {}).get("keywords_top") or top_keywords["topic_ids"].get("all", {}).get("hashtags_top"):
            start_bool = True
            load_data_to_elasticsearch_kw_a(es_db , top_keywords, historical_data_index)
            sleep(1.5)
            load_data_to_elasticsearch_kw_a(es_db , top_keywords_trends, historical_data_index_trends)
            
    logging.info("Finished summarizing all intervals")
    
    return start_bool, old_extracted_keywords
                                                                      
        
def run_keyword_today():
    current_day = datetime.now()-timedelta(days = 1)
    #current_day = datetime.now()

    global restart_needed  # Sử dụng biến toàn cục
    #testdebug
    collections = ['facebook', 'tiktok', 'youtube', 'forums', 'media']
    #collections = ['forums']

    # query_data_files = [query_data_file_fb, query_data_file_tik, query_data_file_ytb, query_data_file_voz, query_data_file_xamvn, query_data_file_oto, query_data_file_media]
    #top_keywords_summarys = [top_keywords_summary_fb, top_keywords_summary_tik, top_keywords_summary_ytb, top_keywords_summary_voz, top_keywords_summary_xamvn, top_keywords_summary_oto, top_keywords_summary_media, top_keywords_summary_reddit]
    while True:
        # now = datetime.now() - timedelta(days=114)
        now = datetime.now()
        #empty_collections_count = 0  # Biến đếm số lượng collections có len(top_keywords_summary) == 0

        if now.day != current_day.day:
 
            while current_day < now:
                start_of_day = current_day.replace(hour=0, minute=0, second=0, microsecond=0)
                end_of_interval = current_day.replace(hour=23, minute=59, second=59, microsecond=59)

                print(start_of_day.strftime("%m/%d/%Y %H:%M:%S"))
                print(end_of_interval.strftime("%m/%d/%Y %H:%M:%S"))
                logging.info(f"Processing interval from {start_of_day.strftime('%m/%d/%Y %H:%M:%S')} to {end_of_interval.strftime('%m/%d/%Y %H:%M:%S')}")
                print(f"Processing interval from {start_of_day.strftime('%m/%d/%Y %H:%M:%S')} to {end_of_interval.strftime('%m/%d/%Y %H:%M:%S')}")
                for coll in collections:
                    extracted_keywords = query_and_extract_keywords(es_db , start_of_day.strftime("%m/%d/%Y %H:%M:%S"), end_of_interval.strftime("%m/%d/%Y %H:%M:%S") , coll )
                    current_day_str = start_of_day.strftime("%m/%d/%Y")
                    calculate_top_keywords_with_topic_2_es(es_db , current_day_str, extracted_keywords, historical_data_index, coll)
                    calculate_top_keywords_with_trend_logic_topic(current_day_str, es_db, historical_data_index,coll )
                current_day +=timedelta(days=1)
                time.sleep(60)
        
        if now.hour > interval_hours or now.hour == interval_hours:

            start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end_of_interval = now.replace( minute=0, second=0, microsecond=0)

            print(start_of_day.strftime("%m/%d/%Y %H:%M:%S"))
            print(end_of_interval.strftime("%m/%d/%Y %H:%M:%S"))
            logging.info(f"Processing interval from {start_of_day.strftime('%m/%d/%Y %H:%M:%S')} to {end_of_interval.strftime('%m/%d/%Y %H:%M:%S')}")
            print(f"Processing interval from {start_of_day.strftime('%m/%d/%Y %H:%M:%S')} to {end_of_interval.strftime('%m/%d/%Y %H:%M:%S')}")
            for coll in collections:
                extracted_keywords = query_and_extract_keywords(es_db , start_of_day.strftime("%m/%d/%Y %H:%M:%S"), end_of_interval.strftime("%m/%d/%Y %H:%M:%S") , coll )
                current_day_str = start_of_day.strftime("%m/%d/%Y")
                calculate_top_keywords_with_topic_2_es(es_db , current_day_str, extracted_keywords, historical_data_index, coll)
                calculate_top_keywords_with_trend_logic_topic(current_day_str, es_db, historical_data_index,coll )
        current_day = datetime.now()
        time.sleep(2*3600)


def main_loop():
    # global restart_needed  # Sử dụng biến toàn cục
    
    logging.info("Main loop started")
    while True:
        try:
            #run_keyword_all_day()
            logging.info("Completed run_keyword_all_day, starting run_keyword_today")
            run_keyword_today()
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
        logging.info("Restarting main loop")
        # restart_needed = False 
if __name__ == "__main__":
    # with open("/usr/app/src/run.log", "a") as f:
    #     f.write("run.py started\n")
    logging.info("Program started")

    index_name_trend_fn = "key-trend-current"

    template_body_trend = {
    "index_patterns": ["key-trend-*"],
    "priority": 200,
    "template": {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 1,
            "index.mapping.total_fields.limit": 10000
        },
        "mappings": {
            "dynamic": "false",
            "properties": {
                "date": {
                    "type": "date",
                    "format": "MM_dd_yyyy||MM/dd/yyyy||strict_date||epoch_millis"
                },
                "type":      {"type": "keyword"},
                "tenant_id": {"type": "keyword"},
                "topic_id":  {"type": "keyword"},
                "keywords_trend": {
                    "type": "nested",
                    "properties": {
                        "keyword":    {"type": "keyword"},
                        "percentage": {"type": "float"},
                        "record":     {"type": "integer"},
                        "score":      {"type": "float"},
                        "isTrend":    {"type": "boolean"}
                    }
                }
            }
        },
        "aliases": {
            "key-trend-current": {}
        }
      }
      }
    if not es_db.indices.exists_index_template(name="key-trend-template"):
        es_db.indices.put_index_template(name="key-trend-template", body=template_body_trend)
        print("✅ Created index template: key-trend-template")
    else:
        print("ℹ️ Template already exists, skipping.")

    # Kiểm tra nếu index đã tồn tại, nếu chưa thì tạo mới
    index_name_trend = "key-trend-000001"
    if not es_db.indices.exists(index=index_name_trend):
        es_db.indices.create(index=index_name_trend, body={
        "aliases": {
            "key-trend-current": {"is_write_index": True}
        }
    })
        print(f"✅ Created index: {index_name_trend}")
    else:
        print(f"ℹ️ Index {index_name_trend} already exists.")

    #if not es_db.indices.exists(index=index_name_trend):
    #    es_db.indices.create(index=index_name_trend, body=mapping_trend)
    #    print(f"Index {index_name_trend} created successfully")
    #else:
    #    print(f"Index {index_name_trend} already exists")
    sleep(1)
    index_name_fn = "key-osint1-current"    
    
    # Định nghĩa mappings và settings cho chỉ mục
    template_body = {
    "index_patterns": ["key-osint1-*"],
    "priority": 200,
    "template": {
        "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 1,
        "index.mapping.total_fields.limit": 10000
        },
        "mappings": {
        "dynamic": "false",
        "properties": {
            "date": {
            "type": "date",
            "format": "MM_dd_yyyy||MM/dd/yyyy||strict_date||epoch_millis"
            },
            "type": { "type": "keyword" },
            "tenant_id": { "type": "keyword" },
            "topic_id": { "type": "keyword" },

            "keywords_top": {
            "type": "nested",
            "properties": {
                "keyword": { "type": "keyword" },
                "percentage": { "type": "float" },
                "record": { "type": "integer" }
            }
            },
            "hashtags_top": {
            "type": "nested",
            "properties": {
                "hashtag": { "type": "keyword" },
                "percentage": { "type": "float" },
                "record": { "type": "integer" }
            }
            }
        }
        },
        "aliases": {
        "key-osint1-current": {}
        }
    }
      }
    if not es_db.indices.exists_index_template(name="key-osint1-template"):
        es_db.indices.put_index_template(name="key-osint1-template", body=template_body)
        print("✅ Created index template: key-osint1-template")
    else:
        print("ℹ️ Template already exists, skipping.")

    # Kiểm tra nếu index đã tồn tại, nếu chưa thì tạo mới
    index_name = "key-osint1-000001"
    if not es_db.indices.exists(index=index_name):
        es_db.indices.create(index=index_name, body={
        "aliases": {
            "key-osint1-current": {"is_write_index": True}
        }
    })
        print(f"✅ Created index: {index_name}")
    else:
        print(f"ℹ️ Index {index_name} already exists.")
    main_loop()

