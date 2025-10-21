from elasticsearch import Elasticsearch
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import urllib3
import time
# es = Elasticsearch(["http://172.168.200.202:9200"] , request_timeout=100)
import os
load_dotenv()

# Lấy URL Elasticsearch từ biến môi trường
elasticsearch_url = os.getenv("ELASTICSEARCH_URL")
# Khởi tạo Elasticsearch client
es = Elasticsearch([elasticsearch_url], request_timeout=100)
logger = logging.getLogger("run")  # Sử dụng logger đã được cấu hình
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

def query_keyword_with_topic(es, start_date_str, end_date_str, type):
    try:
        logger.info(f"Starting query_keyword_with_topic for type: {type}, from {start_date_str} to {end_date_str}")

        # Hàm chung để truy vấn Elasticsearch với search_after
        def fetch_records(index, query_body):
            records = []
            search_after_value = None  # Lưu giá trị search_after

            try:
                while True:
                    # Nếu đã có search_after_value, thêm vào body
                    if search_after_value:
                        query_body["search_after"] = search_after_value

                    # Thực hiện truy vấn
                    result = es.options(request_timeout=2000).search(index=index, body=query_body)

                    hits = result['hits']['hits']
                    if not hits:
                        logger.info(f"No more records found in index: {index}")

                        break  # Kết thúc khi không còn dữ liệu

                    for hit in hits:
                        # Xử lý dữ liệu cho từng chỉ mục
                        if index == 'posts':
                            if 'topic_id' not in hit['_source']:
                                hit['_source']['topic_id'] = []
                            # if 'hashtag' not in hit['_source']:
                            #     hit['_source']['hashtag'] = []

                            keywords = hit['_source'].get('keyword', [])
                            keywords = keywords if isinstance(keywords, list) else []

                            key_words = hit['_source'].get('key_word', [])
                            key_words = key_words if isinstance(key_words, list) else []

                            # Lấy danh sách từ theo điều kiện từ trường key_word_type
                            key_word_type = hit['_source'].get('key_word_type', [])
                            filtered_words = []

                            if isinstance(key_word_type, list) and key_word_type:
                                for item in key_word_type:
                                    if isinstance(item, list) and len(item) == 2:  # Đảm bảo item là danh sách có 2 phần tử
                                        w, pos = item
                                        # if (pos in ['Np', 'Ny', 'Nb', 'Vb'] or (pos in ['N'] and '_' in w and not w.startswith('_'))):
                                        if (pos in ['Np', 'Ny', 'Nb', 'Vb'] or (pos in ['N'] and '_' in w and not w.startswith('_'))):

                                            filtered_words.append(w.lower())
                                # Nếu key_word_type không rỗng, chỉ lấy các từ từ đây
                                combined_keywords = list(set(filtered_words))
                            else:
                                # Nếu key_word_type rỗng, kết hợp keywords và key_word
                                combined_keywords = list(set(keywords + key_words))

                            # Cập nhật lại keyword trong hit['_source']
                            hit['_source']['keyword'] = combined_keywords

                            # Xóa các trường không cần thiết
                            if 'key_word' in hit['_source']:
                                del hit['_source']['key_word']
                            if 'key_word_type' in hit['_source']:
                                del hit['_source']['key_word_type']

                            

                            hashtag = hit['_source'].get('hashtag', [])
                            hashtag = hashtag if isinstance(hashtag, list) else []

                            hit['_source']['hashtag'] = hashtag

                        elif index == 'comments':
                            if 'topic_id' not in hit['_source']:
                                hit['_source']['topic_id'] = []

                            keywords = hit['_source'].get('keywords', [])
                            keywords = keywords if isinstance(keywords, list) else []

                            key_words = hit['_source'].get('key_word', [])
                            key_words = key_words if isinstance(key_words, list) else []
                            
                            key_word_type = hit['_source'].get('key_word_type', [])
                            filtered_words = []

                            if isinstance(key_word_type, list) and key_word_type:
                                for item in key_word_type:
                                    if isinstance(item, list) and len(item) == 2:  # Đảm bảo item là danh sách có 2 phần tử
                                        w, pos = item
                                        if (pos in ['Np', 'Ny', 'Nb', 'Vb'] or (pos in ['N'] and '_' in w and not w.startswith('_'))):
                                            filtered_words.append(w.lower())
                                # Nếu key_word_type không rỗng, chỉ lấy các từ từ đây
                                combined_keywords = list(set(filtered_words))
                            else:
                                combined_keywords = list(set(keywords + key_words))
                            hit['_source']['keywords'] = combined_keywords

                            hashtags = hit['_source'].get('hashtags', [])
                            hashtags = hashtags if isinstance(hashtags, list) else []

                            hashtag = hit['_source'].get('hashtag', [])
                            hashtag = hashtag if isinstance(hashtag, list) else []

                            combined_hashtag = list(set(hashtags + hashtag))
                            hit['_source']['hashtags'] = combined_hashtag
                            if 'key_word' in hit['_source']:
                                del hit['_source']['key_word']
                            if 'key_word_type' in hit['_source']:
                                del hit['_source']['key_word_type']

                            if 'hashtag' in hit['_source']:
                                del hit['_source']['hashtag']

                        records.append(hit)

                    # Lấy giá trị sort cuối cùng từ kết quả để sử dụng cho search_after
                    search_after_value = hits[-1]['sort']
                    # logger.info(f"Fetched {len(hits)} records from index: {index}, continuing...")

            except Exception as e:
                logger.error(f"Error fetching records from {index}: {e}")

            return records  # Đảm bảo trả về danh sách (có thể rỗng)

        # Định nghĩa truy vấn Elasticsearch cho cả hai chỉ mục
        body_posts = {
            "query": {
                "bool": {
                    "must": [
                        {"match_phrase": {"type": type}},
                        {"range": {"created_time": {"gte": start_date_str, "lte": end_date_str}}}
                    ]
                }
            },
            "sort": [{"created_time": {"order": "asc"}}, {"time_crawl.keyword": {"order": "asc"}}],
            "_source": ["keyword", "hashtag", "created_time", "topic_id", "key_word", "key_word_type","tenancy_ids"],
            "size": 4000
        }

        body_comments = {
            "query": {
                "bool": {
                    "must": [
                        {"match_phrase": {"type": type}},
                        {"range": {"created_time": {"gte": start_date_str, "lte": end_date_str}}}
                    ]
                }
            },
            "sort": [{"created_time": {"order": "asc"}}, {"time_crawl.keyword": {"order": "asc"}}],
            "_source": ["keywords", "hashtags", "created_time", "topic_id", "key_word", "hashtag", "key_word_type","tenancy_ids"],
            "size": 4000
        }

        # Lấy dữ liệu từ cả hai chỉ mục
        records_posts = fetch_records('posts', body_posts)
        logger.info(f"Fetched {len(records_posts)} records from posts index.")

        records_comments = fetch_records('comments', body_comments)
        logger.info(f"Fetched {len(records_comments)} records from comments index.")

        # Gộp dữ liệu từ cả hai chỉ mục
        records = records_posts + records_comments
        logger.info(f"Total records fetched: {len(records)}")

        # Lưu dữ liệu vào file JSON
        # with open(query_data_file, 'w', encoding='utf-8') as f:
        #     json.dump(records, f, ensure_ascii=False, indent=4)

        return records

    except Exception as e:
        logger.error(f"An error occurred during query_keyword_with_topic: {e}")

        return []
    

def query_keyword_with_trend(es,INDEX, body):
    try:


        # Hàm chung để truy vấn Elasticsearch với search_after
        def fetch_records(index, query_body):
            records = []
            search_after_value = None  # Lưu giá trị search_after

            try:
                while True:
                    # Nếu đã có search_after_value, thêm vào body
                    if search_after_value:
                        query_body["search_after"] = search_after_value

                    # Thực hiện truy vấn
                    result = es.options(request_timeout=2000).search(index=index, body=query_body)

                    hits = result['hits']['hits']
                    if not hits:
                        logger.info(f"No more records found in index: {index}")

                        break  # Kết thúc khi không còn dữ liệu

                    for hit in hits:
                        # Xử lý dữ liệu cho từng chỉ mục
                        records.append(hit["_source"])

                    # Lấy giá trị sort cuối cùng từ kết quả để sử dụng cho search_after
                    search_after_value = hits[-1]["sort"]
                    # logger.info(f"Fetched {len(hits)} records from index: {index}, continuing...")

            except Exception as e:
                logger.error(f"Error fetching records from {index}: {e}")

            return records  # Đảm bảo trả về danh sách (có thể rỗng)

        # Định nghĩa truy vấn Elasticsearch cho cả hai chỉ mục


        # Lấy dữ liệu từ cả hai chỉ mục


        records = fetch_records(INDEX, body)
        logger.info(f"Total records fetched: {len(records)}")

        # Lưu dữ liệu vào file JSON
        # with open(query_data_file, 'w', encoding='utf-8') as f:
        #     json.dump(records, f, ensure_ascii=False, indent=4)

        return records
    except Exception as e:
        logger.error(f"An error occurred during query_keyword_with_topic: {e}")

        return []

# def query_keyword_with_topic(es, start_date_str, end_date_str, type):
#     try:
#         logger.info(f"Starting query_keyword_with_topic for type: {type}, from {start_date_str} to {end_date_str}")

#         # Hàm chung để truy vấn Elasticsearch với search_after
#         def fetch_records(index, query_body):
#             records = []
#             search_after_value = None  # Lưu giá trị search_after

#             try:
#                 while True:
#                     # Nếu đã có search_after_value, thêm vào body
#                     if search_after_value:
#                         query_body["search_after"] = search_after_value

#                     # Thực hiện truy vấn
#                     result = es.options(request_timeout=2000).search(index=index, body=query_body)

#                     hits = result['hits']['hits']
#                     if not hits:
#                         logger.info(f"No more records found in index: {index}")

#                         break  # Kết thúc khi không còn dữ liệu

#                     for hit in hits:
#                         # Xử lý dữ liệu cho từng chỉ mục
#                         if index == 'posts':
#                             if 'topic_id' not in hit['_source']:
#                                 hit['_source']['topic_id'] = []
#                             # if 'hashtag' not in hit['_source']:
#                             #     hit['_source']['hashtag'] = []

#                             keywords = hit['_source'].get('keyword', [])
#                             keywords = keywords if isinstance(keywords, list) else []

#                             key_words = hit['_source'].get('key_word', [])
#                             key_words = key_words if isinstance(key_words, list) else []
                            
#                             combined_keywords = list(set(keywords + key_words))
#                             hit['_source']['keyword'] = combined_keywords
#                             if 'key_word' in hit['_source']:
#                                 del hit['_source']['key_word']

                            

#                             hashtag = hit['_source'].get('hashtag', [])
#                             hashtag = hashtag if isinstance(hashtag, list) else []

#                             hit['_source']['hashtag'] = hashtag

#                         elif index == 'comments':
#                             if 'topic_id' not in hit['_source']:
#                                 hit['_source']['topic_id'] = []

#                             keywords = hit['_source'].get('keywords', [])
#                             keywords = keywords if isinstance(keywords, list) else []

#                             key_words = hit['_source'].get('key_word', [])
#                             key_words = key_words if isinstance(key_words, list) else []

#                             combined_keywords = list(set(keywords + key_words))
#                             hit['_source']['keywords'] = combined_keywords

#                             hashtags = hit['_source'].get('hashtags', [])
#                             hashtags = hashtags if isinstance(hashtags, list) else []

#                             hashtag = hit['_source'].get('hashtag', [])
#                             hashtag = hashtag if isinstance(hashtag, list) else []

#                             combined_hashtag = list(set(hashtags + hashtag))
#                             hit['_source']['hashtags'] = combined_hashtag
#                             if 'key_word' in hit['_source']:
#                                 del hit['_source']['key_word']
#                             if 'hashtag' in hit['_source']:
#                                 del hit['_source']['hashtag']

#                         records.append(hit)

#                     # Lấy giá trị sort cuối cùng từ kết quả để sử dụng cho search_after
#                     search_after_value = hits[-1]['sort']
#                     # logger.info(f"Fetched {len(hits)} records from index: {index}, continuing...")

#             except Exception as e:
#                 logger.error(f"Error fetching records from {index}: {e}")

#             return records  # Đảm bảo trả về danh sách (có thể rỗng)

#         # Định nghĩa truy vấn Elasticsearch cho cả hai chỉ mục
#         body_posts = {
#             "query": {
#                 "bool": {
#                     "must": [
#                         {"match_phrase": {"type": type}},
#                         {"range": {"created_time": {"gte": start_date_str, "lte": end_date_str}}}
#                     ]
#                 }
#             },
#             "sort": [{"created_time": {"order": "asc"}}, {"time_crawl.keyword": {"order": "asc"}}],
#             "_source": ["keyword", "hashtag", "created_time", "topic_id", "key_word", "time_crawl"],
#             "size": 4000
#         }

#         body_comments = {
#             "query": {
#                 "bool": {
#                     "must": [
#                         {"match_phrase": {"type": type}},
#                         {"range": {"created_time": {"gte": start_date_str, "lte": end_date_str}}}
#                     ]
#                 }
#             },
#             "sort": [{"created_time": {"order": "asc"}}, {"time_crawl.keyword": {"order": "asc"}}],
#             "_source": ["keywords", "hashtags", "created_time", "topic_id", "key_word", "hashtag"],
#             "size": 4000
#         }

#         # Lấy dữ liệu từ cả hai chỉ mục
#         records_posts = fetch_records('posts', body_posts)
#         logger.info(f"Fetched {len(records_posts)} records from posts index.")

#         records_comments = fetch_records('comments', body_comments)
#         logger.info(f"Fetched {len(records_comments)} records from comments index.")

#         # Gộp dữ liệu từ cả hai chỉ mục
#         records = records_posts + records_comments
#         logger.info(f"Total records fetched: {len(records)}")

#         # Lưu dữ liệu vào file JSON
#         # with open(query_data_file, 'w', encoding='utf-8') as f:
#         #     json.dump(records, f, ensure_ascii=False, indent=4)

#         return records

#     except Exception as e:
#         logger.error(f"An error occurred during query_keyword_with_topic: {e}")

#         return []

