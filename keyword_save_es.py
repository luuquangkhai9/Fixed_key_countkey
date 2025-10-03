from elasticsearch import Elasticsearch , helpers
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout
import time
from datetime import datetime, timedelta
from elasticsearch.exceptions import NotFoundError
from typing import List, Dict
from collections import defaultdict
import logging
import json
from fastapi import FastAPI , HTTPException
import uvicorn
from time import sleep
from dotenv import load_dotenv
import os
load_dotenv()

app = FastAPI()
elasticsearch_url = os.getenv("ELASTICSEARCH_URL")
elasticsearch_db_url = os.getenv("ELASTICSEARCH_DB_URL")
es = Elasticsearch([elasticsearch_url], request_timeout=100)

es_db = Elasticsearch([elasticsearch_db_url], request_timeout=100)

def fetch_all_records(index_name,  es):
    # Khởi tạo scroll
    result = es.search(
        index=index_name,
        scroll='2m',  # Giữ scroll mở trong 2 phút
        size=100,     # Số lượng records trên mỗi trang
        body={
            "query": {"match_all": {}}
        }
    )

    # Lấy scroll ID
    scroll_id = result['_scroll_id']
    scroll_size = len(result['hits']['hits'])

    # Tạo một list chứa tất cả records
    records = []

    # Lặp qua scroll để lấy tất cả records
    while scroll_size > 0:
        records.extend(result['hits']['hits'])
        result = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = result['_scroll_id']
        scroll_size = len(result['hits']['hits'])

    return  [record['_source'] for record in records]
def print_json(index_name,  es):
    data = fetch_all_records(index_name, es)
    with open('nhap_2.json', 'w' , encoding='utf-8') as json_file:
        json.dump(data, json_file,  ensure_ascii=False , indent=4)

    
def load_data_to_elasticsearch_new_data(data, index_name , es):
    # Đọc dữ liệu từ file JSON

    # Tạo index nếu chưa tồn tại
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)

    actions = [
        {
            "_index": index_name,
            "_id": f"{record['date'].replace('/', '_')}",  # Tạo ID dựa trên index và ngày
            "_source": record
        }
        for  record in data  # Lặp qua list, không phải dictionary
    ]

    # Sử dụng helpers.bulk để đẩy dữ liệu
    helpers.bulk(es, actions)

def delete_first_data(last_id, index_name , es):
    try:
        response = es.delete(index=index_name, id=last_id)
        sleep(1.5)
        print("Document deleted successfully", response)
    except Exception as e:
        print("Error in deleting document", e)
def load_data_to_elasticsearch_keyword(historical_data_file, index_name):
    # Đọc dữ liệu từ file JSON
    with open(historical_data_file, 'r', encoding='utf-8') as file:
        historical_data = json.load(file)

    # Tạo index nếu chưa tồn tại
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)

    actions = [
        {
            "_index": index_name,
            "_id": f"{record['date'].replace('/', '_')}",  # Tạo ID dựa trên index và ngày
            "_source": record
        }
        for idx, record in enumerate(historical_data)  # Lặp qua list, không phải dictionary
    ]

    # Sử dụng helpers.bulk để đẩy dữ liệu
    helpers.bulk(es, actions)
def load_data_to_elasticsearch(historical_data_file, index_name, es):
    # Đọc dữ liệu từ file JSON
    with open(historical_data_file, 'r', encoding='utf-8') as file:
        historical_data = json.load(file)
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
    # Chuẩn bị dữ liệu cho việc tải lên Elasticsearch
    actions = [
        {
            "_index": index_name,
            "_id": record_id,
            "_source": record_content
        }
        for record_id, record_content in historical_data.items()
    ]

    # Sử dụng helpers.bulk để tải dữ liệu lên Elasticsearch
    helpers.bulk(es, actions)

def load_data_extract_keyword(historical_data_file, index_name, es):
    # Đọc dữ liệu từ file JSON
    with open(historical_data_file, 'r', encoding='utf-8') as file:
        historical_data = json.load(file)

    # Tạo index nếu chưa tồn tại
    if not es.indices.exists(index=index_name):
        # Định nghĩa cấu trúc của index với định dạng ngày giờ cho trường created_time
        es.indices.create(index=index_name, body={
            "mappings": {
                "properties": {
                    "created_time": {
                        "type": "date",
                        "format": "MM/dd/yyyy HH:mm:ss"  # Định dạng ngày tháng năm giờ phút giây
                    }
                    # Các định nghĩa mapping khác cho các trường còn lại nếu cần
                }
            }
        })

    # Chuẩn bị dữ liệu cho việc tải lên Elasticsearch
    actions = [
        {
            "_index": index_name,
            "_id": record_id,
            "_source": record_content
        }
        for record_id, record_content in historical_data.items()
    ]

    # Sử dụng helpers.bulk để tải dữ liệu lên Elasticsearch
    helpers.bulk(es, actions)


    # Lưu các bản ghi vào Elasticsearch
    # for record in historical_data:
    #     # Lấy giá trị date và chuyển dấu "/" thành dấu "-"
    #     # record_date = record["date"].replace("/", "-")
        
    #     # Sử dụng giá trị date đã chuyển đổi làm id cho bản ghi
    #     es.index(index=index_name, id=record["date"], body=record)
    # for record_id, record_content in historical_data.items():
    #     # Sử dụng ID của bài viết làm id cho bản ghi
    #     es.index(index=index_name, id=record_id, body=record_content)

        
def get_historical_data_from_es(index_name, es):
    if es.indices.exists(index=index_name):
        result = es.search(index=index_name, body={"query": {"match_all": {}}, "size": 100})
        hits = result['hits']['hits']
        return [hit['_source'] for hit in hits]
    else:
        return []
def update_records_bulk(index_name, records):
    # Chuẩn bị dữ liệu cho Bulk API
    actions = []
    for hit in records:
        record = hit['_source']
        record_id = hit['_id']
        if '/' in record['date']:
            new_date = record['date'].replace("/", "_")
            action = {
                "_op_type": "update",
                "_index": index_name,
                "_id": record_id,
                "doc": {"date": new_date}
            }
            actions.append(action)

    # Thực hiện cập nhật bằng Bulk API
    if actions:
        response = helpers.bulk(es, actions=actions)
        return response
    else:
        return "No records to update"
def upsert_records_bulk(index_name, records):
    actions = []
    
    for record in records:
        # Định danh document dựa trên ngày trong record, thay thế "/" bằng "-"
        doc_id = record['date']
        
        # Chuẩn bị action cho mỗi record
        action = {
            "_op_type": "update",
            "_index": index_name,
            "_id": doc_id,
            "doc": record,
            "doc_as_upsert": True  # Đảm bảo rằng nếu document không tồn tại, nó sẽ được thêm mới
        }
        actions.append(action)
    
    # Thực hiện bulk operation
    responses = helpers.bulk(es, actions)
    return responses

def update_historical_data_to_es(data, index_name, es, max_retries=3, timeout=1000):
    es.delete_by_query(index=index_name, body={"query": {"match_all": {}}})
    sleep(1.5)
    actions = [
        {
            "_op_type": "index",
            "_index": index_name,
            "_id": f"{record['date'].replace('/', '_')}",  # Sử dụng ngày làm ID
            "_source": record
        }
        for record in data
    ]

    for attempt in range(max_retries):
        try:
            helpers.bulk(es, actions, request_timeout=timeout)
            sleep(1)
            break
        except Exception as e:
            if attempt + 1 == max_retries:
                raise e
            time.sleep(2 ** attempt)
# Gọi hàm để lưu dữ liệu vào Elasticsearch
def get_data_from_elasticsearch(index_name, output_file):
    # Kết nối tới Elasticsearch

    # Truy vấn tất cả các tài liệu trong index
    result = es.search(index=index_name, body={"query": {"match_all": {}}, "size": 100})

    # Lấy dữ liệu từ kết quả trả về
    hits = result['hits']['hits']
    data = [hit['_source'] for hit in hits]
    # Ghi dữ liệu ra tệp JSON
    with open(output_file, 'w' , encoding='utf-8') as json_file:
        json.dump(data, json_file,  ensure_ascii=False , indent=4)

# Sử dụng hàm để lấy dữ liệu từ Elasticsearch và ghi ra một tệp JSON
# get_data_from_elasticsearch("historical_data_index", "data_from_elasticsearch.json")
def delete_latest_record_and_update_es(index_name, es):
    # Lấy tất cả bản ghi
    records = fetch_all_records(index_name, es)

    # Kiểm tra nếu không có bản ghi nào thì không làm gì cả
    if not records:
        return "No records to delete."

    # Loại bỏ bản ghi đầu tiên (mới nhất)
    latest_record_date = records[0]['date']  # Lưu ngày của bản ghi mới nhất để thông báo
    del records[0]

    # Cập nhật lại dữ liệu vào Elasticsearch
    update_historical_data_to_es(records, index_name, es)

    return f"Deleted latest record and updated Elasticsearch. Removed record date: {latest_record_date}"
def delete_latest_record(index_name):
    # Truy vấn để tìm ngày mới nhất
    response = es.search(
        index=index_name,
        body={
            "size": 1,
            "sort": [{"date": {"order": "desc"}}],
            "_source": ["date"]
        }
    )
    
    
    if response["hits"]["hits"]:
        # Lấy ngày từ bản ghi mới nhất
        latest_date = response["hits"]["hits"][0]["_source"]["date"]
        # ID đã được cập nhật để sử dụng "-" thay vì "/", không cần thay đổi
        record_id = latest_date
        
        # Xóa bản ghi có ID tương ứng
        es.delete(index=index_name, id=record_id)
        print(f"Deleted record with ID (date): {record_id}")
    else:
        print("No records found to delete.")

def calculate_top_keywords(data: List[Dict]) -> Dict[str, int]:
    keyword_frequency = defaultdict(int)
    for record in data:
        keywords = record.get('keywords_top', [])
        for keyword in keywords[:10]:  # Lấy top 10 từ mỗi bản ghi
            keyword_frequency[keyword['keyword']] += 1
    # Sắp xếp và trả về
    return dict(sorted(keyword_frequency.items(), key=lambda x: x[1], reverse=True))
def load_data_to_elasticsearch_kw_a(es , data, index_name):
    try:
        # Tạo ID dựa trên ngày và nền tảng
        document_id = f"{data['date']}_{data['type']}"

        # Sử dụng index với op_type="index" để cập nhật hoặc thêm mới document
        es.index(
            index=index_name,
            id=document_id,
            body=data,
            op_type='index'
        )
        logging.info(f"Data loaded to Elasticsearch index {index_name}")
    except Exception as e:
        logging.error(f"Error loading data to Elasticsearch: {e}")
def load_data_to_elasticsearch_kw_a(es , data, index_name):
    try:
        # Tạo ID dựa trên ngày và nền tảng
        document_id = f"{data['date']}_{data['type']}"

        # Sử dụng index với op_type="index" 
        #op_type="index" (mặc định):

        #Nếu document chưa tồn tại → sẽ được tạo mới.

        #Nếu document đã tồn tại (cùng _id) → sẽ ghi đè (overwrite) toàn bộ document, chứ không phải update từng field.

        #op_type="create":

        #Chỉ cho phép tạo mới. Nếu _id đã tồn tại → sẽ báo lỗi version_conflict_engine_exception.

        #testdebug

        es.index(
            index=index_name,
            id=document_id,
            body=data,
            op_type='index'
        )
        print("update thanh cong tu khoa ", document_id)
        logging.info(f"Data loaded to Elasticsearch index {index_name}")
    except Exception as e:
        logging.error(f"Error loading data to Elasticsearch: {e}")



def chunks(lst, n):
    """Chia list thành các batch nhỏ với kích thước n"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def bulk_data_to_elasticsearch_kw_a(es , data, index_name):
    try:
        for batch in chunks(data, 1000):   # mỗi lần lấy 1000 phần tử
            actions = [
                {
                    "_index": index_name,   # alias hoặc index đích
                    "_id": str(doc["id"]),           # nếu muốn set id, còn không thì bỏ đi
                    "_source": doc
                }
                for doc in batch
            ]
            helpers.bulk(es, actions)
        print("update thanh cong tu khoa ")
        logging.info(f"Data loaded to Elasticsearch index {index_name}")
    except Exception as e:
        logging.error(f"Error loading data to Elasticsearch: {e}")

def get_latest_date_from_elasticsearch(es, index_name):
    query = {
        "size": 1,
        "sort": [
            {
                "date": {
                    "order": "desc",
                    "format": "MM_dd_yyyy"
                }
            }
        ],
        "query": {
            "match_all": {}
        }
    }

    # Execute the query
    response = es.search(index=index_name, body=query)

    # Get the latest date from the response
    if response['hits']['hits']:
        latest_document = response['hits']['hits'][0]
        last_day_str = latest_document['_source']['date']

        # Convert the date format from 'MM_dd_yyyy' to '%m/%d/%Y'
        date_obj = datetime.strptime(last_day_str, '%m_%d_%Y')
        formatted_date = date_obj.strftime('%m/%d/%Y')
        
        return formatted_date
    else:
        return None

if __name__ == "__main__":  
    # with open("data_from_elasticsearch.json.", 'r', encoding='utf-8') as file:
    #     historical_data = json.load(file)

    # es.delete_by_query(index="keyword_a", body={"query": {"match_all": {}}})
    index_name = "keyword_a_trend_test"

    mapping = {
        "mappings": {
            "properties": {
                "date": {
                    "type": "date",
                    "format": "MM_dd_yyyy"  # Định dạng ngày như bạn yêu cầu
                },
                "type": {
                    "type": "keyword"
                },
                "keywords_top": {
                    "type": "nested",  # Để lưu trữ danh sách các từ khóa
                    "properties": {
                        "keyword": {
                            "type": "keyword"
                        },
                        "percentage": {
                            "type": "float"
                        },
                        "record": {
                            "type": "integer"
                        }
                    }
                }
            }
        }
    }

    # Kiểm tra nếu index đã tồn tại, nếu có thì xóa nó đi
    if not es_db.indices.exists(index=index_name):
        es_db.indices.create(index=index_name, body=mapping)
    else:
        print(f"Index {index_name} already exists")

    # sleep(3)
    # new_data =   {
    #     "date": "01/29/2024",
    #     "keywords_top": [
    #         {"keyword": "mới", "percentage": 1.5},
    #         {"keyword": "ví_dụ", "percentage": 1.2}
    #     ],
    #     "keywords": [
    #         {"keyword": "mới", "percentage": 1.5},
    #         {"keyword": "ví_dụ", "percentage": 1.2}
    #     ]
    # }
    # load_data_extract_keyword("keyword_test_27.1_filter_new.json" , 'hastag_data' , es)
    # print_json("top_kw_test_taile_2" , es)
    # delete_first_data("04_11_2024", "top_kw_test_taile" , es)
    # load_data_to_elasticsearch('keyword_test_27.1_filter_new_2.json' , 'keyword_extract_data_leetai' , es)    # load_data_to_elasticsearch_keyword('nhap.json', "top_kw_test_taile_2")
    # # sleep(0.5)
    # update_or_add_records(es, "top_kw_test_taile", historical_data)    # sleep(1.5)
    # all_records = fetch_all_records("top_kw_test_taile")
    # # upsert_records_bulk("top_kw_test_taile" , historical_data)
    # get_data_from_elasticsearch("top_kw_test_taile"  ,"data_from_elasticsearch.json")
    # delete_data_from_es("top_kw   _tilte_taile")
    # delete_latest_record_and_update_es("top_kw_test_taile_2" , es)
    # load_data_to_elasticsearch_keyword('keyword_percentages_main_title.json' , "top_kw_pd_title")
    # load_data_extract_keyword('keyword_test_27.1_filter_new_2.json' , 'keyword_extract_phandong_leetai' , es)    