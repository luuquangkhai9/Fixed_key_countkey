# run_keyword_extraction.py
"""File này sẽ thay thế cho logic xử lý dữ liệu thô trong các file run.py và keyword_save_es.py cũ. 
Nhiệm vụ duy nhất của nó là đọc dữ liệu bài báo mới, dùng py_vncorenlp để trích xuất từ khóa và 
lưu vào Elasticsearch với cấu trúc chuẩn. Đây là một bước tiền xử lý, cần được chạy mỗi khi có dữ liệu mới."""


import json
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
import py_vncorenlp

# --- CẤU HÌNH ---
ES_HOST = "localhost"
ES_PORT = 9200
INDEX_NAME = "newspaper_articles_with_keywords" # Index để lưu kết quả
VNCORENLP_MODEL_PATH = "C:/path/to/your/vncorenlp" # Đường dẫn đến model VnCoreNLP của bạn

# --- KẾT NỐI ---
es_client = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT, 'scheme': 'http'}])
rdrsegmenter = py_vncorenlp.VnCoreNLP(annotators=["wseg", "pos", "ner"], save_dir=VNCORENLP_MODEL_PATH)

def create_index_if_not_exists(client, index):
    """Tạo index với mapping phù hợp nếu chưa tồn tại."""
    mapping = {
        "properties": {
            "title": {"type": "text"},
            "content": {"type": "text"},
            "author": {"type": "keyword"},
            "created_time": {"type": "date"},
            "keywords": {"type": "keyword"} # Quan trọng: để 'keyword' để aggregate
        }
    }
    if not client.indices.exists(index=index):
        client.indices.create(index=index, mappings=mapping)
        print(f"✅ Index '{index}' đã được tạo.")

def extract_keywords(text):
    """
    Sử dụng VnCoreNLP để trích xuất các cụm danh từ (Noun Phrases)
    và thực thể có tên (Named Entities) làm từ khóa.
    """
    if not text or not isinstance(text, str):
        return []

    annotations = rdrsegmenter.annotate_text(text)
    keywords = set() # Dùng set để tránh trùng lặp

    for sentence in annotations:
        for word in sentence:
            # Lấy các cụm danh từ, thực thể tên riêng
            # Ví dụ: Np (Proper Noun), Nc (Common Noun), B-PER (Person), B-LOC (Location)...
            if word['posTag'].startswith('N') or word['nerLabel'].startswith('B-'):
                keywords.add(word['wordForm'].replace(" ", "_"))
    return list(keywords)

def process_and_index_data(input_file_path):
    """
    Đọc dữ liệu từ file JSON, trích xuất từ khóa và đẩy vào Elasticsearch.
    """
    actions = []
    with open(input_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                article = json.loads(line)
                # Giả sử bài báo có các trường 'title', 'content', 'created_time'
                text_to_analyze = article.get('title', '') + " " + article.get('content', '')
                keywords = extract_keywords(text_to_analyze)

                # Chuẩn bị document để index
                document = {
                    "_index": INDEX_NAME,
                    "_source": {
                        "title": article.get('title'),
                        "content": article.get('content'),
                        "author": article.get('author'),
                        # Chuyển đổi định dạng ngày tháng nếu cần
                        "created_time": datetime.strptime(article.get('created_time'), "%d/%m/%Y %H:%M").isoformat(),
                        "keywords": keywords
                    }
                }
                actions.append(document)

                # Đẩy dữ liệu theo batch để tối ưu hiệu suất
                if len(actions) >= 500:
                    helpers.bulk(es_client, actions)
                    print(f"Đã index {len(actions)} bản ghi...")
                    actions = []

            except (json.JSONDecodeError, KeyError) as e:
                print(f"⚠️ Bỏ qua dòng lỗi: {line.strip()} - Lỗi: {e}")
                continue

    # Index nốt phần dữ liệu còn lại
    if actions:
        helpers.bulk(es_client, actions)
        print(f"Đã index {len(actions)} bản ghi cuối cùng.")

    print("🎉 Hoàn tất quá trình xử lý và index dữ liệu.")


if __name__ == "__main__":
    # 1. Đảm bảo index đã tồn tại
    create_index_if_not_exists(es_client, INDEX_NAME)

    # 2. Cung cấp đường dẫn đến file dữ liệu thô của bạn
    INPUT_DATA_FILE = "path/to/your/articles.json"
    process_and_index_data(INPUT_DATA_FILE)