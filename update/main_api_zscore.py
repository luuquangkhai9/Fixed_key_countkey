# main_api_zscore.py

"""File này sẽ thay thế api_keyword.py cũ. Nó chứa endpoint /trending được xây dựng bằng FastAPI, 
chịu trách nhiệm tính toán và trả về các từ khóa xu hướng theo thời gian thực."""

from fastapi import FastAPI, HTTPException, Query
from elasticsearch import Elasticsearch, NotFoundError
from datetime import datetime, timedelta
import math

# --- CẤU HÌNH ---
# Thay thế bằng cấu hình thực tế của bạn
ES_HOST = "localhost"
ES_PORT = 9200
# Đây là index chứa dữ liệu bài báo đã được bóc tách từ khóa
# Ví dụ: {'created_time': '2025-10-10T10:00:00', 'keywords': ['keyword1', 'keyword2']}
INDEX_NAME = "newspaper_articles_with_keywords"


# --- KHỞI TẠO ỨNG DỤNG VÀ KẾT NỐI ---
app = FastAPI(
    title="Real-time Trending Keywords API with Z-Score",
    description="API để tính toán và truy vấn các từ khóa thịnh hành dựa trên thuật toán Z-Score.",
    version="2.0.0",
)

try:
    es_client = Elasticsearch(
        [{'host': ES_HOST, 'port': ES_PORT, 'scheme': 'http'}],
        request_timeout=30,
        max_retries=5,
        retry_on_timeout=True
    )
    if not es_client.ping():
        raise ConnectionError("Không thể kết nối tới Elasticsearch.")
except ConnectionError as e:
    print(f"Lỗi kết nối Elasticsearch: {e}")
    # Trong môi trường production, bạn có thể muốn hệ thống thoát hoặc thử lại
    # ở đây chúng ta chỉ in ra lỗi
    es_client = None

# --- CÁC HÀM HỖ TRỢ ---

def build_zscore_es_query(t1_start, t1_end, t0_start, t0_end):
    """
    Xây dựng một truy vấn aggregation duy nhất cho Elasticsearch để tính toán
    tần suất từ khóa trong 2 khoảng thời gian T1 (gần) và T0 (lịch sử).
    """
    return {
        "size": 0,
        "query": {
            "range": {
                "created_time": {
                    "gte": t0_start.isoformat(),
                    "lte": t1_end.isoformat(),
                    "format": "strict_date_optional_time"
                }
            }
        },
        "aggs": {
            "keywords_agg": {
                "terms": {
                    "field": "keywords.keyword",  # Sử dụng .keyword để aggregate trên chuỗi chính xác
                    "size": 200  # Lấy 200 từ khóa có tần suất cao nhất để tính toán
                },
                "aggs": {
                    "time_split": {
                        "filters": {
                            "filters": {
                                "T1_recent": {
                                    "range": {
                                        "created_time": {
                                            "gte": t1_start.isoformat(),
                                            "lte": t1_end.isoformat(),
                                            "format": "strict_date_optional_time"
                                        }
                                    }
                                },
                                "T0_baseline": {
                                    "range": {
                                        "created_time": {
                                            "gte": t0_start.isoformat(),
                                            "lte": t0_end.isoformat(),
                                            "format": "strict_date_optional_time"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }


def calculate_z_score(buckets, t1_duration_hours, t0_duration_hours):
    """
    Tính toán Z-Score từ kết quả aggregation của Elasticsearch.
    """
    trending_keywords = []
    if not buckets:
        return []

    for bucket in buckets:
        keyword = bucket['key']
        count_t1 = bucket['time_split']['buckets']['T1_recent']['doc_count']
        count_t0 = bucket['time_split']['buckets']['T0_baseline']['doc_count']

        # Chuẩn hóa tần suất theo giờ để so sánh công bằng
        freq_t1 = count_t1 / t1_duration_hours if t1_duration_hours > 0 else 0
        freq_t0 = count_t0 / t0_duration_hours if t0_duration_hours > 0 else 0

        # Để tránh chia cho 0 và Z-score vô hạn, ta cần xử lý các trường hợp đặc biệt
        if freq_t0 == 0:
            # Nếu tần suất lịch sử = 0, bất kỳ sự xuất hiện nào cũng là bất thường
            # Gán một điểm Z-score rất cao
            z_score = 10.0 if freq_t1 > 0 else 0
        else:
            # Đây là công thức Z-score cơ bản cho phân phối Poisson (phù hợp cho đếm sự kiện)
            # Z = (quan sát - trung bình) / sqrt(trung bình)
            # Ở đây, quan sát là freq_t1 và trung bình (kỳ vọng) là freq_t0
            try:
                z_score = (freq_t1 - freq_t0) / math.sqrt(freq_t0)
            except ValueError:
                z_score = 0 # Xảy ra nếu freq_t0 âm (dù không thể)

        # Chỉ coi là xu hướng nếu có sự tăng trưởng và số lượng đủ lớn
        if z_score > 1.96 and count_t1 > 5:  # 1.96 tương ứng với độ tin cậy 95%
            trending_keywords.append({
                "keyword": keyword,
                "z_score": round(z_score, 2),
                "recent_count": count_t1,
                "baseline_count": count_t0
            })

    # Sắp xếp theo Z-score giảm dần
    trending_keywords.sort(key=lambda x: x['z_score'], reverse=True)
    return trending_keywords


# --- API ENDPOINTS ---

@app.get("/trending")
def get_trending_keywords(
    duration_mins: int = Query(60, description="Khoảng thời gian gần nhất (T1) để phân tích, tính bằng phút."),
    baseline_hours: int = Query(24, description="Khoảng thời gian lịch sử (T0) để so sánh, tính bằng giờ."),
    top_n: int = Query(20, description="Số lượng từ khóa xu hướng hàng đầu cần trả về.")
):
    """
    Endpoint chính để xác định các từ khóa đang là xu hướng.

    Nó so sánh tần suất xuất hiện của từ khóa trong `duration_mins` gần nhất
    với tần suất trung bình trong `baseline_hours` trước đó để tính Z-Score.
    """
    if not es_client:
        raise HTTPException(status_code=503, detail="Dịch vụ Elasticsearch không khả dụng.")

    now = datetime.utcnow()
    t1_end = now
    t1_start = t1_end - timedelta(minutes=duration_mins)
    t0_end = t1_start
    t0_start = t0_end - timedelta(hours=baseline_hours)

    try:
        # 1. Gửi truy vấn duy nhất đến Elasticsearch
        query = build_zscore_es_query(t1_start, t1_end, t0_start, t0_end)
        response = es_client.search(index=INDEX_NAME, body=query)

        # 2. Xử lý kết quả và tính toán Z-score
        buckets = response['aggregations']['keywords_agg']['buckets']
        trending_results = calculate_z_score(
            buckets,
            t1_duration_hours=duration_mins / 60.0,
            t0_duration_hours=float(baseline_hours)
        )

        # 3. Trả về kết quả
        return {
            "analysis_window": {
                "recent_period_T1": {"from": t1_start.isoformat(), "to": t1_end.isoformat()},
                "baseline_period_T0": {"from": t0_start.isoformat(), "to": t0_end.isoformat()},
            },
            "trending_keywords": trending_results[:top_n]
        }

    except Exception as e:
        print(f"Lỗi xảy ra khi truy vấn trending: {e}")
        raise HTTPException(status_code=500, detail=f"Lỗi máy chủ nội bộ khi xử lý yêu cầu: {e}")


@app.get("/articles_by_keyword")
def get_articles_by_keyword(
    keyword: str,
    days_ago: int = Query(7, description="Tìm các bài báo trong N ngày vừa qua chứa từ khóa.")
):
    """
    Lấy danh sách các bài báo gần đây có chứa một từ khóa cụ thể.
    """
    if not es_client:
        raise HTTPException(status_code=503, detail="Dịch vụ Elasticsearch không khả dụng.")

    start_date = datetime.utcnow() - timedelta(days=days_ago)
    query = {
        "size": 50, # Giới hạn 50 bài báo
        "query": {
            "bool": {
                "must": [
                    {"term": {"keywords.keyword": keyword}},
                    {"range": {"created_time": {"gte": start_date.isoformat()}}}
                ]
            }
        },
        "sort": [{"created_time": {"order": "desc"}}]
    }
    try:
        response = es_client.search(index=INDEX_NAME, body=query)
        articles = [{"id": hit["_id"], **hit["_source"]} for hit in response["hits"]["hits"]]
        return {"articles": articles}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khi tìm kiếm bài báo: {e}")


# --- Chạy ứng dụng (dùng cho debug) ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)