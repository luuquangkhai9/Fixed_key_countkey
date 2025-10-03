import json
from collections import defaultdict
from datetime import datetime , timedelta
# from langdetect import detect 
import numpy as np
import string
import re
# import matplotlib.pyplot as plt
from keyword_save_es import  load_data_to_elasticsearch_kw_a, bulk_data_to_elasticsearch_kw_a
from elasticsearch import Elasticsearch
from time import sleep
import os
from dotenv import load_dotenv
from main_query_es import  query_keyword_with_trend
from collections import defaultdict

# def CheckBig(arrcheck):
#     min_value = min(arrcheck)
#     if min_value < 1:
#         return False
#     if arrcheck[6] >min_value*3.2:
#         return True
#     else:
#         return False
def CheckBig(arrcheck):
    min_value = min(arrcheck)
    if min_value < 1:
        return False
    if arrcheck[6] >min_value + 3:
        return True
    else:
        return False

# def CheckBig(arrcheck):
#     for ar in arrcheck:
#         if ar < 1:
#             return False
#     if arrcheck[6] >2.5:
#         return True
#     else:
#         return False
# def CheckBig2(arrcheck):
#     count_out_25 = sum(1 for ar in arrcheck if ar == 0)
#     count_in_6 = sum(1 for ar in arrcheck[:7] if ar <= 6)
#     if count_out_25 >= 3  and count_in_6 >=5:
#         return True
#     else:
#         return False

def CheckSmall(arrcheck):
    for ar in arrcheck:
        if ar > 1:
            return False
    return True
def CheckPre(arrcheck):
    bRet = False
    for ar in arrcheck:
        if ar < 2:
            bRet = True
            break
    if bRet == True:
        if ((arrcheck[6]-min(arrcheck))>0.4) and (arrcheck[6]>2) and (arrcheck[6]>arrcheck[5]):
            return True
        else:
            return False          
    else:
        return False
def CheckOld(arrcheck):
    bRet = False
    for ar in arrcheck:
        if ar > 2:
            bRet = True
            break
    if bRet == True:
        if arrcheck[6]<2 :
            return True
        else:
            return False          
    else:
        return False
def CheckTrend(arrcheck):
    min_value = min(arrcheck)
    count_below_half = sum(1 for ar in arrcheck if ar <= 0.5)
    if count_below_half >= 4:
        if arrcheck[6] - min_value >= 1.1:
            return True

    elif count_below_half >= 3:
        if arrcheck[6] - min_value >= 1.4:
            return True

    elif count_below_half >= 2:
        if arrcheck[6] - min_value >= 1.6:
            return True
    elif count_below_half == 1:
        if arrcheck[6] - min_value >= 1.8:
            return True
    elif min_value < 1:
        if arrcheck[6] > 2:
            return True

    return False

def Check(keywordtop):
    categorized_keywords = {
        'BigTrend': [],
        'Trend': [],
        'PreTrend': [],
        'OldTrend': [],
        'Other': [],
        'Small': []
        
    }

    for arr in keywordtop:
        if CheckSmall(arr['percentage']):
            # continue  
            categorized_keywords['Small'].append({'keyword': arr['keyword'], 'percentage': arr['percentage'][-1] ,'record': arr['record'] ,'score': arr['score'] ,  'isTrend': True})

        if CheckBig(arr['percentage']):
            categorized_keywords['BigTrend'].append({'keyword': arr['keyword'], 'percentage': arr['percentage'][-1] ,'record': arr['record'] ,'score': arr['score'] ,  'isTrend': True})
        elif CheckTrend(arr['percentage']):
            categorized_keywords['Trend'].append({'keyword': arr['keyword'], 'percentage': arr['percentage'][-1] ,'record': arr['record'] ,'score': arr['score'], 'isTrend': True})
        elif CheckPre(arr['percentage']):
            categorized_keywords['PreTrend'].append({'keyword': arr['keyword'], 'percentage': arr['percentage'][-1] ,'record': arr['record'] ,'score': arr['score'], 'isTrend': False})
        # elif CheckOld(arr['percentage']):
        #     categorized_keywords['OldTrend'].append({'keyword': arr['keyword'], 'percentage': arr['percentage'][-1] ,'record': arr['record'] , 'isTrend': True})
        else:
            categorized_keywords['Other'].append({'keyword': arr['keyword'], 'percentage': arr['percentage'][-1] ,'record': arr['record'] ,'score': arr['score'], 'isTrend': False})

    for category in categorized_keywords:
        categorized_keywords[category].sort(key=lambda x: x['score'], reverse=True)
    
    sorted_keywords = []
    for category in ['BigTrend', 'Trend', 'PreTrend', 'Other', "Small"]:
        sorted_keywords.extend(categorized_keywords[category])

    return sorted_keywords
def check_big2(results, keyword, topic_id):
    actual_days = len(results)
    missing_days = 20 - actual_days 
    
    count_out_top20 = 0
    count_in_top6 = 0
    
    for record in results:
        topic_data = record['_source']['topic_ids'].get(topic_id, {'keywords_top': []})
        keywords = [kw_info['keyword'] for kw_info in topic_data['keywords_top']]
        try:
            if keyword not in keywords[:20]:
                count_out_top20 += 1
        except : 
            if keyword not in keywords:
                count_out_top20 += 1
        try: 
            if keyword in keywords[:6]:
                count_in_top6 += 1
        except:
            if keyword in keywords:
                count_in_top6 += 1

    count_out_top20 += missing_days

    if count_out_top20 >= 3 and count_in_top6 >= 4:
        return True
    return False

def is_not_blackword(word):
    
    with open('black_list.txt', 'r', encoding='utf-8') as f:
        black_words = f.read().splitlines()

    if word in black_words:
        return False
    
    if 'ảnh' in word :
        return False
    
    return True

# def is_keyword_selected(keyword, historical_percentages, daily_keywords, check_date_str):
#     percentage_on_check_date = next((item['percentage'] for item in daily_keywords if item['keyword'] == keyword), 0)    
    
#     # other_dates_percentages = [
#     #     historical_percentages.get(i, 0)  # Ensure a default value of 0 if the topic key is missing
#     #     for i in range(6)  # Chỉ lấy 6 ngày trước đó, bỏ qua ngày hiện tại
#     # ]
#     for key, value in historical_percentages.items():
#         if isinstance(value, list):
#             other_dates_percentages = value[:6]  # Chỉ lấy 6 ngày trước đó, bỏ qua ngày hiện tại
#             break
    
#     if not other_dates_percentages:
#         other_dates_percentages = [0] * 6

#     count_higher_09 = sum(perc >= 0.88 for perc in other_dates_percentages)
#     count_higher_11 = sum(perc >= 1.1 for perc in other_dates_percentages)
#     count_higher_14 = sum(perc >= 1.4 for perc in other_dates_percentages)

#     if count_higher_09 >= 3 and count_higher_11 >= 2:
#         min_other_percentage = min(other_dates_percentages) if other_dates_percentages else 0
#         if percentage_on_check_date > 3 * min_other_percentage and count_higher_14 <= 5:
#             return True
#         else:
#             return False
#     else:
#         return True
def is_keyword_selected(keyword, historical_percentages, daily_keywords, check_date_str):
    percentage_on_check_date = next((item['percentage'] for item in daily_keywords if item['keyword'] == keyword), 0)    
    
    # other_dates_percentages = [
    #     historical_percentages.get(i, 0)  # Ensure a default value of 0 if the topic key is missing
    #     for i in range(6)  # Chỉ lấy 6 ngày trước đó, bỏ qua ngày hiện tại
    # ]
    for key, value in historical_percentages.items():
        if isinstance(value, list):
            other_dates_percentages = value[:6]  # Chỉ lấy 6 ngày trước đó, bỏ qua ngày hiện tại
            break
    
    if not other_dates_percentages:
        other_dates_percentages = [0] * 6

    count_higher_09 = sum(perc >= 0.88 for perc in other_dates_percentages)
    count_higher_11 = sum(perc >= 1.1 for perc in other_dates_percentages)
    count_higher_14 = sum(perc >= 1.4 for perc in other_dates_percentages)

    if count_higher_09 >= 3 and count_higher_11 >= 2:
        min_other_percentage = min(other_dates_percentages) if other_dates_percentages else 0
        if percentage_on_check_date > 3 +  min_other_percentage and count_higher_14 <= 5:
            return True
        else:
            return False
    else:
        return True

# def is_subkeyword(keyword, other_keyword):
#     """
#     Check if any word in 'keyword' is present in 'other_keyword' after splitting both by underscores.
#     """
#     # Splitting the keywords into lists of words
#     keyword_words = keyword.lower().split('_')
#     other_keyword_words = other_keyword.lower().split('_')

#     # Checking if any word in keyword is present in other_keyword
#     return all(word in other_keyword_words for word in keyword_words) or all(word in keyword_words for word in other_keyword_words)
def is_subkeyword(keyword, other_keyword):
    """
    Check if any word in 'keyword' is present in 'other_keyword' after splitting both by underscores.
    Additionally, return true if there are at least four common words between the two keywords.
    """
    # Splitting the keywords into lists of words
    keyword_words = set(keyword.lower().split('_'))
    other_keyword_words = set(other_keyword.lower().split('_'))

    # Checking if any word in keyword is present in other_keyword
    basic_check = all(word in other_keyword_words for word in keyword_words) or all(word in keyword_words for word in other_keyword_words)

    # Checking for at least four common words
    common_words = keyword_words.intersection(other_keyword_words)
    four_common_words_check = len(common_words) >= 4

    return basic_check or four_common_words_check


def filter_keywords_all_words_no_sort(keyword_list):
    """
    Filter the keywords based on the all-words subkeyword relation without sorting.
    Each keyword in the list is a tuple of (keyword, percentage).
    """
    filtered_keywords = []
    
    for keyword, percentage in keyword_list:
        if(keyword =='thanh_hoá'):
            print(1)
        # Check if the current keyword is a subkeyword of any keyword in the filtered list
        if not any(is_subkeyword(keyword, existing_keyword) for existing_keyword, _ in filtered_keywords):
            filtered_keywords.append((keyword, percentage))

    return filtered_keywords



# def calculate_top_keywords_with_topic_2_es(es, input_date, data, index_name, platform):
#     try:
#         with open('blacklist_hashtag.txt', 'r', encoding='utf-8') as f:
#             blacklist = set(line.strip() for line in f if line.strip())
#     except Exception as e:
#         print(f"Error reading blacklist file: {e}")
#         blacklist = set()

#     # Định dạng ngày
#     date_format = "%m/%d/%Y"
#     # Chuyển input_date thành datetime object
#     date_obj = datetime.strptime(input_date, date_format)
#     date_str = date_obj.strftime("%m_%d_%Y")

#     # Khởi tạo các biến để lưu trữ số liệu thống kê
#     date_counts = defaultdict(int)
#     topic_article_counts = defaultdict(int)
#     keyword_counts = defaultdict(lambda: defaultdict(int))
#     hashtag_counts = defaultdict(lambda: defaultdict(int))
#     topic_ids_set = set()
    
#     # Duyệt qua từng item trong dữ liệu
#     for item in data:
#         keywords_field = 'keyword' if item['_index'] == 'posts' else 'keywords'
#         hashtags_field = 'hashtag' if item['_index'] == 'posts' else 'hashtags'
        
#         # keywords_field = 'keyword'
#         # hashtags_field = 'hashtag'

#         # Lấy ngày tạo từ _source và chuyển đổi thành chuẩn định dạng ngày
#         item_date_str = datetime.strptime(item['_source']['created_time'], '%m/%d/%Y %H:%M:%S').strftime(date_format)
        
#         # Nếu ngày tạo trùng khớp với input_date, tiến hành thống kê
#         if item_date_str == input_date:
#             date_counts[item_date_str] += 1
#             for topic_id in item['_source'].get('topic_id', []):
#                 topic_article_counts[topic_id] += 1
#                 topic_ids_set.add(topic_id)
#             topic_article_counts["all"] += 1  # Tăng tổng số bài viết cho "all"

#             for keyword in item['_source'].get(keywords_field, []):
#                 if len(keyword) > 2: 
#                     # if keyword == "vạn_thịnh_phát":
#                     #     pass
#                     for topic_id in item['_source'].get('topic_id', []):
#                         keyword_counts[topic_id][keyword] += 1
#                     keyword_counts["all"][keyword] += 1
#                     # if keyword == "vạn_thịnh_phát":
#                     #     print(keyword_counts["all"][keyword])
#             # print(keyword_counts["all"]["vạn_thịnh_phát"])

#             for hashtag in item['_source'].get(hashtags_field, []):
#                 if len(hashtag) > 2 and hashtag not in blacklist:  # Lọc các hashtag có độ dài lớn hơn 2
#                     for topic_id in item['_source'].get('topic_id', []):
#                         hashtag_counts[topic_id][hashtag] += 1
#                     hashtag_counts["all"][hashtag] += 1
#     # if (len(keyword_counts)>0):        
#     #     print(keyword_counts["all"]["vạn_thịnh_phát"])

#     # Tính toán tỉ lệ phần trăm của các từ khóa
#     topic_data = {}
#     topic_ids = list(topic_ids_set) + ["all"]

#     for topic_id in topic_ids:
#         # if topic_id == "all":
#         #     pass
#         total_articles = topic_article_counts[topic_id]
#         # keyword_percentages = []
#         # for keyword, count in keyword_counts[topic_id].items():
#         #     if keyword == "vạn_thịnh_phát":
#         #         pass
#         #     keyword_percentages.append({"keyword": keyword, "percentage": (count / total_articles) * 100, "record": count})
#         keyword_percentages = [
#             {"keyword": keyword, "percentage": (count / total_articles) * 100, "record": count}
#             for keyword, count in keyword_counts[topic_id].items()
#         ]
#         keyword_percentages = sorted(keyword_percentages, key=lambda x: x['percentage'], reverse=True)[:600]

#         hashtag_percentages = [
#             {"hashtag": hashtag, "percentage": (count / total_articles) * 100, "record": count}
#             for hashtag, count in hashtag_counts[topic_id].items()
#         ]
#         hashtag_percentages = sorted(hashtag_percentages, key=lambda x: x['percentage'], reverse=True)[:600]

#         if keyword_percentages or hashtag_percentages or topic_id == "all":
#             topic_data[topic_id] = {
#                 "keywords_top": keyword_percentages,
#                 "hashtags_top": hashtag_percentages
#             }
#     # Lưu kết quả vào Elasticsearch
#     if topic_data.get("all", {}).get("keywords_top") or topic_data.get("all", {}).get("hashtags_top"):
#         # Lưu kết quả vào Elasticsearch
#         data = {
#             "date": date_str,
#             "type": platform,
#             "topic_ids": topic_data
#         }
#         sleep(1.5)
#         load_data_to_elasticsearch_kw_a(es, data, index_name)

#     # Trả về kết quả
#     return {
#         "date": date_str,
#         "type": platform,
#         "topic_ids": topic_data
#     }
    
def calculate_top_keywords_with_topic_2_es(es, input_date, data, index_name, platform):
    try:
        with open('blacklist_hashtag.txt', 'r', encoding='utf-8') as f:
            blacklist = set(line.strip() for line in f if line.strip())
    except Exception as e:
        print(f"Error reading blacklist file: {e}")
        blacklist = set()

    date_format = "%m/%d/%Y"
    date_obj = datetime.strptime(input_date, date_format)
    date_str = date_obj.strftime("%m_%d_%Y")

    keyword_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    hashtag_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    topic_article_counts = defaultdict(lambda: defaultdict(int))
    test_topic = []
    for item in data:
        keywords_field = 'keyword' if item['_index'] == 'posts' else 'keywords'
        hashtags_field = 'hashtag' if item['_index'] == 'posts' else 'hashtags'

        item_date_str = datetime.strptime(item['_source']['created_time'], '%m/%d/%Y %H:%M:%S').strftime(date_format)
        if item_date_str != input_date:
            continue

        tenancy_ids = item['_source'].get('tenancy_ids', [])
        topic_ids = item['_source'].get('topic_id', [])
        test_topic.extend(topic_ids)
        for tenancy_id in tenancy_ids:
            topic_article_counts[tenancy_id]["all"] += 1
            for topic_id in topic_ids:
                topic_article_counts[tenancy_id][topic_id] += 1

            for keyword in item['_source'].get(keywords_field, []):
                if len(keyword) > 2:
                    keyword_counts[tenancy_id]["all"][keyword] += 1
                    for topic_id in topic_ids:
                        keyword_counts[tenancy_id][topic_id][keyword] += 1

            for hashtag in item['_source'].get(hashtags_field, []):
                if len(hashtag) > 2 and hashtag not in blacklist:
                    hashtag_counts[tenancy_id]["all"][hashtag] += 1
                    for topic_id in topic_ids:
                        hashtag_counts[tenancy_id][topic_id][hashtag] += 1

    print(f"Total topics found: {len(set(test_topic))}")
    arr_key_hash = []

    for tenancy_id in topic_article_counts:
        for topic_id in topic_article_counts[tenancy_id]:
            total_articles = topic_article_counts[tenancy_id][topic_id]

            keyword_percentages = [
                {"keyword": keyword, "percentage": (count / total_articles) * 100, "record": count}
                for keyword, count in keyword_counts[tenancy_id][topic_id].items()
            ]
            keyword_percentages = sorted(keyword_percentages, key=lambda x: x['percentage'], reverse=True)[:100]

            hashtag_percentages = [
                {"hashtag": hashtag, "percentage": (count / total_articles) * 100, "record": count}
                for hashtag, count in hashtag_counts[tenancy_id][topic_id].items()
            ]
            hashtag_percentages = sorted(hashtag_percentages, key=lambda x: x['percentage'], reverse=True)[:100]
            if keyword_percentages:
                document_id = f"key_{date_str}_{platform}_{tenancy_id}_{topic_id}"
                result_key = {
                    "id": document_id,
                "date": date_str,
                "type": platform,
                "tenant_id": tenancy_id,
                "topic_id":topic_id,
                "keywords_top": keyword_percentages
            }
                arr_key_hash.append(result_key)
            if hashtag_percentages:
                document_id = f"hashtag_{date_str}_{platform}_{tenancy_id}_{topic_id}"
                result_hashtag = {
                    "id": document_id,
                "date": date_str,
                "type": platform,
                "tenant_id": tenancy_id,
                "topic_id":topic_id,
                "hashtags_top": hashtag_percentages
            }
                arr_key_hash.append(result_hashtag)
    if arr_key_hash :
        try:
            bulk_data_to_elasticsearch_kw_a(es, arr_key_hash, index_name)
        except Exception as e:
            print(f"Error uploading to Elasticsearch: {e}")

    return 

def calculate_top_keywords_with_trend_logic_topic(input_date, es, historical_data_index, platform):
    input_datetime = datetime.strptime(input_date, "%m/%d/%Y")
    input_date_str = input_datetime.strftime("%m_%d_%Y")
    start_date_str = (input_datetime - timedelta(days=6)).strftime("%m_%d_%Y")
    end_date_str = input_date_str
    sleep(1.5)

    # Truy vấn dữ liệu 7 ngày gần nhất
    '''
    daily_keywords = es.search(index=historical_data_index, body={
        "query": {
            "bool": {
                "filter": [
                    {"term": {"type": platform}},
                    {
                        "range": {
                            "date": {
                                "gte": start_date_str,
                                "lte": end_date_str,
                                "format": "MM_dd_yyyy"
                            }
                        }
                    }
                ]
            }
        },
        "_source": ["date",  "tenant_id","topic_id","keywords_top"],
        "size": 100,
        "timeout": "60s"
    })['hits']['hits']
    '''

    body={
        "query": {
            "bool": {
                "filter": [
                    {"term": {"type": platform}},
                    {
                        "range": {
                            "date": {
                                "gte": start_date_str,
                                "lte": end_date_str,
                                "format": "MM_dd_yyyy"
                            }
                        }
                    }
            ]
            }
        },
        "sort": [{"date": {"order": "asc"}}, {"topic_id": {"order": "asc"}}],
        "_source": ["date",  "tenant_id","topic_id","keywords_top"],
        "size":4000
        
    }
    #current_day_keywords = [hit['_source'] for hit in daily_keywords if hit['_source']['date'] == input_date_str]
    #daily_keywords = [hit['_source'] for hit in daily_keywords]
    daily_keywords = query_keyword_with_trend(es,historical_data_index,body)
    # Nhóm theo date
    grouped = defaultdict(list)
    for keyword_day in daily_keywords:
        if "keywords_top" not in keyword_day:
            continue
        grouped[keyword_day["date"]].append(keyword_day)
    daily_keywords = [grouped[date] for date in sorted(grouped.keys())]
    
    for keyword_day in daily_keywords:
        if keyword_day[0]["date"] == input_date_str:
            current_day_keywords   = keyword_day
            break


    # Truy vấn dữ liệu 20 ngày cho check_big2
    start_date_str_big = (input_datetime - timedelta(days=19)).strftime("%m_%d_%Y")

    body={
    "query": {
        "bool": {
            "filter": [
                {"term": {"type": platform}},
                {
                    "range": {
                        "date": {
                            "gte": start_date_str_big,
                            "lte": end_date_str,
                            "format": "MM_dd_yyyy"
                        }
                    }
                }
            ]
        }
    },
    "sort": [{"date": {"order": "asc"}}, {"topic_id": {"order": "asc"}}],
    "_source": ["date",  "tenant_id","topic_id","keywords_top"],
    "size":4000
    
}
    #ndc
    #results = query_keyword_with_trend(es,historical_data_index,body)
    results= []


    results_by_tenancy = {}
    arr_key_trend = []
    if len(daily_keywords) == 10:
        historical_percentages = {}
        for day_keyword in daily_keywords:
            for keywords in day_keyword:
                for keyword_info in keywords.get('keywords_top', [])[:640]:
                    keyword = keyword_info['keyword'] 
                    if keyword not in historical_percentages:
                        historical_percentages[keyword] = {}
                    if topic_id not in historical_percentages[keyword]:
                        historical_percentages[keyword][topic_id] = [0] * 7
                    index = (input_datetime - datetime.strptime(day_keyword[0]['date'], "%m_%d_%Y")).days
                    historical_percentages[keyword][topic_id][6 - index] = keyword_info['percentage']
                



        for tenancy_id, topic_dict in current_day_keywords[0]['topic_ids'].items():
            for topic_id, topic_data in topic_dict.items():
                keywordtop_for_check = [
                    {
                        "keyword": keyword_info['keyword'],
                        "percentage": historical_percentages.get(keyword_info['keyword'], {}).get(topic_id, [0] * 7),
                        "record": keyword_info["record"],
                        "score": keyword_info.get("score", 0),
                        # "isTrend": keyword_info.get("isTrend", False)
                    }
                    for keyword_info in topic_data.get('keywords_top', [])[:640]
                ]

                sorted_keywords = Check(keywordtop_for_check)
                top_keywords, top_keywords_big = [], []
                un_top_keywords, un_top_keywords_2, black_keywords = [], [], []
                top_10_current_day_keywords = [kw_info['keyword'] for kw_info in topic_data.get('keywords_top', [])[:4]]

                for kw_dict in sorted_keywords:
                    if is_not_blackword(kw_dict['keyword']):
                        topic_specific_percentages = historical_percentages[kw_dict['keyword']].get(topic_id, [0] * 7)
                        if check_big2(results, kw_dict['keyword'], topic_id):
                            top_keywords_big.append(kw_dict)
                            kw_dict['isTrend'] = True
                        elif is_keyword_selected(kw_dict['keyword'], {topic_id: topic_specific_percentages}, sorted_keywords, input_date_str):
                            kw_dict['isTrend'] = True
                            if '_' in kw_dict['keyword'] or kw_dict['keyword'] in top_10_current_day_keywords:
                                top_keywords.append(kw_dict)
                            else:
                                un_top_keywords.append(kw_dict)
                        else:
                            un_top_keywords_2.append(kw_dict)
                            kw_dict['isTrend'] = False
                    else:
                        black_keywords.append(kw_dict)
                        kw_dict['isTrend'] = False

                final_keywords = (
                    top_keywords[:8] + top_keywords_big + top_keywords[8:400] +
                    un_top_keywords + un_top_keywords_2 + top_keywords[400:] + black_keywords
                    if len(top_keywords) > 400
                    else top_keywords[:8] + top_keywords_big + top_keywords[8:] +
                         un_top_keywords + un_top_keywords_2 + black_keywords
                )

                if final_keywords:
                    if tenancy_id not in results_by_tenancy:
                        results_by_tenancy[tenancy_id] = {}
                    results_by_tenancy[tenancy_id][topic_id] = final_keywords

    else:
        last_day_data = daily_keywords[-1] if daily_keywords else {}
        for keywords_data in  last_day_data:
            default_keywords = [
                {
                    "keyword": kw_info['keyword'],
                    "percentage": kw_info['percentage'],
                    "record": kw_info['record'],
                    "score": 0,
                    "isTrend": False
                }
                for kw_info in keywords_data.get('keywords_top', [])[:100]
            ]

            document_id = f"trend_{keywords_data['date']}_{platform}_{keywords_data['tenant_id']}_{keywords_data['topic_id']}"
            result_key = {
                "id": document_id,
                "date": keywords_data['date'],
                "type": platform,
                "tenant_id": keywords_data['tenant_id'],
                "topic_id":keywords_data['topic_id'],
                "keywords_trend": default_keywords
            }
            arr_key_trend.append(result_key)




    if arr_key_trend :
        try:
            bulk_data_to_elasticsearch_kw_a(es, arr_key_trend, "key-trend-current")
        except Exception as e:
            print(f"Error uploading to Elasticsearch: {e}")
        print("update trend thanh cong tu khoa xu huong ", f"{input_datetime.strftime('%m_%d_%Y')}_{platform}")
    return 

    
