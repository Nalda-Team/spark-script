from datetime import datetime, timezone
import urllib.parse
import json
from pytz import timezone as py_timezone
import re

def decode_url_text(text):
    return urllib.parse.unquote(text)

def get_airport_timezone(airport_code, airport_map):
    return py_timezone(airport_map[airport_code]['time_zone'])


# 시간 문자열을 문자열로 변환하는 함수 (안전하게 처리)
def convert_to_utc_str(time_str, airport_code, airport_map):
    tz = get_airport_timezone(airport_code, airport_map)
    date   = time_str[:-4]   
    hour   = time_str[-4:-2]   
    minute = time_str[-2:]     
    dt = datetime.strptime(f"{date} {hour}:{minute}", "%Y%m%d %H:%M")
    local = tz.localize(dt)
    utc   = local.astimezone(py_timezone('UTC'))
    return utc.strftime("%Y-%m-%d %H:%M:%S")

# 공항 코드 존재 여부 확인하는 함수
def check_airport_exists(airport_code, airport_map):
    return airport_code in airport_map

# URL에서 Fare Class 추출하는 함수
def parse_fare_class(url):
    try:
        match = re.search(r'FareRuleItnInfo=([^&]+)', url)
        if not match:
            return 'n'
        
        fare_info = match.group(1)
        parts = fare_info.split('/')
        if len(parts) >= 3:
            return parts[2]
        return 'n'
    except Exception:
        return 'n'

# URL 디코드 함수
def decode_url_text(text):
    try:
        return urllib.parse.unquote(text)
    except:
        return text