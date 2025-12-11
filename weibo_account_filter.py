import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime

# 目标账号信息
ACCOUNT = "罗永浩的十字路口"
UID = "7762107285"
KEYWORDS = ["西贝", "预制菜", "餐饮", "食品", "料理"]

def crawl_weibo():
    """爬取微博数据"""
    print("正在爬取罗永浩的微博...")
    
    # 访问移动端页面
    url = f"https://m.weibo.cn/u/{UID}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }
    
    weibo_list = []
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 查找微博正文
            weibo_contents = soup.find_all('div', attrs={'class': re.compile(r'weibo-text|txt')})
            
            print(f"找到 {len(weibo_contents)} 条微博")
            
            for i, content_div in enumerate(weibo_contents):
                # 获取微博文本
                text = content_div.get_text(strip=True)
                
                # 检查是否包含关键词
                matched_keywords = []
                for keyword in KEYWORDS:
                    if keyword in text:
                        matched_keywords.append(keyword)
                
                if not matched_keywords:
                    continue
                
                # 获取发布时间
                time_elem = content_div.find_previous('span', class_=re.compile(r'time|from'))
                time_str = time_elem.get_text(strip=True) if time_elem else "未知时间"
                
                # 只保留需要的5个要素
                weibo = {
                    '账号': ACCOUNT,
                    '用户ID': UID,
                    '匹配关键词': '、'.join(matched_keywords),
                    '发布时间': time_str,
                    '微博内容': text[:500]  # 限制长度
                }
                
                weibo_list.append(weibo)
                
                # 显示前几条
                if i < 5:
                    print(f"\n微博{i+1}:")
                    print(f"  时间: {time_str}")
                    print(f"  关键词: {weibo['匹配关键词']}")
                    print(f"  内容: {text[:100]}...")
        
        # 保存数据
        if weibo_list:
            df = pd.DataFrame(weibo_list)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            excel_file = f"微博数据_{timestamp}.xlsx"
            df.to_excel(excel_file, index=False)
            
            print(f"\n✓ 共找到 {len(weibo_list)} 条相关微博")
            print(f"✓ 数据已保存到: {excel_file}")
        else:
            print("\n没有找到包含关键词的微博")
            
    except Exception as e:
        print(f"爬取失败: {e}")

if __name__ == "__main__":
    crawl_weibo()