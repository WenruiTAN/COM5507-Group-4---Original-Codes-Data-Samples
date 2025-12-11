import pandas as pd
import numpy as np
from datetime import datetime
import time

def parse_weibo_time(time_str):
    """
    专门解析微博时间格式：Fri Dec 05 13:21:43 +0800 2025
    返回datetime对象，解析失败则返回NaT（Not a Time）
    """
    if pd.isna(time_str) or time_str == '':
        return pd.NaT
    
    try:
        # 匹配格式：星期 月份 日期 时:分:秒 时区 年份
        # %a=星期缩写(Fri), %b=月份缩写(Dec), %d=日期(05), %H:%M:%S=时间, %z=时区(+0800), %Y=年份
        return datetime.strptime(time_str, '%a %b %d %H:%M:%S  %Y')
    except:
        # 兼容部分格式异常的情况（如时区符号/空格差异）
        try:
            return datetime.strptime(time_str, '%a %b %d %H:%M:%S  %Y').astimezone()
        except:
            return pd.NaT

def generate_weibo_codebook(
    df: pd.DataFrame,
    categorical_top_n: int = 3,  # 分类型变量显示前3个高频类别
    save_path: str = None,       # 保存路径（csv/xlsx）
    decimal_places: int = 2      # 数值统计量保留小数位数
) -> pd.DataFrame:
    """适配微博爬取数据的Codebook生成函数（支持解析特殊时间格式）"""
    codebook_list = []
    
    for col in df.columns:
        # 基础信息（所有变量通用）
        col_info = {
            '变量名': col,
            '原始数据类型': str(df[col].dtype),
            '样本总数': len(df),
            '缺失值数量': df[col].isnull().sum(),
            '缺失值比例(%)': round((df[col].isnull().sum() / len(df)) * 100, decimal_places),
            '唯一值数量': df[col].nunique(dropna=True)
        }

        # 1. 时间型变量（先判断是否是发布时间列，再解析）
        # 匹配包含「发布时间」「时间」等关键词的列，强制解析时间格式
        time_col_keywords = ['发布时间', '时间', '发布日期', 'date', 'time']
        if any(keyword in col for keyword in time_col_keywords) or np.issubdtype(df[col].dtype, np.datetime64):
            # 先对列进行时间解析
            df[col] = df[col].apply(parse_weibo_time)
            
            col_info['变量类型'] = '时间型'
            valid_times = df[col].dropna()
            col_info['最早时间'] = valid_times.min() if not valid_times.empty else '-'
            col_info['最晚时间'] = valid_times.max() if not valid_times.empty else '-'
            if not valid_times.empty:
                time_span = (valid_times.max() - valid_times.min()).total_seconds() / 3600  # 转换为小时
                col_info['时间跨度(小时)'] = round(time_span, decimal_places)
            else:
                col_info['时间跨度(小时)'] = '-'
            # 非时间字段置空
            col_info['均值'] = col_info['中位数'] = col_info['标准差'] = '-'
            col_info['最小值'] = col_info['最大值'] = col_info['第一四分位数(Q1)'] = col_info['第三四分位数(Q3)'] = '-'
            col_info['高频类别及计数'] = col_info['所有类别'] = '-'

        # 2. 数值型变量（点赞数/评论数/转发数等）
        elif np.issubdtype(df[col].dtype, np.number):
            col_info['变量类型'] = '数值型'
            col_info['均值'] = round(df[col].mean(), decimal_places) if not df[col].dropna().empty else '-'
            col_info['中位数'] = round(df[col].median(), decimal_places) if not df[col].dropna().empty else '-'
            col_info['标准差'] = round(df[col].std(), decimal_places) if not df[col].dropna().empty else '-'
            col_info['最小值'] = round(df[col].min(), decimal_places) if not df[col].dropna().empty else '-'
            col_info['最大值'] = round(df[col].max(), decimal_places) if not df[col].dropna().empty else '-'
            col_info['第一四分位数(Q1)'] = round(df[col].quantile(0.25), decimal_places) if not df[col].dropna().empty else '-'
            col_info['第三四分位数(Q3)'] = round(df[col].quantile(0.75), decimal_places) if not df[col].dropna().empty else '-'
            # 非数值字段置空
            col_info['最早时间'] = col_info['最晚时间'] = col_info['时间跨度(小时)'] = '-'
            col_info['高频类别及计数'] = col_info['所有类别'] = '-'

        # 3. 文本/分类型变量（用户名/微博内容/话题标签等）
        else:
            col_info['变量类型'] = '文本/分类型'
            # 非文本字段置空
            col_info['均值'] = col_info['中位数'] = col_info['标准差'] = '-'
            col_info['最小值'] = col_info['最大值'] = col_info['第一四分位数(Q1)'] = col_info['第三四分位数(Q3)'] = '-'
            col_info['最早时间'] = col_info['最晚时间'] = col_info['时间跨度(小时)'] = '-'
            
            # 高频类别（截断长文本，避免显示异常）
            top_categories = df[col].value_counts(dropna=True).head(categorical_top_n)
            col_info['高频类别及计数'] = ', '.join([f'{str(k)[:20]}: {v}' for k, v in top_categories.items()])
            
            # 所有类别（文本类变量重点提示唯一值数量）
            all_categories = df[col].dropna().unique()
            if len(all_categories) > 10:
                col_info['所有类别'] = f'共{len(all_categories)}个唯一值（文本过长，不展示具体内容）'
            else:
                col_info['所有类别'] = str([str(x)[:30] for x in all_categories])

        codebook_list.append(col_info)
    
    # 调整列顺序（优化可读性）
    codebook_df = pd.DataFrame(codebook_list)
    column_order = [
        '变量名', '变量类型', '原始数据类型', '样本总数', '缺失值数量', '缺失值比例(%)',
        '唯一值数量', '均值', '中位数', '标准差', '最小值', '最大值',
        '第一四分位数(Q1)', '第三四分位数(Q3)', '最早时间', '最晚时间', '时间跨度(小时)',
        '高频类别及计数', '所有类别'
    ]
    codebook_df = codebook_df[column_order]
    
    # 保存文件
    if save_path is not None:
        if save_path.endswith('.csv'):
            codebook_df.to_csv(save_path, index=False, encoding='utf-8-sig')
        elif save_path.endswith('.xlsx'):
            codebook_df.to_excel(save_path, index=False, engine='openpyxl')
        else:
            raise ValueError("仅支持.csv/.xlsx格式")
        print(f"✅ 微博数据Codebook已保存至: {save_path}")
    
    return codebook_df

# ------------------- 读取CSV文件并生成Codebook -------------------
if __name__ == "__main__":
    # 1. 读取你的CSV文件（替换为你的实际路径）
    file_path = "comments_all.csv"
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        print(f"✅ 成功读取CSV文件：{file_path}")
        print(f"📊 数据规模：共 {df.shape[0]} 行样本，{df.shape[1]} 个变量")
        print(f"📋 变量列表：{list(df.columns)}")
        
        # 提前预览发布时间列的原始格式（确认解析前的状态）
        time_cols = [col for col in df.columns if '时间' in col or 'date' in col.lower()]
        if time_cols:
            print(f"\n🔍 发现时间列：{time_cols[0]}，原始格式示例：")
            print(df[time_cols[0]].head(3).tolist())
            
    except Exception as e:
        print(f"❌ 读取CSV文件失败：{str(e)}")
        raise
    
    # 2. 生成Codebook（自动解析特殊时间格式）
    codebook = generate_weibo_codebook(
        df=df,
        categorical_top_n=3,
        save_path="微博评论数据Codebook.xlsx",
        decimal_places=1
    )
    
    # 3. 打印解析后的结果
    print("\n📄 解析时间格式后的Codebook核心结果：")
    # 只显示时间相关列的信息
    time_codebook = codebook[codebook['变量类型'] == '时间型']
    print(time_codebook[['变量名', '最早时间', '最晚时间', '时间跨度(小时)']].to_string(index=False))