# 檔案路徑: C:\sideProject\house_data_ETL\upload_to_sql.py

import pandas as pd
from sqlalchemy import create_engine
import os

# 1. 讀取你 ETL 跑完產生的那個 CSV 檔案
# (請確認這個檔名跟你 ETL 產出的檔名一樣)
csv_path = r'C:\sideProject\house_data_ETL\cleaned_data.csv'

# 檢查一下檔案在不在，以免報錯
if not os.path.exists(csv_path):
    print(f"錯誤：找不到檔案 {csv_path}")
    exit(1)

# 2. 讀取 CSV 資料
# (Pandas 會在這裡自動偵測欄位名稱和型態)
df = pd.read_csv(csv_path)
print(f"已讀取 CSV，共 {len(df)} 筆資料，準備上傳...")

# 3. 設定資料庫連線
# (請修改你的帳號、密碼、資料庫名稱)
db_url = 'mysql+pymysql://root:password@localhost:3306/house_price_db'
engine = create_engine(db_url)

# 4. 【這裡就是你要寫的那行】
# 這一行執行下去，因為表不存在(或被我們刪了)，Pandas 會自動幫你建立所有欄位！
try:
    df.to_sql(name='house_data', con=engine, if_exists='replace', index=False)
    print(">>> 成功！資料表已自動建立，並完成匯入。")
except Exception as e:
    print(f">>> 失敗：{e}")