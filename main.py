# 這是修正後的 main.py，讓邏輯順暢且不會讀到舊資料

import os
import shutil
import pandas as pd

# 引用您的自定義模組
from etl_01_extract.download_house_data import HouseDownload
from etl_01_extract.raw_merger import RawMerger
from etl_01_extract.io_handler import IOHandler
from etl_02_transform.filter_basic import FilterBasic
from etl_02_transform.date_houseage import DateHouseAge
from etl_02_transform.material_processing import MaterialProcessing
from etl_02_transform.parking_processing import ParkingProcessing
from etl_02_transform.floor_processing import FloorProcessing
from etl_02_transform.price_final_cleaning import PriceFinalCleaning
from etl_02_transform.elevator_processing import ElevatorProcessing
from etl_02_transform.lat_lng_processing import LatLngUpdate
from etl_02_transform.MRT_distance import MrtDistance
from etl_03_load.save_handler import MainDataLoader

def check_file_has_data(filepath):
    """檢查檔案是否存在且有資料 (不包含 header)"""
    if not os.path.exists(filepath):
        return False
    try:
        df = pd.read_csv(filepath, nrows=1)
        return not df.empty
    except:
        return False

def main():
    # ==========================================
    # 1. 路徑設定 (Parent Directory 修正版)
    # ==========================================
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)  # 指向上一層 C:\sideProject

    MASTER_DATA_PATH = os.path.join(PROJECT_ROOT, "cleaning_house_rawdata", "cleaning_main_data.csv")
    TEMP_OUTPUT_PATH = os.path.join(PROJECT_ROOT, "cleaning_house_rawdata", "main_data_updated.csv")
    RAW_FOLDER = os.path.join(PROJECT_ROOT, "house_rawdata")
    MERGED_RAW_PATH = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_rawdata.csv")
    MERGED_CLEANED_PATH = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_cleaned.csv")
    MRT_LOCATION_PATH = os.path.join(CURRENT_DIR, "mrt_location.csv")

    MASTER_EXISTS = os.path.exists(MASTER_DATA_PATH)

    print(f"專案根目錄: {PROJECT_ROOT}")
    print(f"主資料檔: {MASTER_DATA_PATH}")
    print(f"原始檔下載區: {RAW_FOLDER}")

    # ==========================================
    # 2. Extract: 下載最新資料
    # ==========================================
    print("\n[Step 1] 開始下載資料...")
    try:
        rawdata_download = HouseDownload()
        rawdata_download.visit()
        rawdata_download.search()
        rawdata_download.save_csv()
    except Exception as e:
        print(f"下載過程發生錯誤: {e}")

    # ==========================================
    # 3. Merge: 智慧合併判斷 (讓 RawMerger 負責清除舊中繼檔)
    # ==========================================
    print("\n[Step 2] 檢查新檔案並合併...")
    
    if not os.path.exists(RAW_FOLDER):
        print(f"錯誤: 找不到原始資料夾 {RAW_FOLDER}")
        return

    # 執行 RawMerger，它內部會先刪除舊的 merged_rawdata.csv，然後比對 Master，寫入新資料。
    raw_merger = RawMerger(
        raw_folder=RAW_FOLDER,
        merged_path=MERGED_RAW_PATH,
        master_path=MASTER_DATA_PATH  # 傳入 Master Data 進行比對
    )
    merged_path = raw_merger.merge()

    # 檢查是否產生資料 (這才是 Step 3 該讀取的內容)
    if not check_file_has_data(merged_path):
        print("最終檢查：沒有產生任何新資料，程式結束 (無需 ETL)。")
        return

    # ==========================================
    # 4. Transform: ETL 清整流程
    # ==========================================
    print(f"\n[Step 3] 開始 ETL 清整...")
    
    os.makedirs(os.path.dirname(MERGED_CLEANED_PATH), exist_ok=True)

    # 這裡的 io.load() 現在會讀取一個乾淨的 merged_rawdata.csv (只包含本次新資料)
    io = IOHandler(input_path=merged_path, output_path=MERGED_CLEANED_PATH)
    df = io.load()
    print(f"  -> 載入資料筆數: {len(df)}") # 這裡應該是 A/F/H 的實際筆數

    # --- 各項清洗邏輯 ---
    print("  -> 執行基礎過濾...")
    filter_basic = FilterBasic(df)
    filter_basic.remove_pua_chars_from_address()
    filter_basic.drop_duplicates_by_id()
    filter_basic.unify_columns()
    filter_basic.filter_out_transaction_targets()
    filter_basic.keep_residential_usage()
    filter_basic.keep_urban_zone_residential()
    filter_basic.filter_out_non_urban_zones()
    filter_basic.remove_notes_with_keywords()
    filter_basic.cleaning_house_type()
    filter_basic.add_city_from_source()
    df = filter_basic.df

    print("  -> 計算屋齡...")
    house_age = DateHouseAge(df)
    house_age.parse_dates()
    house_age.calculate_house_age()
    house_age.drop_abnormal_houseage()
    df = house_age.df

    print("  -> 處理車位資訊...")
    parking_process = ParkingProcessing(df)
    parking_process.process_parking()
    parking_process.impute_parking_type()
    parking_process.impute_parking_price_rf()
    parking_process.calculate_parking_price_per_ping()
    df = parking_process.df

    print("  -> 填補主要建材...")
    material_process = MaterialProcessing(df)
    material_process.impute_main_material()
    df = material_process.df

    print("  -> 處理樓層資訊...")
    floor_process = FloorProcessing(df)
    floor_process.total_floor()
    floor_process.count_transfer_floors()
    floor_process.extract_highest_floor()
    df = floor_process.df

    print("  -> 計算單價與清除缺失值...")
    price_clean = PriceFinalCleaning(df)
    price_clean.price_ping()
    price_clean.drop_missing_core_fields()
    df = price_clean.df

    print("  -> 推論電梯...")
    elevator_process = ElevatorProcessing(df)
    elevator_process.infer_elevator()
    df = elevator_process.df

    print("  -> 補經緯度...")
    lat_lng_master_path = MASTER_DATA_PATH if MASTER_EXISTS else None
    
    try:
        lat_lng_update = LatLngUpdate(df, main_data_path=lat_lng_master_path)
        lat_lng_update.visit()
        lat_lng_update.update_lat_lng()
        lat_lng_update.quit()
        df = lat_lng_update.df
    except Exception as e:
        print(f"經緯度更新發生例外: {e}")

    print("  -> 計算捷運距離...")
    if os.path.exists(MRT_LOCATION_PATH):
        mrt_distance = MrtDistance(df, mrt_path=MRT_LOCATION_PATH)
        mrt_distance.calculate_distance_to_mrt()
        mrt_distance.process_mrt_name_and_grade()
        df = mrt_distance.df
    else:
        print(f"警告: 找不到捷運座標檔，跳過。")

    io.save(df)
    print(f"ETL 清整完成。")

    # ==========================================
    # 5. Load: 寫入/更新 主資料庫
    # ==========================================
    print("\n[Step 4] 寫入主資料庫...")

    os.makedirs(os.path.dirname(MASTER_DATA_PATH), exist_ok=True)

    if MASTER_EXISTS:
        print("  -> 模式: Append (合併至現有主檔)")
        try:
            loader = MainDataLoader(
                main_data_path=MASTER_DATA_PATH,
                new_data_path=MERGED_CLEANED_PATH,
                output_path=TEMP_OUTPUT_PATH
            )
            loader.load()
            
            print("  -> 執行檔案置換...")
            if os.path.exists(TEMP_OUTPUT_PATH):
                shutil.copy(MASTER_DATA_PATH, MASTER_DATA_PATH + ".bak")
                os.remove(MASTER_DATA_PATH)
                os.rename(TEMP_OUTPUT_PATH, MASTER_DATA_PATH)
                print(f"  主資料庫更新成功: {MASTER_DATA_PATH}")
            else:
                print("錯誤：找不到合併後的暫存檔。")
        except Exception as e:
            print(f"Append 過程失敗: {e}")
    else:
        print("  -> 模式: Initialization (建立新主檔)")
        try:
            shutil.copy(MERGED_CLEANED_PATH, MASTER_DATA_PATH)
            print(f"已成功建立主資料庫: {MASTER_DATA_PATH}")
        except Exception as e:
            print(f"建立主檔失敗: {e}")

    # ==========================================
    # 6. Cleanup
    # ==========================================
    print("\n[Step 5] 清理暫存檔案...")
    for temp_file in [MERGED_RAW_PATH, MERGED_CLEANED_PATH]:
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
                print(f"  - 已刪除: {os.path.basename(temp_file)}")
            except:
                pass

    print("全部流程執行完畢！")

if __name__ == "__main__":
    main()