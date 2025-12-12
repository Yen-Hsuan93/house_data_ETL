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
import os
import shutil

def main():

   
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

    MASTER_DATA_PATH = os.path.join(PROJECT_ROOT, "cleaning_house_rawdata", "cleaning_main_data.csv")
    TEMP_OUTPUT_PATH = os.path.join(PROJECT_ROOT, "cleaning_house_rawdata", "main_data_updated2.csv")
    
    RAW_FOLDER = os.path.join(PROJECT_ROOT, "house_rawdata")
    MERGED_RAW_PATH = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_rawdata.csv")
    MERGED_CLEANED_PATH = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_cleaned.csv")
    MRT_LOCATION_PATH = os.path.join(CURRENT_DIR, "mrt_location.csv")



    # 下載最新資料
    rawdata_download = HouseDownload()
    rawdata_download.visit()
    rawdata_download.search()
    rawdata_download.save_csv()

    # 合併 rawdata
    raw_merger = RawMerger(
        raw_folder=RAW_FOLDER,
        merged_path=MERGED_RAW_PATH
    )
    merged_path = raw_merger.merge()

    # ETL 清整
    io = IOHandler(
        input_path=merged_path,
        output_path=MERGED_CLEANED_PATH
    )
    df = io.load()

    # --- 資料清洗 ---
    filter_basic = FilterBasic(df)
    filter_basic = filter_basic.drop_duplicates_by_id()
    filter_basic = filter_basic.unify_columns()
    filter_basic = filter_basic.filter_out_transaction_targets()
    filter_basic = filter_basic.keep_residential_usage()
    filter_basic = filter_basic.keep_urban_zone_residential()
    filter_basic = filter_basic.filter_out_non_urban_zones()
    filter_basic = filter_basic.remove_notes_with_keywords()
    filter_basic = filter_basic.cleaning_house_type()
    filter_basic = filter_basic.add_city_from_source()
    df = filter_basic.df

    house_age = DateHouseAge(df)
    house_age = house_age.parse_dates()
    house_age = house_age.calculate_house_age()
    df = house_age.df

    parking_Process = ParkingProcessing(df)
    parking_Process = parking_Process.process_parking()
    parking_Process = parking_Process.impute_parking_type()
    parking_Process = parking_Process.impute_parking_price_rf()
    parking_Process = parking_Process.calculate_parking_price_per_ping()
    df = parking_Process.df

    material_process = MaterialProcessing(df)
    material_process = material_process.impute_main_material()
    df = material_process.df

    floor_Process = FloorProcessing(df)
    floor_Process = floor_Process.total_floor()
    floor_Process = floor_Process.count_transfer_floors()
    floor_Process = floor_Process.extract_highest_floor()
    df = floor_Process.df

    price_clean = PriceFinalCleaning(df)
    price_clean = price_clean.price_ping()
    price_clean = price_clean.drop_missing_core_fields()
    df = price_clean.df

    elevator_process = ElevatorProcessing(df)
    elevator_process = elevator_process.infer_elevator()
    df = elevator_process.df

    lat_lng_update = LatLngUpdate(df, main_data_path=MASTER_DATA_PATH)
    lat_lng_update = lat_lng_update.visit()
    lat_lng_update = lat_lng_update.update_lat_lng()
    lat_lng_update = lat_lng_update.quit()
    df = lat_lng_update.df

    mrtDistance = MrtDistance(df, mrt_path=MRT_LOCATION_PATH)
    mrtDistance = mrtDistance.calculate_distance_to_mrt()
    mrtDistance = mrtDistance.process_mrt_name_and_grade()
    df = mrtDistance.df

    io.save(df)
    print(" ETL 清整完成，整併資料中")

    # Step 4: 清整後資料合併
    loader = MainDataLoader(
        main_data_path=MASTER_DATA_PATH,
        new_data_path=MERGED_CLEANED_PATH,
        output_path=TEMP_OUTPUT_PATH      
    )
    loader.load()

    # Step 5: 備份與更新
    print("備份中...")

    if os.path.exists(TEMP_OUTPUT_PATH):
        
        if os.path.exists(MASTER_DATA_PATH):
            try:
                shutil.copy(MASTER_DATA_PATH, MASTER_DATA_PATH + ".bak")
            except Exception as e:
                print(f"備份失敗 (不影響更新): {e}")

        try:
            if os.path.exists(MASTER_DATA_PATH):
                os.remove(MASTER_DATA_PATH)
            
            os.rename(TEMP_OUTPUT_PATH, MASTER_DATA_PATH)
            print("備份及更新完成！")
        except Exception as e:
            print(f"更新失敗: {e}")
    
    else:
        print("錯誤：找不到更新檔")

    print("ETL完成")

if __name__ == "__main__":
    main()