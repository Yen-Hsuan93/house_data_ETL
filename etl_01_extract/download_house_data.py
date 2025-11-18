
# class HouseDownload:
#     """內政部實價登錄資料下載模組"""

#     def __init__(self, download_dir=None, target_dir=None):
#         self.download_dir = Path(download_dir or r"C:\Users\hnchen\Downloads")
#         self.target_dir = Path(target_dir or r"C:\sideProject\house_rawdata")
#         self.driver = None

#     # ---------- 建立瀏覽器 ----------
#     def create_driver(self):
#         options = webdriver.ChromeOptions()
#         options.add_argument("--start-maximized")
#         options.add_argument("--disable-popup-blocking")
#         options.add_argument("--disable-notifications")
#         options.add_argument("--lang=zh-TW")
#         options.add_argument("--safebrowsing-disable-download-protection")
#         prefs = {
#             "download.default_directory": str(self.download_dir),
#             "download.directory_upgrade": True,
#             "safebrowsing.enabled": True,
#             "profile.default_content_setting_values.automatic_downloads": 1,
#         }
#         options.add_experimental_option("prefs", prefs)
#         self.driver = webdriver.Chrome(options=options)
#         return self

#     # ---------- 打開頁面 ----------
#     def visit(self):
#         self.driver.get("https://plvr.land.moi.gov.tw/DownloadOpenData")
#         sleep(2)
#         original_window = self.driver.current_window_handle

#         for handle in self.driver.window_handles:
#             self.driver.switch_to.window(handle)
#             if "data.gov.tw/license" in self.driver.current_url:
#                 print("發現授權頁，關閉：", self.driver.current_url)
#                 self.driver.close()

#         self.driver.switch_to.window(original_window)
#         sleep(2)
#         return self

#     # ---------- 點選條件 ----------
#     def search(self):
#         tab_button = WebDriverWait(self.driver, 10).until(
#             EC.element_to_be_clickable((By.LINK_TEXT, "非本期下載"))
#         )
#         tab_button.click()

#         seasonclick = self.driver.find_element(
#             By.CSS_SELECTOR, 'li.nav-item a.btndl.px-1.py-2.text-decoration-none.active'
#         )
#         seasonclick.click()
#         sleep(2)

#         csvclick = self.driver.find_element(
#             By.CSS_SELECTOR, 'select[name=fileFormat] option[value=csv]'
#         )
#         csvclick.click()

#         downloadclick = self.driver.find_element(By.CSS_SELECTOR, "#downloadTypeId2")
#         downloadclick.click()
#         sleep(2)

#         for city in ["臺北市", "新北市", "桃園市"]:
#             xpath = f"//tr[@class='advDownloadClass'][td[@class='form_intro']/font[text()='{city}']]//input[contains(@class, 'checkBoxGrp') and contains(@class, 'landTypeA')]"
#             self.driver.find_element(By.XPATH, xpath).click()
#         return self

#     # ---------- 下載 CSV ----------
#     def download_csv(self):
#         wait = WebDriverWait(self.driver, 10)
#         target_files = ["A_lvr_land_A.csv", "F_lvr_land_A.csv", "H_lvr_land_A.csv"]

#         for year in range(114, 115):
#             for season in range(3, 4):
#                 value = f"{year}S{season}"
#                 try:
#                     option = wait.until(
#                         EC.element_to_be_clickable(
#                             (By.CSS_SELECTOR, f'select[name="season"] > option[value="{value}"]')
#                         )
#                     )
#                     option.click()
#                     print(f"已選擇季別：{value}")

#                     download_button = wait.until(
#                         EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[type=button]'))
#                     )
#                     download_button.click()
#                     print(f"已下載：{value}")
#                     sleep(50)

#                     for filename in os.listdir(self.download_dir):
#                         if filename.endswith(".zip"):
#                             self._handle_zip(filename, target_files, value)
#                             break

#                 except Exception as e:
#                     print(f"錯誤於 {value}：{e}")
#                     continue
#         return self

#     # ---------- 處理 ZIP ----------
#     def _handle_zip(self, filename, target_files, value):
#         zip_path = self.download_dir / filename
#         try:
#             with zipfile.ZipFile(zip_path, "r") as zip_ref:
#                 for file in zip_ref.namelist():
#                     base = os.path.basename(file)
#                     if base in target_files:
#                         prefix = base[0]  # A, F, H
#                         new_name = f"{prefix}_{value}.csv"
#                         zip_ref.extract(file, self.download_dir)
#                         src_path = self.download_dir / file
#                         dest_path = self.target_dir / new_name
#                         shutil.move(src_path, dest_path)
#                         print(f"檔案已重新命名並移動為：{dest_path}")
#             os.remove(zip_path)
#             print(f"刪除原始壓縮檔：{zip_path}")
#         except Exception as e:
#             print(f"解壓或移動檔案時發生錯誤：{e}")

#     # ---------- 主流程 ----------
#     def run(self):
#         print("[HouseDownload] 開始下載實價登錄資料...")
#         self.create_driver().visit().search().download_csv()
#         print("[HouseDownload] 任務完成！")
# # ---------- main ----------
# if __name__ == "__main__":
#     downloader = HouseDownload()
#     downloader.run()

import os
import zipfile
import shutil
from time import sleep
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from datetime import date
from typing import Optional


class HouseDownload:
    #setting Chrome and path
    def __init__(self,download_path: Optional[str] = None, save_path:Optional[str] = None):
        self.download_path = download_path or Path(r"C:\Users\hnchen\Downloads")
        self.save_path = save_path or Path(r"C:\sideProject\house_rawdata")

        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--disable-notifications")
        options.add_argument("--lang=zh-TW")
        options.add_argument("--safebrowsing-disable-download-protection")
        prefs = {
            "download.default_directory": str(self.download_path),
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_setting_values.automatic_downloads": 1,
        }
        options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome(options=options)
        
    #visit page
    def visit(self):
        self.driver.get("https://plvr.land.moi.gov.tw/DownloadOpenData")
        sleep(2)

        original_window = self.driver.current_window_handle
        
        for handle in self.driver.window_handles:
            self.driver.switch_to.window(handle)
            if "data.gov.tw/license" in self.driver.current_url:
                print(f"[關閉授權頁:{self.driver.current_url}]")
                self.driver.close()
        self.driver.switch_to.window(original_window)
        sleep(2)

    #search
    def search(self):

        tab_btn = self.driver.find_element(
            By.CSS_SELECTOR,
            "li.nav-item > a.btndl.px-1.py-2[aria-controls=tab_opendata_history_content]"
        )
        tab_btn.click()
        
        
        csv_btn = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR,
                 "option[value=csv]")
                )#非本期下載btn
        )
        csv_btn.click()


        pro_btn = self.driver.find_element(
            By.CSS_SELECTOR,
            "input#downloadTypeId2"
        )
        pro_btn.click()
        sleep(2)

        taipei_btn = self.driver.find_element(
            By.CSS_SELECTOR,
            "input[value=A_lvr_land_A]"
        )
        taipei_btn.click()

        new_taipei_btn = self.driver.find_element(
            By.CSS_SELECTOR,
            "input[value=F_lvr_land_A]"
        )
        new_taipei_btn.click()

        
        taoyuan_btn = self.driver.find_element(
            By.CSS_SELECTOR,
            "input[value=H_lvr_land_A]"
        )
        taoyuan_btn.click()



    #save_csv
    def save_csv(self):
        download_btn = self.driver.find_element(
            By.CSS_SELECTOR,
            "input[name=button9]"
        )
        download_btn.click()
        sleep(50)



        target_csv = ["A_lvr_land_A.csv", "F_lvr_land_A.csv", "H_lvr_land_A.csv"]
        #find the new zip
        zip_files = list(self.download_path.glob("*.zip"))
        if not zip_files:
            print(f"[not found zips :{zip_files}]")
            return
        latest_zip = max(zip_files, key = lambda f :f.stat().st_mtime)
        print(f"[found zip:{latest_zip}]")
        self.handle_zip(latest_zip,target_csv)

    # ---------- 處理 ZIP ----------
    def handle_zip(self, zip_path, target_files):
        # year and season

        today = date.today()
        year = today.year - 1911
        season = (today.month - 1) // 3 + 1
        season -= 1 # history
        if season == 0:
            season = 4
            year -= 1

        temp_dir = self.download_path / "temp_extract"
        temp_dir.mkdir(exist_ok = True)

         
        # temp_file
        temp_dir = self.download_path / "temp_extract"
        temp_dir.mkdir(exist_ok=True)
        try:
            with zipfile.ZipFile(zip_path, "r") as zfile:
                zfile.extractall(temp_dir)
            
            for file in target_files:
                src = temp_dir / file
                if src.exists():
                    prefix = file[0]
                    new_name = f"{prefix}_{year}S{season}"
                    dest = self.save_path / new_name
        except Exception:
            pass

        try:
            with zipfile.ZipFile(zip_path, "r") as zfile:
                zfile.extractall(temp_dir)

            # new name
            for file in target_files:
                src = temp_dir / file
                if src.exists():
                    prefix = file[0]   # A / F / H
                    new_name = f"{prefix}_{year}S{season}.csv"
                    dest = self.save_path / new_name
                    shutil.move(src, dest)
                    print(f"[file new_name：{dest}]")

            # remove zip
            shutil.rmtree(temp_dir)
            zip_path.unlink()
            print("[remove ZIP and temp_file]")
            self.driver.quit()

        except Exception as e:
            print(f"[ZIP error：{e}]")

def main():
    rawdata_download = HouseDownload()
    rawdata_download.visit()
    rawdata_download.search()
    rawdata_download.save_csv()


if __name__ == "__main__":
    main()