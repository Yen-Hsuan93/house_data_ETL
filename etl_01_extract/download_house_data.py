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

    def __init__(self,download_path: Optional[str] = None, save_path:Optional[str] = None):
        self.download_path = download_path or Path(r"C:\Users\hnchen\Downloads")
        self.save_path = save_path or Path(r"C:\sideProject\house_rawdata")

        options = webdriver.ChromeOptions()
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")
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
        sleep(3)

        self.driver.refresh()

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

        """
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
        """

        csv_btn = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR,
                 "option[value=csv]")
                )#本期下載btn
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
        """
        download_btn = self.driver.find_element(
            By.CSS_SELECTOR,
            "input[name=button9]"
        )
        download_btn.click()
        """
        download_btn = self.driver.find_element(
            By.CSS_SELECTOR,
            "a.btn.btn-secondary"
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


    def handle_zip(self, zip_path, target_files):
        # year and season

        today = date.today()
        year = today.year - 1911
        season = (today.month - 1) // 3 + 1
        season -= 1 # history
        if season == 0:
            season = 4
            year -= 1
         
        # temp_file
        temp_dir = self.download_path / "temp_extract"
        temp_dir.mkdir(exist_ok=True)
        try:
            with zipfile.ZipFile(zip_path, "r") as zfile:
                zfile.extractall(temp_dir)

            # new name
            for file in target_files:
                src = temp_dir / file
                if src.exists():
                    prefix = file[0]   # A / F / H
                    #new_name = f"{prefix}_{year}S{season}.csv"
                    new_name = f"{prefix}_{today}.csv"
                    dest = self.save_path / new_name
                    shutil.move(src, dest)
                    print(f"[file new_name：{dest}]")


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