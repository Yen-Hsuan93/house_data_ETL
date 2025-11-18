import pandas as pd
from typing import Optional

class IOHandler:
    def __init__(self, 
                 input_path: Optional[str] = None, 
                 output_path: Optional[str] = None,
                 encoding: str =  'utf-8'):
        self.input_path = input_path
        self.output_path = output_path
        self.encoding = encoding
    def load(self):
        if not self.input_path:
            raise ValueError("未指定輸入路徑")
        try:
            df = pd.read_csv(self.input_path, encoding =self.encoding,  sep = ",")
            print("使用utf-8編碼傳入")
            return df
            
        except UnicodeDecodeError:
            df = pd.read_csv(self.input_path, encoding = "big5", sep = "\t")
            print("使用big5編碼傳入")
            return df
    def save(self, df):
        if not self.output_path:
            raise ValueError("未指定輸出路徑")
        df.to_csv(self.output_path, encoding = "utf-8", index = False, sep = ",")
        print(f"以utf-8儲存:{self.output_path}")

def main():
    io = IOHandler("input_data.csv", "output_data.csv")
    df = io.load()
    io.save(df)


if __name__ == "__main__":
    main()




import pandas as pd
from typing import Optional

class IOHandler:
    def __init__(self, 
                 input_path : Optional[str] = None,
                 output_path : Optional[str] = None,
                 encoding : str = "utf-8"
                 ):
        self.input_path = input_path
        self.output_path = output_path
        self.encoding = encoding
    def load(self):
        if not self.input_path:
            raise ValueError(f"{self.input_path}:路徑未提供")
        try:
            df = pd.read_csv(self.input_path, encoding =self.encoding, sep = ",")
            print("使用utf-8儲存")
            return df
        except UnicodeDecodeError:
            df = pd.read_csv(self.input_path, encoding = "big5", sep = "\t")
            print("使用big5儲存")
            return df
        
    def save(self,df):
        if not self.output_path:
            raise ValueError("未提供輸出路徑")
        df.to_csv(self.output_path,encoding = self.encoding, index = False, sep = ",")
        print(f"已utf-8儲存到{self.output_path}")
    
def main():
    io = IOHandler(
        input_path=r"C:\sideProject\main_house_rawdata\merged_rawdata.csv",
        output_path=r"C:\sideProject\main_house_rawdata\merged_cleaned.csv"
    )
    df = io.load()
    io.save(df)

if __name__ == "__main__":
    main()