import os
import pandas as pd

class ElevatorProcessing:

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()

    def infer_elevator(self):
        

        if "電梯" not in self.df.columns:
            self.df["電梯"] = pd.NA

        s = self.df["電梯"].astype(str)
        self.df.loc[s.str.contains("有", na=False), "電梯"] = 1
        self.df.loc[s.str.contains("無", na=False), "電梯"] = 0

        mask = self.df["電梯"].isna()
        if "建物型態" in self.df.columns:
            t = self.df["建物型態"].astype(str)
            no_elev = (
                t.str.contains(r"公寓", na=False)
                | t.str.contains("透天厝", na=False)
            )
            self.df.loc[mask & no_elev, "電梯"] = 0
            self.df.loc[mask & ~no_elev, "電梯"] = 1

        self.df["電梯"] = self.df["電梯"].astype("Int64")
        return self
    

def main():
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
    input_path = os.path.join(PROJECT_ROOT, "main_house_rawdata", "merged_rawdata.csv")

    df = pd.read_csv(input_path, encoding="utf-8-sig")

    elevator_process = ElevatorProcessing(df)
    elevator_process = elevator_process.infer_elevator()
    df = elevator_process.df

    cols = ["電梯"]  
    print(elevator_process.df[cols].head(5))

    for col in cols:
        print(f"\n【{col}】種類：")
        print(elevator_process.df[col].dropna().unique())

if __name__ == "__main__":
    main()
