import pandas as pd
from dotenv import load_dotenv
import os
import re
load_dotenv()

class Product:
    def __init__(self,file_path):
        self.file_path=file_path
        self.df=None; 

    def load_file(self):
        try:
            self.df=pd.read_csv(self.file_path,on_bad_lines='skip')
            return self.df
        except Exception as e:
            print(e)
    
    def transform_file(self):
        try:
            self.df['year']=self.df['year'].apply(pd.to_numeric)
            self.df['year'].fillna(0, inplace=True)
            self.df['year']=self.df['year'].astype(int)
        # print(df['year'].dtype)
            self.df['gender']=self.df['gender'].apply(lambda x : x[:1])
            return self.df
        except Exception as e :
            print(e)    

# print(df[df['year'].isna()==True])