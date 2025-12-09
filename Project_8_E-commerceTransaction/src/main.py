import pandas as pd 
from dotenv import load_dotenv
import os
import re 
load_dotenv()

class Customer:
    def __init__(self,file_path):
        self.file_path=file_path
        self.df= None

    def load_data(self):
        try:
            self.df=pd.read_csv(self.file_path)
            return self.df
        except Exception as e:
            print(f"Something went wrong with load step {e}")

    def emailTransform(self, email):
        match=re.search(r"_([a-zA-Z0-9+!@#$%^]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.].)" ,email)
        return match.group(1)

    def extractDevice(self, device):
        if re.search(r'(Android)',device):
            return 'Android' 
        if re.search(r'(Mac)',device):
            return 'Mac' 
        else:
            return 'Something new'

    def processData(self):
        self.df['username']=self.df['username'].apply(lambda x : x[-12:])
        self.df['email']=self.df['email'].apply(lambda x : self.emailTransform(x))
        self.df['device_version']=self.df['device_version'].apply(lambda x : self.extractDevice(x))
        return self.df

cust = Customer(file_path='data/customer.csv')
cust.load_data()
dfcust = cust.processData()
print(dfcust.head())
# print(cust.df.head())



#         print(f"Something went wrong with load step {e}")
# print(df.columns)
# print(df['username'])

# print(df['device_version'].unique())
# print(df['device_version'])
# print(df)

     
# str='f_c286a1176f7e@startupca'
# str1 = 'Android 4.0.1'
# str2 = 'iPhone; CPU iPhone OS 14_2_1 like Mac OS X'
# print(extractDevice(str1))

# m=re.search(r"_([a-zA-Z0-9_+!@#$%^]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.].)" ,str)
# print(m.group(1))
# str='671a0865-ac4e-4dc4-9c4f-c286a1176f7e'

# reheks =str[-12:]
# print(reheks)