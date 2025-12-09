import pandas as pd 
from dotenv import load_dotenv
import os
import re 
load_dotenv()

# def load_data(file_path):
#     try:

#     except Exception as e:
def emailTransform(email):
    match=re.search(r"_([a-zA-Z0-9+!@#$%^]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.].)" ,email)
    return match.group(1)


def extractDevice(device):
    if re.search(r'(Android)',device):
        return 'Android' 
    if re.search(r'(Mac)',device):
        return 'Mac' 
    else:
        return 'Something new'
#         print(f"Something went wrong with load step {e}")
file_path='data/customer.csv'
df= pd.read_csv(file_path)
# print(df.columns)
# print(df['username'])

df['username']=df['username'].apply(lambda x : x[-12:])
df['email']=df['email'].apply(lambda x : emailTransform(x))
df['device_version']=df['device_version'].apply(lambda x : extractDevice(x))
# print(df['device_version'].unique())
# print(df['device_version'])
dane=df.groupby(['device_version']).size()
print(dane)
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