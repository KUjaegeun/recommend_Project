#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import geopandas as gpd
build = gpd.GeoDataFrame.from_file('/home/bigdata/Desktop/build_map/AL_11_D010_20190706.shp',encoding='cp949')
filename = "/media/bigdata/Seagate/Preprocess_DB/structure_info_Data/structure_info_Datavoucher_sg_sgis_201907.csv"
result =pd.read_csv(filename)
result=result.rename(columns={"pnu":"A2"})
result['A2']=result['A2'].astype('str')
nulldata=[]
for pnu in result['A2']:
    have_pnu=build.index[(build['A2']==str(pnu))].tolist()[0]
    if have_pnu is None:
        nulldata.append(result.index[(result['A2']==pnu)].tolist()[0])
filePath='./null_data.txt'
with open(filePath,'w',encoding='UTF-8') as f:
    for data in nulldata:
        f.write(data+'\n')


# In[ ]:




