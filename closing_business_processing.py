import pandas as pd
import os
import glob
import multiprocessing
import geopandas as gpd
def processing(file):
    structure_data = pd.read_csv(file)
    store_result = []
    for index,pnu in enumerate(structure_data['pnu']):
        if area[area["A1"].isin([str(pnu)])].empty:
            store_result.append(0)
        else :
            store_result.append(1)
    structure_data = structure_data.assign(closing_of_business=store_result)
    structure_data.to_csv("../Preprocess_DB/structure_info_Data/" + file)

os.chdir("/media/bigdata/New_Volumns/structure_info_Data")
file_list = glob.glob("*.csv")
global area
area = gpd.GeoDataFrame.from_file('/home/bigdata/Desktop/AL_11_D002_20211002/AL_11_D002_20211002.shp',encoding='cp949')
pool = multiprocessing.Pool(processes=4)
pool.map(processing, file_list)
pool.close()
pool.join()
