# =========================================== [ setting ] ============================================================
import pandas as pd
import os
import glob
import re
import math
import glob

os.chdir("/home/bigdata/다운로드/recommendation_Project/Data/상권데이터/noData")
file_list = glob.glob("*.csv")

# =========================================== [ preprocessing ] ============================================================
## + [ remove columns ] ====================
structure_info_drop = ["sido_cd", "sido_nm", "sig_cd", "sig_nm", "emd_cd", "emd_nm",
"park_cnt", "ho_cnt", "apt_area_sum", "apt_area_cnt", "apt_area_avg",
"apt_price_sum", "apt_price_cnt", "apt_price_avg"]
demography_info_drop = ["in_wed_001", "in_wed_002", "in_wed_003", "in_wed_004", "in_wed_005", "in_wed_006", "in_wed_007", "in_wed_008", "in_wed_999",
"ho_ar_001", "ho_ar_002", "ho_ar_003", "ho_ar_004", "ho_ar_005", "ho_ar_006", "ho_ar_007", "ho_ar_008", "ho_ar_009", "ho_ar_999",
"ho_yr_001", "ho_yr_002", "ho_yr_003", "ho_yr_004", "ho_yr_005", "ho_yr_006", "ho_yr_007", "ho_yr_008", "ho_yr_009", "ho_yr_010", "ho_yr_011", "ho_yr_012", "ho_yr_013", "ho_yr_014", "ho_yr_999"]

## + [ Data parsing ] =======================
structure_info_Data = pd.DataFrame(None)
demography_info_Data = pd.DataFrame(None)
n = 10

chunksize = (10 ** 5)
for file in file_list[9:] :
for cnt, chunk in enumerate(pd.read_csv(file, chunksize = chunksize)):
structure_info_DB = chunk.iloc[:, 1:635]
demography_info_DB = chunk.iloc[:, 636:]

structure_info_DB = structure_info_DB.drop_duplicates()
structure_info_DB = structure_info_DB[structure_info_DB["pnu"].apply(lambda x: str(x)[:2] == "11")]
structure_info_DB.drop(structure_info_drop, axis = 1, inplace = True)

col_sort = structure_info_DB.iloc[:, structure_info_DB.columns.str.contains("^pop")].columns
structure_info_DB[col_sort] = structure_info_DB[col_sort].applymap(lambda x: math.ceil(x)) # applymay function == map_dfc in R function
structure_info_Data = structure_info_Data.append(structure_info_DB)

demography_info_DB = demography_info_DB.drop_duplicates()
demography_info_DB = demography_info_DB[demography_info_DB["adm_dr_cd"].apply(lambda x: str(x)[:2]) == "11"]
demography_info_DB.drop(demography_info_drop, axis = 1, inplace = True)
demography_info_Data = demography_info_Data.append(demography_info_DB)
print("chunk cont : ", cnt)

structure_info_Data.to_csv("../Preprocess_DB/structure_info_Data" + file)
demography_info_Data.to_csv("../Preprocess_DB/demography_info_Data" + file)

n += 1
print("================================== [ complie file" + str(n) + "] ========================================")
