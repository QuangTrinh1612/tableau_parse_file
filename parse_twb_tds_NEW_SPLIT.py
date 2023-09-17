import os, sys
import pandas as pd
import numpy as np
import itertools
from bs4 import BeautifulSoup
import parse_support_NEW as ps
import re

# 09042021
# Additional Sheet for List of Tables
# 14042021 - Mapped Formula Column to Caption Instead of Column ID

#STEP 1: given the folder list all subfolder
# folder = "C:/Users/ext.bui.toan/python_projects/migrate_data_analytics/DashboardMig21"
# folder = "C:/Users/ext.ike.suharyanti/source/repos/PythonTableau/PythonTableau/Workbook/DashboardMig21/"
# outFileName = "C:/Users/ext.ike.suharyanti/source/repos/PythonTableau/PythonTableau/Workbook/Output/Report-Tableau-ALL-Data-1.xlsx"
# subFolder =  os.listdir(folder)
# subFolder = [os.path.join(folder, f) for f in subFolder]
# subFolder = [f for f in subFolder if os.path.isdir(f)]

folder = "C:/Users/ext.trinh.quang/Desktop/Desktop/Python repos/metadata_parse_file/Dashboards/" + sys.argv[1]
outFileName = "C:/Users/ext.trinh.quang/Desktop/Desktop/Python repos/metadata_parse_file/Outputs/Report-Tableau-ALL-" + sys.argv[1] + ".xlsx"
# subFolder =  os.listdir(folder)
# # subFolder = [os.path.join(folder, f) for f in subFolder]
# subFolder = [os.path.join(folder, f) for f in subFolder if os.path.isdir(os.path.join(folder, f))]
subFolder = [os.path.join(folder)]

##############################################################################################
#given you have a list of directories, you want to go to each of folder and unzip those file
# for folder in subFolder:
#     files = os.listdir(folder)
#     zippedFiles = [f for f in files if len(re.findall("twbx|tdsx", f)) > 0]
#     zippedPaths = [os.path.join(folder, f) for f in zippedFiles]
#     #unzip zip files.
#     for p in zippedPaths:
#         zip_ref = zipfile.ZipFile(p)
#         zip_ref.extractall(folder) # extract file to dir
#         zip_ref.close()

tableList = []
columnListOne = []
columnListTwo = []
binList = []
sheetList = []
missingTds = []
columnParam =[]
for f in subFolder:
    #STEP 2: get workbook and subFolder
    workBooks = [sub for sub in os.listdir(f) if sub.endswith(".twb")]
    wbPaths = [os.path.join(f, sub) for sub in workBooks]
    print(wbPaths)
    #STEP 3: list all datasources in workbooks.
    for i in range(len(workBooks)):
        path = wbPaths[i]
        workBook = workBooks[i]
        infile = open(path,encoding="utf-8")
        contents = infile.read()
        soup = BeautifulSoup(contents,'html.parser')

        datasources1st= soup.find("datasources")
        datasources = datasources1st.find_all("datasource")


        # print("path ", path)
        sourceNames = [d.get("caption") for d in datasources if d.find("connection") is not None]
        sourceTypes = [d.find("connection").get("class") for d in datasources if d.find("connection") is not None]
        sourceTypes = list(itertools.chain(*sourceTypes))
        # print("sourcename ",sourceNames)
        for sourceName, sourceType in zip(sourceNames, sourceTypes):
            count = 0
            print(sourceName,"-",sourceType)
            if sourceType == "sqlproxy":
                tdsPath = os.path.join(f, (sourceName + ".tds"))
                #check is the path exists:
                if os.path.exists(tdsPath):
                    infile = open(tdsPath,encoding="utf-8")
                    contents = infile.read()
                    soup = BeautifulSoup(contents,'html.parser')

                    tableData = ps.find_tables(soup=soup, folderPath=f, wbPath=workBook,tdsPath=sourceName)
                    tableList.append(tableData)
                    columnDataOne = ps.parse_column_tag(soup=soup, folderPath=f, wbPath=workBook,tdsPath=sourceName)

                    soupWb = datasources1st.find("datasource",attrs={"caption":sourceName})
                    columnDataOneWb = ps.parse_column_tag(soup=soupWb, folderPath=f, wbPath=workBook,tdsPath=sourceName)

                    # loc[columnDataTwo["physical_table"] != "[Extract]"]
                    # print(columnDataOne.loc[columnDataOne['column_type'] == "DERIVED"])

                    # 14042021 - Mapped Formula Column to Caption 
                    # Listed Column Fields
                    # ColumnDataCombined = columnDataOne.append(columnDataOneWb,ignore_index=True)
                    # gColumnDataCombined = getFormulaCaption()
                    # columnListOne.append()
                    columnListOne.append(columnDataOne)
                    columnListOne.append(columnDataOneWb)

                    # Listed Metadata Records
                    columnDataTwo = ps.parse_metadata_record_tag(soup=soup, folderPath=f, wbPath=workBook,tdsPath=sourceName)
                    columnListTwo.append(columnDataTwo)
                    binData = ps.parse_bin(soup=soup, folderPath=f, wbPath=workBook,tdsPath=sourceName)
                    binList.append(binData)
                    #tableData.to_csv((n + ".csv"))
                else: 
                    print("the tds path which do not exist: ", tdsPath)
                    missingTds.append(tdsPath)
                    soup = datasources1st.find("datasource",attrs={"caption":sourceName})
                    tableData = ps.find_tables_from_twb(soup=soup, folderPath=f, wbPath=workBook,tdsSrcName = sourceName,is_published=True)       
                    tableList.append(tableData)
                    #print(tableData)
                    columnDataOne = ps.parse_column_tag_twb(soup=soup, folderPath=f, wbPath=workBook,tdsSrcName = sourceName)
                    # print(columnDataOne)
                    columnListOne.append(columnDataOne)
                    columnDataTwo = ps.parse_metadata_record_tag_twb(soup=soup, folderPath=f, wbPath=workBook,tdsSrcName=sourceName,is_published=True)
                    columnListTwo.append(columnDataTwo)
                    binData = ps.parse_bin_twb(soup=soup, folderPath=f, wbPath=workBook,tdsSrcName = sourceName)
                    binList.append(binData)
            else: 
                #the wbPath is always exists
                print("workbook without data source: ", path)
                print("source name: ", sourceName)
                #alwasy parse twb files
                soup = datasources1st.find("datasource",attrs={"caption":sourceName})
                tableData = ps.find_tables_from_twb(soup=soup, folderPath=f, wbPath=workBook,tdsSrcName = sourceName,is_published=False)       
                tableList.append(tableData)
                columnDataOne = ps.parse_column_tag_twb(soup=soup, folderPath=f, wbPath=workBook,tdsSrcName = sourceName)
                columnListOne.append(columnDataOne)
                columnDataTwo = ps.parse_metadata_record_tag_twb(soup=soup, folderPath=f, wbPath=workBook,tdsSrcName=sourceName,is_published=False)
                columnListTwo.append(columnDataTwo)
                binData = ps.parse_bin_twb(soup=soup, folderPath=f, wbPath=workBook,tdsSrcName = sourceName)
                binList.append(binData)
            
            

        
        # Get information on Worksheet
        # - 26032021 : We need to extract column details of <role,datatype,type>
        infile = open(path,encoding="utf-8")
        contents = infile.read()
        soup = BeautifulSoup(contents,'html.parser') 
        workSheetData = ps.parse_sheet(soup=soup, folderPath=f, wbPath=workBook)
        sheetList.append(workSheetData)

        #append parameter data with current table data
        # print("HERE  ",datasources1st.find("datasource",attrs={"name":'Parameters'}))
        if datasources1st.find("datasource",attrs={"name":"Parameters"}) is not None:
            tableList.append(ps.find_parameter(folderPath=f, wbPath=workBook))
            soup = datasources1st.find("datasource",attrs={"name":"Parameters"})
            paramColumnData = ps.parse_param_tag(soup, folderPath=f, wbPath=workBook,tdsSrcName = "Parameters")
            columnParam.append(paramColumnData)

#this is parent folder level (include all projects folders)
tableData = pd.concat(tableList)
columnData = pd.concat(columnListOne)
columnData.drop_duplicates(subset=["folderName", "twb", "tds", "column_id"], keep="last", inplace=True)
# columnListTwo["column_id"]=columnListTwo["column_id"].astype("str").replace("[","").str.replace("]","")
columnDataTwo =  pd.concat(columnListTwo)
columnDataTwo = columnDataTwo.loc[columnDataTwo["physical_table"] != "[Extract]"]
columnDataTwo = columnDataTwo[["folderName", "twb", "tds", "tableau_field_name", "physical_table", "column_id"]]
columnDataTwo["physical_table_column"] = columnDataTwo["column_id"]
columnDataTwo.drop_duplicates(subset=["folderName", "twb", "tds", "column_id"], inplace=True)
binData = pd.concat(binList)
sheetData = pd.concat(sheetList)

#special handling for NA paramdata
if len(columnParam) == 0:
    paramData  = pd.DataFrame() 
    paramData["folderName"]= ""
    paramData["twb"] = ""
else: 
    paramData = pd.concat(columnParam)
    paramData["folderName"] = paramData["folderName"].apply(lambda x: x.split("\\")[-1].split("/")[-1])
    paramData["twb"] = paramData["twb"].apply(lambda x: x.split(".twb")[0] if x is not None else None)

#write data frame to excel file
#clean dataframe before write it to excel file.
tableData["folderName"] = tableData["folderName"].apply(lambda x: x.split("\\")[-1].split("/")[-1])
tableData["twb"] = tableData["twb"].apply(lambda x: x.split(".twb")[0] if x is not None else None)
#tableData["tds"] = tableData["tds"].apply(lambda x: x.split(".")[0] if x is not None else None)
tableData["tableName"] = tableData["tableName"].astype("str").str.replace("[","").str.replace("]","")
tableData["queries"] = tableData["queries"].str[:31000]
tableData["is_query_too_long"] = tableData["queries"].apply(lambda x: "YES" if len(str(x)) == 31000 else "NO")
#clean columnData
columnData["folderName"] = columnData["folderName"].apply(lambda x: str(x).split("\\")[-1].split("/")[-1])
columnData["twb"] = columnData["twb"].apply(lambda x: x.split(".twb")[0] if x is not None else None)
#columnData["tds"] = columnData["tds"].apply(lambda x: x.split(".")[0] if x is not None else None)
columnData["column_id"] = columnData["column_id"].str.replace("[","").str.replace("]","")
columnData["tableau_field_name"] = columnData.apply(lambda x: x["tableau_field_name"] if x["tableau_field_name"] is not None else x["column_id"], axis=1)
columnData["formulation_class"] = columnData["formulation_class"].astype("str")
columnData["formulation_class"] = columnData["formulation_class"].str.replace("[","").str.replace("]","").str.replace("'","")
columnData = columnData.loc[~columnData["column_id"].isin([":Measure Names", "Number of Records"]),:]
#clean columnDataTwo
columnDataTwo["folderName"] = columnDataTwo["folderName"].apply(lambda x: str(x).split("\\")[-1].split("/")[-1])
columnDataTwo["twb"] = columnDataTwo["twb"].apply(lambda x: x.split(".twb")[0] if x is not None else None)
#columnDataTwo["tds"] = columnDataTwo["tds"].apply(lambda x: x.split(".")[0] if x is not None else None)
columnDataTwo["column_id"] = columnDataTwo["column_id"].str.replace("[","").str.replace("]","")
columnDataTwo["tableau_field_name"] = columnDataTwo.apply(lambda x: x["tableau_field_name"] if x["tableau_field_name"] is not None else x["column_id"], axis=1)
columnDataTwo["physical_table"] = columnDataTwo["physical_table"].fillna("")
columnDataTwo["physical_table"]  = columnDataTwo["physical_table"].str.replace("[","").str.replace("]","")
columnDataTwo["physical_table_column"] = columnDataTwo["physical_table_column"].astype("str").str.replace("[","").str.replace("]","")
columnDataTwo = columnDataTwo.loc[~columnDataTwo["column_id"].isin([":Measure Names", "Number of Records"]),:]
#out join columnData with columnDataTwo
# print("Metadata : ",columnData)
# print("SELECT :",columnData.loc[columnData['column_id'] == "aov"])
columnData = pd.merge(columnData, columnDataTwo, on=["folderName", "twb", "tds", "column_id"], how="outer")

# print("Metadata : ",columnData)
columnData["tableau_field_name"] = columnData.apply(lambda x: x["tableau_field_name_x"] if x["tableau_field_name_x"] is not None else x["tableau_field_name_y"], axis=1)
columnData.drop(columns=["tableau_field_name_x", "tableau_field_name_y"], inplace=True)
columns = ["folderName", "twb", "tds", "tableau_field_name", "column_id", "role", "datatype", "type", "column_type", 
"formulation_class", "formula", "physical_table", "physical_table_column"]
columnData = columnData[columns]
columnData.drop_duplicates(subset=["folderName", "twb", "tds", "column_id"], inplace=True)
columnData["tableau_field_name"] = columnData.apply(lambda x: x["tableau_field_name"] if pd.notnull(x["tableau_field_name"]) else x["column_id"], axis=1)
columnData["column_type"] = columnData.apply(lambda x: ps.assign_column_type(x), axis=1)
columnData.rename(columns={"physical_table": "tableRefName"}, inplace=True)
#change columnData["physical_table"] to the real physical name instead of refname.
# tableRefData = tableData[["twb", "tds", "tableName", "tableRefName"]].copy()
# tableRefData.rename(columns={"tableRefName": "physical_table"}, inplace=True)

"""baseTest = columnData["column_type"] == "BASE"
noneTest = columnData["physical_table"].isnull()
testResult = np.logical_and(baseTest, noneTest)
columnData = columnData.loc[np.logical_not(testResult), :]
"""
# columnData["physical_table"] = columnData["tds"]
# columnData.to_csv("col_test.csv")
# columnData = pd.merge(columnData, tableRefData, on=["twb", "tds", "physical_table"], how="left")
# columnData["physical_table"] = columnData["tableName"]
# columnData.drop(columns=["tableName"], inplace=True)
#clean binData
binData["folderName"] = binData["folderName"].apply(lambda x: x.split("\\")[-1].split("/")[-1])
binData["twb"] = binData["twb"].apply(lambda x: x.split(".")[0] if x is not None else None)
#binData["tds"] = binData["tds"].apply(lambda x: x.split(".")[0] if x is not None else None)
binData["columnName"] = binData["columnName"].astype("str").str.replace("[","").str.replace("]","")
binData["binClass"] = binData["binClass"].astype("str").str.replace("[","").str.replace("]","").str.replace("'","")
binData["binFormula"]  = binData["binFormula"].astype("str").str.replace("[","").str.replace("]","")

#clean sheetData
sheetData["FolderName"] = sheetData["FolderName"].apply(lambda x: x.split("\\")[-1].split("/")[-1])
sheetData["twb"] = sheetData["twb"].apply(lambda x: x.split(".")[0])
sheetData["workSheet"] = sheetData["workSheet"].apply(lambda x: x.split(".")[0])
sheetData["dataSource"] = sheetData["dataSource"].astype("str").str.replace("[","").str.replace("]","")
sheetData["tableau_field_name"] = sheetData["tableau_field_name"].astype("str").str.replace("[","").str.replace("]","")
#append data
columnData = columnData.append(paramData)
columnData["is_available"] = columnData.apply(lambda x: ", ".join(sheetData["workSheet"][(sheetData["dataSource"] == x["tds"]) & (sheetData["tableau_field_name"] == x["tableau_field_name"])]), axis=1)

# 26032021 : To get available information on column details specific on metadata column sourceconnectionTag.get("class")connectionTag.get("class")
# 1. Prepare the Sheet Mapping Data
sheetRefData = sheetData[["FolderName","twb", "dataSource", "tableau_field_name","roles","dataTypes","types"]].copy()
sheetRefData.drop_duplicates(subset=["FolderName","twb", "dataSource", "tableau_field_name"], keep="last", inplace=True)
sheetRefData.rename(columns={"FolderName": "folderName","dataSource":"tds"}, inplace=True)
print("SHEET REF DATA",sheetRefData.loc[sheetRefData["tds"]=="Retention Info"])
# 2. Merging with Column Data
columnData = pd.merge(columnData, sheetRefData, on=["folderName", "twb", "tds", "tableau_field_name"], how="left")
columnData["role"] = columnData.apply(lambda x: x["role"] if x["role"] is not None or x["role"] !='' or x["role"]=="NaN" else x["roles"], axis=1)
columnData["datatype"] = columnData.apply(lambda x: x["datatype"] if x["datatype"] is not None or x["datatype"] !='' else x["dataTypes"], axis=1)
columnData["type"] = columnData.apply(lambda x: x["type"] if x["type"] is not None or x["type"] != '' else x["types"], axis=1)
print("COLUMN AFter DATA",columnData.loc[columnData["tds"]=="Retention Info",["folderName", "tableau_field_name", "column_id", "role","roles", "datatype","dataTypes", "type","types", "column_type"]])
columns = ["folderName", "twb", "tds", "tableau_field_name", "column_id", "role", "datatype", "type", "column_type", 
"formulation_class", "formula", "tableRefName", "physical_table_column","is_available"]
columnData = columnData[columns]
# Sheet Mapping Data Selecting Columns
columns = ["FolderName", "twb", "workSheet", "dataSource", "tableau_field_name"]
sheetData = sheetData[columns]

# 09042021 : Add new additional Sheet for Listing Tables
listTablesData = tableData.loc[tableData["tableTypes"].isin(["table", "text"]),["folderName","twb","tds","tableName","tableRefName","listTables"]]
listTablesDataLatest = listTablesData.assign(listTables=listTablesData['listTables']).explode('listTables')
listTablesDataLatest["listTables"]=listTablesDataLatest["listTables"].astype("str").str.replace("[","").str.replace("]","")
tableData = tableData.drop('listTables', 1)

with pd.ExcelWriter(outFileName) as writer:
    tableData.sort_values(['folderName','twb','tds'],ascending=[True,True,False]).to_excel(writer, sheet_name='table', index=False)
    columnData.sort_values(['folderName','twb','tds','column_type','tableau_field_name'],ascending=[True,True,False,True,True]).to_excel(writer, sheet_name="column", index=False)
    binData.sort_values(['folderName','twb','tds'],ascending=[True,True,False]).to_excel(writer, sheet_name="bin", index=False)
    sheetData.sort_values(['FolderName','twb','dataSource'],ascending=[True,True,False]).to_excel(writer, sheet_name="mapping", index=False)
    listTablesDataLatest.sort_values(['folderName','twb','tds'],ascending=[True,True,False]).to_excel(writer, sheet_name="list_table", index=False)