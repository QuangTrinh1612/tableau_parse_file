import pandas as pd
from bs4 import BeautifulSoup
import re

# 09042021 : 
# - Include Get_tables
# - Add these function to find_tables and find_tables_from_twb
#   append the function to include these queries data
# - Change Table Type if class = google-sheets

def get_tables(sql):
    result = []
    try:
        # Remove comment /**/
        sql = re.sub(r'\/\*.*?\*\/', '', sql)
        sql = re.sub(r'--.*?\n', '', sql)
        sql = re.sub(r'//.*?\n','',sql)
        # TODO : Remove comment --
        sql_lower = sql.lower().replace(r"\n"," ")
        result = list(set([t.replace(" ", "").replace("(", "").replace(")", "").replace('\"',"")  for t in re.findall(r"(?:from|join)[\s]+([^\s]+[.][\s]*[^\s]+)", sql_lower)]))
        result = [re.search(r".*?([^.]+\.[^.]+)$", x).group(1) for x in result]
    except:
        result = []
    return result


def find_tables(soup, folderPath, wbPath, tdsPath=None,):
    tables = soup.find_all("relation")
    tables = [t for t in tables if t.get("connection") is not None]
    folderNames = []
    workBooks = []
    datasourceNames = []
    tableNames = []
    tableRefNames = []
    tableTypes = []
    jointTypes = []
    queries = []
    connectionStrings = []
    datasourceType=[]
    tableQueries = []

    for table in tables:
        folderNames.append(folderPath)
        workBooks.append(wbPath)
        datasourceNames.append(tdsPath)
        if table.get("table"):
            tableNames.append(table.get("table"))
            tableRefNames.append(table.get("name"))
        else:
            tableNames.append(table.get("name"))
            tableRefNames.append(table.get("name"))
        
        jointTypes.append(table.parent.get("join"))
        queries.append(table.string)
        connection = table.get("connection")
        connectionTag = soup.find("named-connection",attrs={"name": connection}).find("connection")
        connectionStrings.append(connectionTag)
        datasourceType.append("Published")

        # Additional code : 09042021
        # Include google sheets into table type
        is_gsheet=False
        if connectionTag.get("class")[0] == "google-sheets":
            tableTypes.append(connectionTag.get("class")[0])
            is_gsheet=True
        else:
            tableTypes.append(table.get("type"))

        # Additional code : 09042021
        # Getting list of tables on query
        if table.string is not None:
            if len(table.string)>0:
                tableQueries.append(get_tables(table.string))
            else:
                tableQueries.append(None)
        elif table.get("type") == "table" and not is_gsheet:
            tableQueries.append([table.get("table")])
        else:
            tableQueries.append(None)
    
    tableData = pd.DataFrame({"folderName": folderNames, "twb": workBooks, "tds": datasourceNames, "tableName": tableNames, "tableRefName": tableRefNames, "tableTypes": tableTypes, "jointTypes": jointTypes, "queries": queries, "connectionStrings": connectionStrings,"datasourceType":datasourceType,"listTables":tableQueries})
    # queriesData = pd.DataFrame({"folderName": folderNames, "twb": workBooks, "tds": datasourceNames})

    print("dataaaa:  ",tableData)
    tableData.drop_duplicates(subset=["twb", "tds", "tableName", "connectionStrings"], keep="last", inplace=True)
    return tableData


def find_tables_from_twb(soup, folderPath, wbPath,tdsSrcName,is_published):
    folderNames = []
    workBooks = []
    datasourceNames = []
    tableNames = []
    tableRefNames = []
    tableTypes = []
    jointTypes = []
    queries = []
    connectionStrings = []
    datasourceType=[]
    tableQueries = []
    if not is_published :
        # datasources = soup.find_all("datasource")
        # datasources = [d for d in datasources if d.find("connection") is not None]
        # sourceNames = [d.get("caption") for d in datasources]
        # for i in range(len(datasources)):
        #     d = datasources[i]
        #     sourceName = sourceNames[i]
        tables = soup.find_all("relation")
        tables = [t for t in tables if t.get("connection") is not None]
        for table in tables:
            folderNames.append(folderPath)
            workBooks.append(wbPath)
            datasourceNames.append(tdsSrcName)
            if table.get("table"):
                tableNames.append(table.get("table"))
                tableRefNames.append(table.get("name"))
            else:
                tableNames.append(table.get("name"))
                tableRefNames.append(table.get("name"))
            # tableTypes.append(table.get("type"))
            jointTypes.append(table.parent.get("join"))
            queries.append(table.string)
            connection = table.get("connection")
            connectionTag = soup.find("named-connection",attrs={"name": connection}).find("connection")
            connectionStrings.append(connectionTag)
            datasourceType.append("Unpublished")

            # Additional code : 09042021
            # Include google sheets into table type
            # print("connect class : ",connectionTag.get("class"))
            is_gsheet=False
            if connectionTag.get("class")[0] == "google-sheets":
                tableTypes.append(connectionTag.get("class")[0])
                is_gsheet=True
            else:
                tableTypes.append(table.get("type"))

            # Additional code : 09042021
            # Getting list of tables on query
            if table.string is not None:
                if len(table.string)>0:
                    tableQueries.append(get_tables(table.string))
                else:
                    tableQueries.append(None)
            elif table.get("type") == "table" and not is_gsheet:
                tableQueries.append([table.get("table")])
            else:
                tableQueries.append(None)
    else:
        folderNames.append(folderPath)
        workBooks.append(wbPath)
        datasourceNames.append(tdsSrcName)
        tableNames.append(None)
        tableRefNames.append(None)
        tableTypes.append(None)
        jointTypes.append(None)
        queries.append(None)
        connectionStrings.append(None)
        datasourceType.append("Published")
        tableQueries.append(None)

    
        

    tableData = pd.DataFrame({"folderName": folderNames, "twb": workBooks, "tds": datasourceNames, "tableName": tableNames,  "tableRefName": tableRefNames, "tableTypes": tableTypes, "jointTypes": jointTypes, "queries": queries, "connectionStrings": connectionStrings,"datasourceType":datasourceType,"listTables":tableQueries})
    tableData.drop_duplicates(subset=["twb", "tds", "tableName", "connectionStrings"], keep="last", inplace=True)
    print("dataaaa:  ",tableData)
    return tableData

def parse_column_tag(soup, folderPath, wbPath, tdsPath):
    columns = soup.find_all("column")
    columns = [c for c in columns if c.get("hidden") != "true"]
    folderNames = []
    workBooks = []
    datasourceNames = []
    fieldNames = []
    names = []
    roles = []
    dataTypes = []
    types = []
    formulaClass = []
    formulas = []
    columnTypes = [] #Base or Derived
    
    for column in columns:
        #### 26032021 : checking parent tag should be not datasource-dependencies:
        if column.findParent("datasource-dependencies") is None:
            if column.get("role") is not None:
                fieldNames.append(column.get("caption"))
                names.append(column.get("name"))
                folderNames.append(folderPath)
                workBooks.append(wbPath)
                datasourceNames.append(tdsPath)
            if column.get("role") is not None:
                roles.append(column.get("role"))
            if column.get("role") is not None:
                dataTypes.append(column.get("datatype"))
            if column.get("role") is not None:
                types.append(column.get("type"))
            if column.get("role") is not None:
                if column.find("calculation") is None:
                    formulaClass.append(None)
                    formulas.append(None)
                    columnTypes.append("BASE")
                else:
                    formulaClass.append(column.find("calculation").get("class"))
                    formulas.append(column.find("calculation").get("formula"))
                    columnTypes.append("DERIVED")
    columnData = pd.DataFrame({"folderName": folderNames, "twb": workBooks, "tds": datasourceNames, "tableau_field_name": fieldNames,"column_id": names, "role": roles, 
        "datatype": dataTypes, "type": types, "column_type": columnTypes,"formulation_class": formulaClass, "formula": formulas})
    columnData.drop_duplicates(subset=["twb", "tds", "column_id", "role", "datatype", "type"], keep="last", inplace=True)
    return columnData


def parse_column_tag_twb(soup, folderPath, wbPath,tdsSrcName):
    folderNames = []
    workBooks = []
    datasourceNames = []
    fieldNames = []
    names = []
    roles = []
    dataTypes = []
    types = []
    formulaClass = []
    formulas = []
    columnTypes = [] #Base or Derived
    # print("inside -1 : ",soup)
    # datasources = soup.find("datasource")
    # print("inside 0 : ",datasources)
    # datasources = [d for d in datasources if d.find("connection") is not None]
    # sourceNames = [d.get("caption") for d in datasources]
    # print("inside 2: ",sourceNames)
    # print("inside 1: ",datasources)
    
    # for i in range(len(datasources)):
    #     d = datasources[i]
    #     sourceName = sourceNames[i]
    columns = soup.find_all("column")
    columns = [c for c in columns if c.get("hidden") != "true"] 
    for column in columns:

        #### 26032021 : checking parent tag should be not datasource-dependencies:
        if column.findParent("datasource-dependencies") is None:
            if column.get("role") is not None:
                fieldNames.append(column.get("caption"))
                names.append(column.get("name"))
                folderNames.append(folderPath)
                workBooks.append(wbPath)
                datasourceNames.append(tdsSrcName)
            if column.get("role") is not None:
                roles.append(column.get("role"))
            if column.get("role") is not None:
                dataTypes.append(column.get("datatype"))
            if column.get("role") is not None:
                types.append(column.get("type"))
            if column.get("role") is not None:
                if column.find("calculation") is None:
                    formulaClass.append(None)
                    formulas.append(None)
                    columnTypes.append("BASE")
                else:
                    formulaClass.append(column.find("calculation").get("class"))
                    formulas.append(column.find("calculation").get("formula"))
                    columnTypes.append("DERIVED")
    columnData = pd.DataFrame({"folderName": folderNames, "twb": workBooks, "tds": datasourceNames, "tableau_field_name": fieldNames,"column_id": names, "role": roles, 
        "datatype": dataTypes, "type": types, "column_type": columnTypes,"formulation_class": formulaClass, "formula": formulas})
    columnData.drop_duplicates(subset=["twb", "tds", "column_id", "role", "datatype", "type"], keep="last", inplace=True)
    return columnData

def parse_metadata_record_tag(soup, folderPath, wbPath, tdsPath=None):
    columns = soup.find_all("metadata-record", attrs={"class": "column"})
    folderNames = []
    workBooks = []
    datasourceNames = []
    captions = []
    dataTypes = []
    formulas = []
    physicalTables = []
    originCols = []
    for column in columns:
        for child in column.children:
            if child.name == "remote-name":
                captions.append(child.string)
                folderNames.append(folderPath)
                workBooks.append(wbPath)
                datasourceNames.append(tdsPath)
            if child.name == "local-type":
                dataTypes.append(child.string)
            if child.name == "aggregation":
                formulas.append(child.string)
            if child.name == "parent-name":
                physicalTables.append(child.string)
            if child.name == "local-name":
                originCols.append(child.string.replace("[","").replace("]",""))
    columnDataOne = pd.DataFrame({"folderName": folderNames, "twb": workBooks, "tds": datasourceNames, "tableau_field_name": captions,
    "column_id": originCols, "datatype": dataTypes, "formula": formulas, "physical_table": physicalTables})
    columnDataOne.drop_duplicates(subset=["twb", "tds", "formula", "physical_table", "column_id"], inplace=True)
    columnDataOne = columnDataOne.loc[columnDataOne["physical_table"] != "[Extract]", :]
    return columnDataOne

def parse_metadata_record_tag_twb(soup, folderPath, wbPath, tdsSrcName,is_published):
    # datasources = soup.find_all("datasource")
    # datasources = [d for d in datasources if d.find("connection") is not None]
    # sourceNames = [d.get("caption") for d in datasources]
    folderNames = []
    workBooks = []
    datasourceNames = []
    captions = []
    dataTypes = []
    formulas = []
    physicalTables = []
    originCols = []
    # for i in range(len(datasources)):
    #     d = datasources[i]
    #     sourceName = sourceNames[i]
    columns = soup.find_all("metadata-record", attrs={"class": "column"})
    columns = [c for c in columns if c.get("hidden") != "true"] 
    for column in columns:
        for child in column.children:
            if child.name == "remote-name":
                captions.append(child.string)
                folderNames.append(folderPath)
                workBooks.append(wbPath)
                datasourceNames.append(tdsSrcName)
            if child.name == "local-type":
                dataTypes.append(child.string)
            if child.name == "aggregation":
                formulas.append(child.string)
            if child.name == "parent-name":
                if is_published:
                    physicalTables.append(None)
                else:
                    physicalTables.append(child.string)
            if child.name == "local-name":
                originCols.append(child.string.replace("[","").replace("]",""))
    """ print("folderName len: ", len(captions))
    print("twb type len: ", len(workBooks))
    print("tds type len: ", len(datasourceNames))
    print("tableau_field_name type len: ", len(captions))
    print("column_id type len: ", len(originCols))
    print("datatype type len: ", len(dataTypes))
    print("formula type len: ", len(formulas))
    print("physical_table type len: ", len(physicalTables))
    """
    columnDataOne = pd.DataFrame({"folderName": folderNames, "twb": workBooks, "tds": datasourceNames, "tableau_field_name": captions,
    "column_id": originCols, "datatype": dataTypes, "formula": formulas, "physical_table": physicalTables})
    columnDataOne.drop_duplicates(subset=["twb", "tds","formula", "physical_table", "column_id"], inplace=True)
    columnDataOne = columnDataOne.loc[columnDataOne["physical_table"] != "[Extract]", :]
    return columnDataOne

def parse_bin(soup, folderPath, wbPath, tdsPath=None):
    
    bins = soup.find_all("calculation", attrs={"class":"bin"}) + soup.find_all("calculation", attrs={"class":"categorical-bin"})
    folderNames = []
    workBooks = []
    datasourceNames = []
    colNames = []
    binClass = []
    binFormulas = []
    binValues = []
    for b in bins:
        if not b.find_all("value"):
            folderNames.append(folderPath)
            workBooks.append(wbPath)
            datasourceNames.append(tdsPath)
            colNames.append(b.parent.get("name"))
            binClass.append(b.get("class"))
            binFormulas.append(b.get("formula"))
            binValues.append(None)
        for value in b.find_all("value"):
            folderNames.append(folderPath)
            workBooks.append(wbPath)
            datasourceNames.append(tdsPath)
            colNames.append(b.parent.get("name"))
            binClass.append(b.get("class"))
            binFormulas.append(b.get("formula"))
            binValues.append(value.string)
    binData = pd.DataFrame({"folderName": folderNames, "twb": workBooks, "tds": datasourceNames, "columnName": colNames, 
    "binClass": binClass, "binFormula": binFormulas, "binValue": binValues})
    return binData

def parse_bin_twb(soup, folderPath, wbPath,tdsSrcName):
    # datasources = soup.find_all("datasource")
    # datasources = [d for d in datasources if d.find("connection") is not None]
    # sourceNames = [d.get("caption") for d in datasources]
    # bins = soup.find_all("calculation", attrs={"class":"bin"}) + soup.find_all("calculation", attrs={"class":"categorical-bin"})
    folderNames = []
    workBooks = []
    datasourceNames = []
    colNames = []
    binClass = []
    binFormulas = []
    binValues = []
    # for i in range(len(datasources)):
    #     d = datasources[i]
    #     sourceName = sourceNames[i]
    bins = soup.find_all("calculation", attrs={"class":"bin"}) + soup.find_all("calculation", attrs={"class":"categorical-bin"})
    for b in bins:
        if not b.find_all("value"):
            folderNames.append(folderPath)
            workBooks.append(wbPath)
            datasourceNames.append(tdsSrcName)
            colNames.append(b.parent.get("name"))
            binClass.append(b.get("class"))
            binFormulas.append(b.get("formula"))
            binValues.append(None)
        for value in b.find_all("value"):
            folderNames.append(folderPath)
            workBooks.append(wbPath)
            datasourceNames.append(tdsSrcName)
            colNames.append(b.parent.get("name"))
            binClass.append(b.get("class"))
            binFormulas.append(b.get("formula"))
            binValues.append(value.string)
    binData = pd.DataFrame({"folderName": folderNames, "twb": workBooks, "tds": datasourceNames, "columnName": colNames, 
    "binClass": binClass, "binFormula": binFormulas, "binValue": binValues})
    return binData

def parse_sheet(soup, folderPath, wbPath):
    worksheets = soup.find_all("worksheet")
    worksheets = [s for s in worksheets if s.get("name") is not None]
    folderNames = []
    workBooks = []
    datasourceNames = []
    workSheetNames = []
    colNames = []


    roles = []
    dataTypes = []
    types = []
    
    for worksheet in worksheets:
        workSheetName = worksheet.get("name")
        for datasource in worksheet.find_all("datasource-dependencies"):
            sourceId = datasource.get("datasource")
            sourceFind = worksheet.find("datasource", attrs={"name":sourceId})
            # sourceName = sourceFind.get("caption") if sourceFind is not None and sourceFind.get("caption") is not None else sourceFind.get("name") if sourceFind is not None and sourceFind.get("name") is not None else None
            if sourceFind is not None:
                if sourceFind.get("caption") is not None:
                    sourceName = sourceFind.get("caption")
                elif sourceFind.get("name") is not None:
                    sourceName = sourceFind.get("name")
                else:
                    sourceName = None
            else:
                sourceName = None

            # To Get details Column on the worksheet
            # 26032021 : Capture additional information on role,datatype,type ( this one will be required on filled-up the column sheet that comes from metadata and not converted to column tag )
            cols = datasource.find_all("column")
            cols = [col for col in cols if col.get("role") is not None]
            for col in  cols:
                folderNames.append(folderPath)
                workBooks.append(wbPath)
                workSheetNames.append(workSheetName)
                datasourceNames.append(sourceName)
                if col.get("role") is not None:
                    if col.get("caption") is not None:
                        colNames.append(col.get("caption"))
                    else:
                        colNames.append(col.get("name"))
                    roles.append(col.get("role"))
                    dataTypes.append(col.get("datatype"))
                    types.append(col.get("type"))

    workSheetData = pd.DataFrame({"FolderName": folderNames, "twb": workBooks, "workSheet": workSheetNames, 
    "dataSource": datasourceNames, "tableau_field_name": colNames, "roles" : roles,"dataTypes" : dataTypes,"types": types})
    return workSheetData

def assign_column_type(row):
    if pd.notnull(row["column_type"]):
        return row["column_type"]
    elif pd.isnull(row["formula"]):
        return "BASE"
    else:
        return "DERIVED"

def find_parameter(folderPath, wbPath):
    tableData = pd.DataFrame({"folderName": [folderPath], "twb": [wbPath], "tds": ["Parameters"], "tableName": [None], "tableRefName": ["Parameters"], "tableTypes": [None], "jointTypes": [None], "queries": [None], "connectionStrings": [None],"datasourceType":["Parameter"]})
    # print("dataaaa:  ",tableData)
    tableData.drop_duplicates(subset=["twb", "tds", "tableName", "connectionStrings"], keep="last", inplace=True)
    return tableData

def parse_param_tag(soup, folderPath, wbPath,tdsSrcName):
    folderNames = []
    workBooks = []
    datasourceNames = []
    fieldNames = []
    names = []
    roles = []
    dataTypes = []
    types = []
    formulaClass = []
    formulas = []
    columnTypes = [] #Base or Derived
    physical_table = []
    physical_table_column = []
    columns = soup.find_all("column")
    columns = [c for c in columns if c.get("hidden") != "true"] 
    for column in columns:
        if column.get("role") is not None:
            fieldNames.append(column.get("caption"))
            names.append(column.get("name"))
            folderNames.append(folderPath)
            workBooks.append(wbPath)
            datasourceNames.append(tdsSrcName)
        if column.get("role") is not None:
            roles.append(column.get("role"))
        if column.get("role") is not None:
            dataTypes.append(column.get("datatype"))
        if column.get("role") is not None:
            types.append(column.get("type"))
        
        formulaClass.append(None)
        formulas.append(None)
        columnTypes.append("PARAMETER")
        physical_table.append("Parameters")
        physical_table_column.append(None)

    columnData = pd.DataFrame({"folderName": folderNames, "twb": workBooks, "tds": datasourceNames, "tableau_field_name": fieldNames,"column_id": names, "role": roles, 
        "datatype": dataTypes, "type": types, "column_type": columnTypes,"formulation_class": formulaClass, "formula": formulas,"tableRefName":physical_table,"physical_table_column":physical_table_column})
    columnData.drop_duplicates(subset=["twb", "tds", "column_id", "role", "datatype", "type"], keep="last", inplace=True)
    return columnData

def getFormulaCaption(datasource):
    repl = datasource.set_index('column_id')['tableau_field_name'].to_dict()
    datasource["formula"]=datasource.formula.apply(lambda x: pd.Series([x.replace(k,v) for k,v in repl.items() if k in x]))
    return datasource