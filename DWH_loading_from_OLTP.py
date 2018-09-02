import pyodbc
import psycopg2 as pg


#SQL Server connection
server = '192.168.0.80,2000'
database = 'AdventureWorks2016CTP3'
username = 'wntzsombi'
password = 'P@SS'
sourceConn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
sourceCur = sourceConn.cursor()

#PostgeSQL connection
dbConnect = {'AdventureWorksDWH':"dbname=AdventureWorksDWH user=postgres password=P@SS host=127.0.0.1"}
targetConn = pg.connect(dbConnect['AdventureWorksDWH'])
targetCur = targetConn.cursor()

#Getting data from the source SQL Server OLTP database
sourceCur.execute("""SELECT
        PP.Name AS EnglishProductName,
        PP.Size,
        PP.SizeUnitMeasureCode,
        PP.Weight,
        PP.Style,
        PP.Class,
        PP.WeightUnitMeasureCode,
        PP.StandardCost,
        PP.ListPrice,
        CONVERT(money, (PP.ListPrice * 0.2)) AS DealerPrice,
        PP.DaysToManufacture,
        PP.ReorderPoint,
        PP.SafetyStockLevel,
        PP.Color,
        '1' AS FinishedGoodsFlag,
        PP.ProductSubcategoryID AS ProductSubcategoryKey,
        PPM.Name AS ModelName,
        PPD.Description AS EnglishDescription
    FROM Production.Product AS PP
    LEFT JOIN Production.ProductModel AS PPM
        ON PP.ProductModelID = PPM.ProductModelID
    LEFT JOIN  Production.ProductModelProductDescriptionCulture AS PPMPDC
        ON PPM.ProductModelID = PPMPDC.ProductModelID
    LEFT JOIN Production.ProductDescription AS PPD
        ON PPMPDC.ProductDescriptionID = PPD.ProductDescriptionID
    WHERE PPMPDC.CultureID = 'en';""")

#Fetch source data
sourceData = sourceCur.fetchall()

#Inserting data into the target PostgeSql datawarehouse table
for row in sourceData:
        targetCur.execute("""INSERT INTO public.DimProduct (EnglishProductName, Size, SizeUnitMeasureCode, Weight, Style, Class, WeightUnitMeasureCode, StandardCost, ListPrice, DealerPrice, DaysToManufacture, ReorderPoint, SafetyStockLevel, Color, FinishedGoodsFlag, ProductSubcategoryKey, ModelName, EnglishDescription)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""", row)

#Commit transaction
targetConn.commit()

print("The loading finished successfully.")
