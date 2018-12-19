import pyodbc
import json
import pymongo

#getting json data format from SQL Server with T-SQL
def sqlserver_extract ():
    server = '192.168.0.80,2000'
    database = 'test'
    username = 'wntzsombi'
    password = 'P@ss'
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    print("Connecting to the SqlServer database")

    cur = conn.cursor()

    #doing some aggregation on the data and create special type of customer groups 
    cur.execute("""               
    WITH Customer_CTE AS
    (	
        SELECT
    
            TRIM(CONCAT(P.Title, ' ', P.FirstName, ' ', P.MiddleName, ' ', P.LastName)) AS FullName,
            PP.PhoneNumber AS PhoneNumber,
            EmailAddress.EmailAddress AS EmailAddress,
            SUM(CustomerSales.TotalDue) AS SumTotalDue	
    
        FROM AdventureWorks2016CTP3.Person.Person AS P
        INNER JOIN AdventureWorks2016CTP3.Person.EmailAddress AS EmailAddress 
            ON P.BusinessEntityID = EmailAddress.BusinessEntityID
        INNER JOIN AdventureWorks2016CTP3.Person.PersonPhone AS PP 
            ON P.BusinessEntityID = PP.BusinessEntityID
        INNER JOIN AdventureWorks2016CTP3.Sales.Customer AS C 
            ON P.BusinessEntityID = C.PersonID
        INNER JOIN AdventureWorks2016CTP3.Sales.SalesOrderHeader AS CustomerSales
            ON C.CustomerID = CustomerSales.CustomerID	
    
        WHERE CustomerSales.Status = 5
    
        GROUP BY 
            P.Title,
            P.FirstName,
            P.MiddleName,
            P.LastName,
            PP.PhoneNumber,
            EmailAddress.EmailAddress
    ),
     final AS (
        SELECT 
        *,
        CASE
            WHEN SumTotalDue BETWEEN 0 AND 5000 THEN 'Bronze'
            WHEN SumTotalDue BETWEEN 5000 AND 10000 THEN 'Silver'
            WHEN SumTotalDue BETWEEN 10000 AND 20000 THEN 'Gold'
            WHEN SumTotalDue BETWEEN 20000 AND 30000 THEN 'Diamond'
            ELSE 'Star' 
        END AS CustomerCategory
        FROM Customer_CTE
                )
                SELECT TOP 50 * 
                FROM final 
                FOR JSON auto, INCLUDE_NULL_VALUES;""")

    rows = cur.fetchall()

    #transform output to ablo to import to a json file
    newrows = str(rows)[3:-5]

    with open("customer_data.json", 'w') as outfile:
        json.dump(newrows, outfile)

#connectiong to mongodb and loading into Customer collection
def mongodb_load():
    client = pymongo.MongoClient("mongodb+srv://wntzsombi:" +
                                "P@ss" +
                                "@joelcluster-vs1rn.mongodb.net/test?retryWrites=true")

    db = client.JoelTest
    Customer = db.Customer

    #transform file
    txt = open("customer_data.json", 'r')
    changetxt = txt.read()[1:-1]
    finalchange = changetxt.replace('\\', '')


    print(finalchange)
    parsed = json.loads(finalchange)

    for data in parsed:
        print(data)
        Customer.insert(data)

def main ():
    sqlserver_extract()
    mongodb_load()

if __name__ == "__main__":
    main()
