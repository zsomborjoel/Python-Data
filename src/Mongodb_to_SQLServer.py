import pymongo
import csv
import pyodbc

#creating a list of the output of the Mongodb query
def create_list(result):
    records = []

    for record in result:
        tmp = []
        headers = ['restaurant_id', 'name', 'cuisine', 'borough']
        tmp.append(record['restaurant_id'])
        tmp.append(record['name'])
        tmp.append(record['cuisine'])
        tmp.append(record['borough'])
        records.append(tmp)

    print('Restaurant table prepared')

    return records, headers

#then I format the list I created to be suitable for CSV file format
def format_list(list, length, delimeter, quote):
    counter = 1
    string = ''
    for record in list:
        if counter == length:
            string += quote + record + quote + '\n'
        else:
            string += quote + record + quote + delimeter
        counter += 1
    return string

#creating the csv file with the format_list function
def create_csv(records, headers,file_path):
    f = open(file_path, 'w')

    row_len = len(headers)
    f.write(format_list(headers, row_len, ',', '"'))

    for record in records:
        f.write(format_list(record, row_len, ',', '"'))
    f.close()

    print('CSV file successfully created: '.format(file_path))

#finaly we load the data from the csv file
def sqlserver_load_table(file_path):
    try:
        server = '192.168.0.80,2000'
        database = 'test'
        username = 'wntzsombi'
        password = 'P@ss'
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
        print("Connecting to the SqlServer database")

        cur = conn.cursor()

        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            next(reader)
            count = 0
            for row in reader:
                count += 1
                cur.execute("""INSERT INTO dbo.restaurant(restaurant_id, name, cuisine, borough)
                                VALUES (?, ?, ?, ?);""", row)
        print(str(count) + ' rows been inserted into the target table.')

        conn.commit()
        conn.close()

        print("DB connection closed.")
    except Exception as e:
        print("Error: {}".format(str(e)))


def main():
    client = pymongo.MongoClient("mongodb+srv://wntzsombi:"+
                                "P@ss"+
                                "@joelcluster-vs1rn.mongodb.net/test?retryWrites=true")

    db = client.datasets
    restaurants = db.restaurants

    mongoquery = restaurants.find({"cuisine": "Irish"},
                                  {"restaurant_id": "true",
                                   "name": "true",
                                   "cuisine": "true",
                                   "borough": "true"})


    rest_tup = create_list(mongoquery)
    rest_records = rest_tup[0]
    rest_headers = rest_tup[1]
    create_csv(rest_records, rest_headers, './restaurants.csv')
    sqlserver_load_table('./restaurants.csv')

    print('Loading is finished')



if __name__ == "__main__":
    main()
