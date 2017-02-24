# import pymssql
# conn = pymssql.connect(server='azureexpsql.database.windows.net', user='ramz@zhaw.ch', password='yourpassword', database='documents')
# cursor = conn.cursor()
# cursor.execute(
#     'SELECT c.CustomerID, c.CompanyName, COUNT(soh.SalesOrderID) AS OrderCount FROM SalesLT.Customer AS c LEFT OUTER JOIN SalesLT.SalesOrderHeader AS soh ON c.CustomerID = soh.CustomerID GROUP BY c.CustomerID, c.CompanyName ORDER BY OrderCount DESC;')
# row = cursor.fetchone()
# while row:
#     print str(row[0]) + " " + str(row[1]) + " " + str(row[2])
#     row = cursor.fetchone()