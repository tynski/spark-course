from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalSpentByCustomer")
sc = SparkContext(conf=conf)

def parseLines(line):
    fields = line.split(",")
    return (int(fields[0]), float(fields[2])) 
    
lines = sc.textFile("customer-orders.csv")
totalByCustomer = lines.map(parseLines).reduceByKey(lambda x, y: x + y)
totalByCustomerSorted = totalByCustomer.map(lambda x: (x[1], x[0])).sortByKey()

for customer in totalByCustomerSorted.collect():
    print("%i %.2f" % (customer[1], customer[0]))
