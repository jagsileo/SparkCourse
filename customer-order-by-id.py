from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf = conf)
def parseLine(line):
    fields = line.split(',')
    custid = int(fields[0])
    amt = float(fields[2])
    return (custid, amt)

lines = sc.textFile("file:///Users/jagathshreeiyer/Documents/SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine)
totalsByCustId = rdd.reduceByKey(lambda x, y: (x+y))
totalsSorted = totalsByCustId.sortBy(lambda x : x[1], ascending=False)
results = totalsSorted.collect()
for result in results:
    print(result)
