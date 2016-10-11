from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomerSorted")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile("file:///sparkcourse/customer-orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

# the code is below:
# if need to sort by value, we flip it around.
# sort by key which is amount spent
flipped = totalByCustomer.map(lambda (x,y):(y,x))
totalByCustomerSorted = flipped.sortByKey()

results = totalByCustomerSorted.collect();
for result in results:
    print result
