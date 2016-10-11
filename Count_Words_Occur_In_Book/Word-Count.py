from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///C:/Users/User/Desktop/Udemyspark/sparkpython/book.txt")
words = input.flatMap(lambda x: x.split())			# breaks any whitespace into indivudal words
wordCounts = words.countByValue()	# for every whitespace, we get the results.

for word, count in wordCounts.items():		
    cleanWord = word.encode('ascii', 'ignore')		# takes care of encoding issue
    if (cleanWord):						
        print cleanWord, count
