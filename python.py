### Open shell:
pyspark

### Reduce logs:
log4j = sc._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

### Simple variables:
a=5
a+2

### Get data via HTTP URL
import urllib
f = urllib.urlretrieve ("http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data_10_percent.gz", "kddcup.data_10_percent.gz")


### Array:
data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)

distData.count()

### Test:
ar1 = [('Vlad', 1)]
sqlContext.createDataFrame(ar1).collect()
sqlContext.createDataFrame(ar1, ['name', 'age']).collect()

