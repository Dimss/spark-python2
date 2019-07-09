from pyspark import SparkConf, SparkContext
from operator import add
from pyspark.sql.functions import lit

# logFile = "/tmp/README.md"
app_name = "start-test-app1"
master = "k8s://https://ocp-local:8443"
conf = SparkConf().setAppName(app_name).setMaster(master).set('spark.kubernetes.container.image','dimssss/spark-py:v2.4.3-v3').set('deploy-mode', 'client')
sc = SparkContext(conf=conf)
data = sc.parallelize(list("Hello World"))
counts = data.map(lambda x: (x, 1)).reduceByKey(add).sortBy(lambda x: x[1], ascending=False).collect()
for (word, count) in counts:
    print("{}: {}".format(word, count))
sc.stop()
