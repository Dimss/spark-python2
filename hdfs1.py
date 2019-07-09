#!/usr/bin/python
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import lit


class CONF(object):
    SPARK_CONTEXT = None
    APP_NAME = "BNHP_K8S_SPARK_TEST"
    PATH = 'hdfs://172.20.10.5:9000/user/root/df_%s'
    MASTER = "k8s://https://ocp-local:8443"
    PYSPARK_IMAGE = "dimssss/spark-py:v2.4.3-v3"
    DEPLOY_MODE = "cluster"


def get_spark_session():
    if not CONF.SPARK_CONTEXT:
        conf = SparkConf() \
            .setAppName(CONF.APP_NAME) \
            .setMaster(CONF.MASTER) \
            .set('spark.kubernetes.container.image', CONF.PYSPARK_IMAGE) \
            .set('deploy-mode', CONF.DEPLOY_MODE)
        sc = SparkContext(conf=conf)
        CONF.SPARK_CONTEXT = SQLContext(sc)
    return CONF.SPARK_CONTEXT


def write_to_hdfs(df, path):
    df.write.mode("overwrite").format("json").save(path)


def get_path(df_n):
    return CONF.PATH % df_n


def create_generated_dfs(dfs_n, cols_n, rows_n):
    key = 'key'
    key_df = get_spark_session().createDataFrame(map(lambda x: [rows_n], xrange(rows_n)), [key])

    for i in xrange(dfs_n):
        df = key_df
        for col_id in xrange(cols_n):
            df = key_df.withColumn('c_%s' % col_id, lit(("%s" % col_id) * 20))
        df.show()
        write_to_hdfs(df, get_path(i))


create_generated_dfs(2, 2, 2)
