#!/usr/bin/python
import os
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import lit

PROFILE = "dev" if os.environ.get('PROFILE') == "dev" else "prod"


class CONF(object):
    # DEFAULTS
    _DEFAULT_PATH = 'hdfs://172.20.10.5:9000/user/root/df_%s'
    _DEFAULT_IMAGE = 'dimssss/spark-py:v2.4.3-v3'
    _DEFAULT_MODE = "cluster"
    _DEFAULT_MASTER = "k8s://https://ocp-local:8443"

    # SPARK CONFIGS
    SPARK_CONTEXT = None
    APP_NAME = "BNHP_K8S_SPARK_TEST"
    PATH = _DEFAULT_PATH if os.environ.get('SAPP_HDFS') is None else os.environ.get('SAPP_HDFS')
    MASTER = _DEFAULT_MASTER if os.environ.get('SAPP_MASTER') is None else os.environ.get('SAPP_MASTER')
    PYSPARK_IMAGE = _DEFAULT_IMAGE if os.environ.get('SAPP_IMAGE') is None else os.environ.get('SAPP_IMAGE')
    DEPLOY_MODE = _DEFAULT_MODE if os.environ.get('SAPP_DEPLOY_MODE') is None else os.environ.get('SAPP_DEPLOY_MODE')


def get_spark_session():
    if not CONF.SPARK_CONTEXT:
        # Configure spark context
        if PROFILE == "dev":
            conf = SparkConf() \
                .setAppName(CONF.APP_NAME) \
                .setMaster(CONF.MASTER) \
                .set('spark.kubernetes.container.image', CONF.PYSPARK_IMAGE) \
                .set('deploy-mode', CONF.DEPLOY_MODE)
            sc = SparkContext(conf=conf)
            CONF.SPARK_CONTEXT = SQLContext(sc)
        else:
            sc = SparkContext()
        # Set spark context
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


print("App PROFILE: %s".format(PROFILE))
create_generated_dfs(2, 2, 2)
