#!/usr/bin/python
import os
import time
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql.functions import lit

SAPP_PROFILE = "prod" if os.environ.get('SAPP_PROFILE') == "prod" else "dev"


class CONF(object):
    # DEFAULTS
    _DEFAULTS = {
        'PATH': 'hdfs://172.20.10.5:9000/user/root/df_1{id}',
        'IMAGE': 'dimssss/spark-py:v2.4.3-centos7-0.2',
        'MODE': 'cluster',
        'MASTER': 'k8s://https://ocp-local:8443',
        'NAMESPACE': 'spark',
        'DFS_N': 2,
        'COLS_N': 5,
        'ROWS_N': 10
    }

    # SPARK CONFIGS
    SPARK_CONTEXT = None
    SPARK_SQLCONTEXT = None
    APP_NAME = "BNHP_K8S_SPARK_TEST"
    PATH = _DEFAULTS['PATH'] if os.environ.get('SAPP_HDFS') is None else os.environ.get('SAPP_HDFS')
    MASTER = _DEFAULTS['MASTER'] if os.environ.get('SAPP_MASTER') is None else os.environ.get('SAPP_MASTER')
    PYSPARK_IMAGE = _DEFAULTS['IMAGE'] if os.environ.get('SAPP_IMAGE') is None else os.environ.get('SAPP_IMAGE')
    NAMESPACE = _DEFAULTS['NAMESPACE'] if os.environ.get('SAPP_NAMESPACE') is None else os.environ.get('SAPP_NAMESPACE')
    DEPLOY_MODE = _DEFAULTS['MODE'] if os.environ.get('SAPP_DMODE') is None else os.environ.get('SAPP_DMODE')
    DFS_N = int(_DEFAULTS['DFS_N'] if os.environ.get('SAPP_DFS_N') is None else os.environ.get('SAPP_DFS_N'))
    COLS_N = int(_DEFAULTS['COLS_N'] if os.environ.get('SAPP_COLS_N') is None else os.environ.get('SAPP_COLS_N'))
    ROWS_N = int(_DEFAULTS['ROWS_N'] if os.environ.get('SAPP_ROWS_N') is None else os.environ.get('SAPP_ROWS_N'))


def get_spark_session():
    if not CONF.SPARK_SQLCONTEXT:
        # Configure spark context
        if SAPP_PROFILE == "dev":
            conf = SparkConf() \
                .setAppName(CONF.APP_NAME) \
                .setMaster(CONF.MASTER) \
                .set('spark.kubernetes.container.image', CONF.PYSPARK_IMAGE) \
                .set('deploy-mode', CONF.DEPLOY_MODE) \
                .set('spark.kubernetes.namespace', CONF.NAMESPACE)
            CONF.SPARK_CONTEXT = SparkContext(conf=conf)
        else:
            CONF.SPARK_CONTEXT = SparkContext()
        # Set spark context
        CONF.SPARK_SQLCONTEXT = SQLContext(CONF.SPARK_CONTEXT)
    return CONF.SPARK_SQLCONTEXT


def write_to_hdfs(df, path):
    print("Gonna save parquet file. . .")
    df.write.mode("overwrite").format("parquet").save(path)


def get_path(df_n):
    return CONF.PATH.format(id=df_n)


def create_generated_dfs(dfs_n, cols_n, rows_n):
    key = 'key'
    key_df = get_spark_session().createDataFrame(map(lambda x: [rows_n], xrange(rows_n)), [key])
    for i in xrange(dfs_n):
        df = key_df
        for col_id in xrange(cols_n):
            df = key_df.withColumn('c_%s_%s' % (i, col_id), lit(("%s" % col_id) * 20))
        df.show()
        write_to_hdfs(df, get_path(i))


def debug_prints():
    print("#################################### APP CONF ####################################")
    print("App PROFILE: {}, HDFS: {}".format(SAPP_PROFILE, CONF.PATH))
    print("Params: DFS_N: {}, COLS_N: {}, ROWS_N: {}".format(CONF.DFS_N, CONF.COLS_N, CONF.ROWS_N))
    print("##################################################################################")


debug_prints()
create_generated_dfs(CONF.DFS_N, CONF.COLS_N, CONF.ROWS_N)
CONF.SPARK_CONTEXT.stop()