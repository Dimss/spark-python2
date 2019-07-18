import os
from pyspark import SparkConf, SparkContext, SQLContext
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from snakebite.client import Client


class CONF(object):
    # DEFAULTS
    _DEFAULTS = {
        'HDFS_HOST': '172.20.10.5',
        'IMAGE': 'dimssss/spark-py:2.4.3-0.2',
        'MODE': 'cluster',
        'MASTER': 'k8s://https://ocp-local:8443',
        'NAMESPACE': 'spark',
    }

    # SPARK CONFIGS
    SPARK_CONTEXT = None
    SPARK_SQLCONTEXT = None
    APP_NAME = "BNHP_K8S_SPARK_TEST"
    HDFS_HOST = _DEFAULTS['HDFS_HOST'] if os.environ.get('SAPP_HDFS_HOST') is None else os.environ.get('SAPP_HDFS_HOST')
    MASTER = _DEFAULTS['MASTER'] if os.environ.get('SAPP_MASTER') is None else os.environ.get('SAPP_MASTER')
    PYSPARK_IMAGE = _DEFAULTS['IMAGE'] if os.environ.get('SAPP_IMAGE') is None else os.environ.get('SAPP_IMAGE')
    NAMESPACE = _DEFAULTS['NAMESPACE'] if os.environ.get('SAPP_NAMESPACE') is None else os.environ.get('SAPP_NAMESPACE')
    DEPLOY_MODE = _DEFAULTS['MODE'] if os.environ.get('SAPP_DMODE') is None else os.environ.get('SAPP_DMODE')


DUP_SIZE = float(1024 * 1024)
SAPP_PROFILE = "prod" if os.environ.get('PROFILE') == "prod" else "dev"


def _set_spark_session():
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
            CONF.SPARK_SQLCONTEXT = SQLContext(CONF.SPARK_CONTEXT)
        else:
            CONF.SPARK_CONTEXT = SparkContext()
        # Set spark context
        CONF.SPARK_SQLCONTEXT = SQLContext(CONF.SPARK_CONTEXT)


def get_spark_session():
    _set_spark_session()
    return CONF.SPARK_SQLCONTEXT


def _join(small, big, keys):
    val = big[0].join(small[0], keys, how="outer")
    weight = small[1] + big[1]
    return val, weight


def perform_join(dfs, keys):
    next_iter = []
    if len(dfs) == 0:
        raise Exception()
    if len(dfs) == 1:
        return dfs[0][0]

    dfs.sort(key=lambda x: x[1])

    for i in range(int(len(dfs) / 2)):
        next_iter.append(_join(dfs[i], dfs[-i - 1], keys))

    if len(dfs) % 2 == 1:
        next_iter.append(dfs[i + 1])

    return perform_join(next_iter, keys)


def concurrent(func, executors_max, params_lists):
    results = []
    with ThreadPoolExecutor(executors_max) as executor:
        future_dfs = {executor.submit(func, params_list) for params_list in params_lists}
        for future in futures.as_completed(future_dfs):
            res = future.result()
            if res:
                results.append(res)
    return results


def get_dataframe(path):
    return get_spark_session().read.parquet("hdfs://{}:9000/{}".format(CONF.HDFS_HOST, path))


def get_path_size_in_m(path):
    # List files with their properties
    print(path)
    HDFS_CLIENT = Client(CONF.HDFS_HOST, 9000, use_trash=False)
    files = list(HDFS_CLIENT.du([path]))
    # Calculate file size in mega
    size_in_m = sum([f['length'] for f in files]) / DUP_SIZE
    return size_in_m


def get_dataframe_tuple(path):
    return get_dataframe(path), get_path_size_in_m(path)


def get_dataframes_for_join(paths):
    return concurrent(get_dataframe_tuple, 20, paths)


def get_df_pats():
    df_paths = []
    HDFS_CLIENT = Client(CONF.HDFS_HOST, 9000, use_trash=False)
    for file_entry in HDFS_CLIENT.ls(['/user/root']):
        df_paths.append(file_entry['path'])
    return df_paths


_set_spark_session()
df_paths = get_df_pats()
perform_join(get_dataframes_for_join(df_paths), 'key')
CONF.SPARK_CONTEXT.stop()
