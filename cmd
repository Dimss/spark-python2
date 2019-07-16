# PROD
 bin/spark-submit \
    --master k8s://https://CLUSETR_URL:8443 \
    --deploy-mode cluster \
    --name spark-k8s-test \
    --conf spark.kubernetes.namespace=spark \
    --conf spark.executor.instances=10 \
	--conf spark.kubernetes.driverEnv.SAPP_PROFILE=prod \
	--conf spark.kubernetes.driverEnv.SAPP_HDFS="hdfs://HDFS_IP:9000/user/root/df_{id}" \
	--conf spark.kubernetes.driverEnv.SAPP_DFS_N=10 \
	--conf spark.kubernetes.driverEnv.SAPP_COLS_N=10 \
	--conf spark.kubernetes.driverEnv.SAPP_ROWS_N=10 \
	--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark.mount.path=/tmp/pv \
	--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark.options.claimName=spark \
    --conf spark.kubernetes.container.image=spark-image/datagen:v2.4.3-v3 \
    /tmp/pv/datagen.py

# debug locally
 bin/spark-submit \
    --master k8s://https://ocp-local:8443 \
    --deploy-mode cluster \
    --name spark-k8s-test \
    --conf spark.kubernetes.namespace=spark \
    --conf spark.executor.instances=1 \
	--conf spark.kubernetes.driverEnv.SAPP_PROFILE=prod \
	--conf spark.io.compression.codec=snappy \
	--conf spark.kubernetes.driverEnv.SAPP_HDFS="hdfs://172.20.10.5:9000/user/root/df_{id}" \
	--conf spark.kubernetes.driverEnv.SAPP_DFS_N=10 \
	--conf spark.kubernetes.driverEnv.SAPP_COLS_N=10 \
	--conf spark.kubernetes.driverEnv.SAPP_ROWS_N=10 \
    --conf spark.kubernetes.container.image=dimssss/spark-py:v2.4.3-v19 \
    https://raw.githubusercontent.com/Dimss/spark-python2/master/datagen.py


 bin/spark-submit \
    --master k8s://https://ocp-local:8443 \
    --deploy-mode cluster \
    --name spark-k8s-test \
    --conf spark.kubernetes.namespace=spark \
    --conf spark.executor.instances=1 \
    --conf spark.io.compression.codec=snappy \
    --conf spark.io.compression.codec=snappy \
	--conf spark.kubernetes.driverEnv.LD_LIBRARY_PATH="/lib64/" \
	--conf spark.kubernetes.driverEnv.SAPP_PROFILE=prod \
	--conf spark.kubernetes.driverEnv.SAPP_HDFS="hdfs://172.20.10.5:9000/user/root/df_{id}" \
	--conf spark.kubernetes.driverEnv.SAPP_DFS_N=10 \
	--conf spark.kubernetes.driverEnv.SAPP_COLS_N=10 \
	--conf spark.kubernetes.driverEnv.SAPP_ROWS_N=10 \
    --conf spark.kubernetes.container.image=dimssss/spark-py:2.4.3-0.2 \
    https://raw.githubusercontent.com/Dimss/spark-python2/master/datagen.py

