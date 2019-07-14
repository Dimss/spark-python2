
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
	
