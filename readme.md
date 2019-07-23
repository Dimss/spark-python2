# SPARK on OCP

The goal of this POC is to learn, estimate and tests Spark application on OCP.

### The Stack 
- OCP 3.9
- Spark 2.4.3 
- Python 3

### Prerequisite
- OCP cluster
- Spark container image for particular language, in this case Python 3
- Spark 2.4.3

#### Prepare OCP cluster
```bash
# Create new project for running Spark apps 
oc new-project spark
``` 

```bash
# Add node selector for spark workers on the namespace level
oc edit namespace spark 
apiVersion: project.openshift.io/v1
kind: Project
metadata:
  annotations:
    openshift.io/node-selector: purpose=spark
    ...

```

```bash
# Check if any limits are configured by default for the spark project  
oc get limits
# Delete any defined limits (for POC there is no need to limit CPU/Memory of workers pods) 
oc delete limits core-resource-limits
```    

```bash
# To keep things simple for a POC, allow executing Spark application with anyuid policy   
oc adm policy add-scc-to-user anyuid -z default -n spark
```

```bash
# Create service account for executing Spark apps, as of POC use clusterAdmin
apiVersion: authorization.openshift.io/v1
kind: ClusterRoleBinding
metadata:
  name: spark-default-sa-spark
roleRef:
  name: cluster-admin
subjects:
- kind: ServiceAccount
  name: default
  namespace: spark
```

### Create spark docker image
```bash
cd /path/to/your/spark/installation
./bin/docker-image-tool.sh -r docker.io/dimssss -t 0.1  build
```

### Run your spark app on OCP
```bash
# Run without dynamic allocation
cd /path/to/your/spark/installation 
bin/spark-submit \
    --master k8s://https://devmaster.resource.bank:8443 \
    --deploy-mode cluster \
    --name spark-k8s-test \
    --conf spark.kubernetes.namespace=spark \
    --conf spark.executor.instances=3 \
    --conf spark.kubernetes.container.image=docker-external.artifactory.resource.bank/spark-py:v2.4.3-v3 \
    /tmp/sparkapp1.py

```  

```bash
# Run with dynamic allocation
# Please note, current method doesn't supported yet by Spark. It's here only as an example.
# See the following Spark open issue: https://issues.apache.org/jira/browse/SPARK-24432

# Frist you've to build docker image and deploy External Shuffle service
cd shuffle
# Build shuffle docker
docker build -t your-shuffle-docker .
# Deploy shuffle service
oc create -f dep.yaml 

# Second execute the spark apps
cd /path/to/your/spark/installation 
bin/spark-submit \
    --master k8s://https://devmaster.resource.bank:8443 \
    --deploy-mode cluster \
    --name spark-k8s-test \
    --conf spark.kubernetes.namespace=spark \
    --conf spark.dynamicAllocation.initialExecutors=1 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.shuffle.service.enabled=true \
    --conf spark.kubernetes.shuffle.namespace=spark \
    --conf spark.kubernetes.shuffle.labels="app=spark-shuffle" \
    --conf spark.kubernetes.container.image=docker-external.artifactory.resource.bank/spark-py:v2.4.3-v3 \
    /tmp/sparkapp1.py
```  
  

# DATAGEN and DATAPROC
```bash
 # DATAGEN
 bin/spark-submit \
    --master k8s://https://devmaster.resource.bank:8443 \
    --deploy-mode cluster \
    --name spark-k8s-test \
    --conf spark.kubernetes.namespace=spark \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.driverEnv.SAPP_PROFILE=prod \
    --conf spark.kubernetes.driverEnv.SAPP_HDFS="hdfs://172.22.195.54:9000/user/root/df_{id}" \
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark.mount.path=/tmp/pv \
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark.options.claimName=spark \
    --conf spark.kubernetes.container.image=docker-ext.artifactory.resource.bank/spark-py:v2.4.3-centos7-0.2 \
    /tmp/pv/datagen.py
```
```bash
 # DATAPROC
 bin/spark-submit \
    --master k8s://https://devmaster.resource.bank:8443 \
    --deploy-mode cluster \
    --name spark-k8s-test \
    --conf spark.kubernetes.namespace=spark \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.driverEnv.SAPP_PROFILE=prod \
    --conf spark.kubernetes.driverEnv.SAPP_HDFS_HOST=172.22.195.54 \
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark.mount.path=/tmp/pv \
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.spark.options.claimName=spark \
    --conf spark.kubernetes.container.image=docker-ext.artifactory.resource.bank/spark-py:v2.4.3-centos7-0.2 \
    /tmp/pv/dataproc.py
```
