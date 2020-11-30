# Deploying a Custom Plugin
presto plugin  udf  

### doc  
https://prestodb.io/docs/current/develop/spi-overview.html


### plugin path 
mkdir /presto_home/plugin/udf

### dependency
```
部署包  
presto-plugin-udf-331.jar  
presto-plugin-udf-331-services.jar  

依赖  
commons-codec-1.9.jar  
guava-26.0-jre.jar  

jackson-core-2.10.0.jar
jackson-databind-2.10.0.jar

geoip2-0.8.0.jar
maxmind-db-0.3.3.jar

dom4j-2.1.1.jar
```



### deploy at presto master   
```
cat /etc/hosts |grep worker|awk '{print $3}' > presto_worker
for host in `cat presto_worker`
do
    ssh  $host  sudo mkdir -p /opt/apps/ecm/service/presto/331-1.0.1/package/presto-331-1.0.1/plugin/udf
    ssh  $host  sudo  chown hadoop:hadoop /opt/apps/ecm/service/presto/331-1.0.1/package/presto-331-1.0.1/plugin/udf
    scp -r /opt/apps/ecm/service/presto/331-1.0.1/package/presto-331-1.0.1/plugin/udf/*.jar   $host:/opt/apps/ecm/service/presto/331-1.0.1/package/presto-331-1.0.1/plugin/udf/
done
```


###  restart 
In order for Presto to pick up the new plugin, you must restart Presto.


### 扩容后同步
source  emr-header-4   10.24.5.125
同步presto plugin  udf 目录
/usr/lib/presto-current/plugin/udf




