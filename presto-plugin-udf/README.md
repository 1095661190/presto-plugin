presto plugin  udf  

doc  
https://prestodb.io/docs/current/develop/spi-overview.html


Deploying a Custom Plugin  

mkdir /presto_home/plugin/udf

emr-worker-64

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
```



In order for Presto to pick up the new plugin, you must restart Presto.




