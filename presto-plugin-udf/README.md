presto plugin  udf  

doc  
https://prestodb.io/docs/current/develop/spi-overview.html


Deploying a Custom Plugin  

mkdir /presto_home/lib/plugin/udf

```
部署包  
presto-plugin-udf-331.jar  
presto-plugin-udf-331-services.jar  

依赖  
commons-codec-1.9.jar  
guava-26.0-jre.jar  
```

In order for Presto to pick up the new plugin, you must restart Presto.




