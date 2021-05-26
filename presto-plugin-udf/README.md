# Deploying a Custom Plugin
presto plugin  udf  

### doc  
https://prestodb.io/docs/current/develop/spi-overview.html

### wiki
http://wiki.happyelements.net/display/BI/presto


### plugin path 
mkdir /$PRESTO_HOME/plugin/udf

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
    ssh  $host  sudo mkdir -p /usr/lib/presto-current/plugin/udf/
    ssh  $host  sudo  chown hadoop:hadoop /usr/lib/presto-current/plugin/udf/
    scp -r /usr/lib/presto-current/plugin/udf/*.jar   $host:/usr/lib/presto-current/plugin/udf/
done
```


###  restart 
In order for Presto to pick up the new plugin, you must restart Presto.


### 扩容后同步
source  emr-header-4   10.24.5.125
同步presto plugin  udf 目录
/usr/lib/presto-current/plugin/udf


### udf 

- geoip
     
        select geoip('country', '8.8.8.8')
- ctimezone

- unbase64

- presto md5   (add  hive_md5)
  
        用途:presto提供的md5函数使用比较繁琐  所以提供同hive一样便捷的函数 名称 hive_md5
        hive md5 
        select md5('aaaa')
        
        presto  自带 md5
        select lower(to_hex(md5(to_utf8('aaaa'))))
        
        presto提供类似hive的md5函数 名称 : hive_md5
        select hive_md5('aaaa')

- OrthogonalGroup    OrthogonalGroupV2

        用途:正交分组函数  

        正交分组 分组策略函数
        http://wiki.happyelements.net/pages/viewpage.action?pageId=54159084

        正交分组 分组策略函数--添加版本控制参数
        http://wiki.happyelements.net/pages/viewpage.action?pageId=60860019

- abtest_group

        用途:abtesting实验获取用户分组信息   

        select abtest_group(experiment_id  , version   , uid   , flag) from ods_dp.user_new where ds='2021-01-25' limit 1; 
        experiment_id 实验experiment_id
        version 版本号 每变更一次版本号加1
        uid 用户id 不同表中命名不同 gid or user_id or uid
        
        flag   param[ value | group ]
               value:计算值，非分组信息       (uid+hash) hash之后对segment(默认10000)取模
               group: 取mysql中的分组名称
    
       example sql:    select abtest_group(67 , 7 , '11713891' , 'group' ) from ods_dp.user_new where ds='2021-01-25' limit 1;

- maintenance_group 

        用途:获取用户在某日对应线上最后版本的分组信息 
        experiment_id or  xml  not exist  return -1
        
        select maintenance_group( experiment_id, uid  ,flag , date ) as type 
        experiment_id   uid   flag  参数含义请参考 abtesting
        date 日期  格式 yyyy-MM-dd  日期必须<=当前日期  xml 文件暂定 保留前500个
        
        
        
- ipv4 
       
        使用阿里 云解析DNS 产品  IP地理位置库 解析IP
        [阿里云  IP地理位置库  帮助文档](https://help.aliyun.com/document_detail/153347.html?spm=a2c1d.8251892.help.dexternal.2b2c5b76H5HFJu) 
        依赖 
            ipv4.dex离线库文件  
            ipv4.lic授权文件   
            离线SDK文件 v1.20210430161742.jar (从阿里云下载手动添加到项目)
        
        存放路径   jfs://dp/user/hive/common-lib/ali_ip
        
        注意：
            1、每月更新 ipv4.dex、ipv4.lic
            2、仅支持ipv4
            