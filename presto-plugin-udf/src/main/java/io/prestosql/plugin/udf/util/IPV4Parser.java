package io.prestosql.plugin.udf.util;

import com.alibaba.sec.client.FastIPGeoClient;
import com.alibaba.sec.domain.FastGeoConf;
import com.alibaba.sec.exception.FastIPGeoException;
import com.alibaba.sec.license.exception.LicenseException;
import io.prestosql.plugin.udf.scala.OrthogonalGroupV2;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;

/**
 * @author gjm
 * @ClassName IPV4Parser
 * @Date 2021/5/12 5:09 下午
 **/
public class IPV4Parser {
    private static volatile FastGeoConf geoConf = new FastGeoConf();
    private static volatile FastIPGeoClient fastIpGeoClient = null;

    static String GEO_DATA_PATH = "jfs://dp/user/hive/common-lib/ali_ip/";


    private volatile static IPV4Parser mInstance;
    static byte[] lock = new byte[0];

    public static IPV4Parser getInstance() {
        if (mInstance == null) {
            synchronized (lock) {
                if (mInstance == null) {
                    mInstance = new IPV4Parser();
                }
            }
        }
        return mInstance;
    }

    private IPV4Parser() {
    }


    static {

        // load data from local
    /*
        String DATA_FILE_PATH = "ipv4.dex";
        InputStream DATA_FILE_PATH_STREAM = IPV4Parser.class.getClassLoader().getResourceAsStream(DATA_FILE_PATH);
        String LICENSE_FILE_PATH = "ipv4.lic";
        InputStream LICENSE_FILE_PATH_STREAM = IPV4Parser.class.getClassLoader().getResourceAsStream(LICENSE_FILE_PATH);
        try {
            geoConf.setDataInput(DATA_FILE_PATH_STREAM);
            geoConf.setLicenseInput(LICENSE_FILE_PATH_STREAM);
        } catch (Exception e) {
            e.printStackTrace();
        }
        */

        // load data from jfs
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(IPV4Parser.class.getClassLoader())) {
            Configuration config = new Configuration();
            config.set("fs.AbstractFileSystem.jfs.impl", "com.aliyun.emr.fs.jfs.JFS");
            config.set("fs.jfs.impl", "com.aliyun.emr.fs.jfs.JindoFileSystem");
            FileSystem hdfs;
            FileStatus[] fs;
            try {
                hdfs = FileSystem.get(URI.create(GEO_DATA_PATH), config);
                fs = hdfs.listStatus(new Path(GEO_DATA_PATH));
                Path[] listPath = FileUtil.stat2Paths(fs);
                FileSystem fileSystem;
                InputStream is;
                for (Path p : listPath) {
                    fileSystem = FileSystem.get(URI.create(p.toString()), config);
                    is = fileSystem.open(new Path(p.toString()));
                    if (p.getName().endsWith(".dex")) {
                        geoConf.setDataInput(is);
                    } else if (p.getName().endsWith(".lic")) {
                        geoConf.setLicenseInput(is);

                    }
                }

            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        //指定sdk返回字段，不指定字段默认返回全部，可节省内存开销

        HashSet<String> set = new HashSet<>(Arrays.asList(
                "country", "province", "province_code", "city", "city_code", "county",
                "county_code", "isp", "isp_code", "routes", "longitude", "latitude"
        ));

        geoConf.setProperties(set);
        //空值字段，不在结果中返回
        geoConf.filterEmptyValue();
        //通过构造方法初始化（非单例，旧版SDK中使用）
        //FastIPGeoClient fastIpGeoClient = new FastIPGeoClient(geoConf);
        //通过静态方法（单例）, 推荐使用该方法，sdk需升级到最新版本
        try {
            fastIpGeoClient = FastIPGeoClient.getSingleton(geoConf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String findipv4(String ip) {
        String result = null;
        try {
            result = fastIpGeoClient.search(ip);
        } catch (LicenseException e) {
            e.printStackTrace();
        } catch (FastIPGeoException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void main(String[] args) {
        IPV4Parser ipv4Parser = new IPV4Parser();
        System.out.println(ipv4Parser.findipv4("221.206.131.10"));
        System.out.println(ipv4Parser.findipv4("120.133.42.134"));
        System.out.println(ipv4Parser.findipv4("112.224.6.225"));
    }

}
