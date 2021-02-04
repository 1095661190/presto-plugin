package io.prestosql.plugin.udf.scala;

import io.airlift.slice.Slice;
import io.prestosql.plugin.udf.util.EncryptionByMD5;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class MaintenanceGroup {
    public MaintenanceGroup() {}

    static String path = "jfs://dp/user/hive/common-lib/configxml/";
    static String suffix = "_maintenance.xml";

    static String MODE_KEY_PRE = "orthogonal_group";
    static String MODE_KEY_PRE_V2 = "orthogonal_group_v2";


    /**
     * key: fileName#id
     * value: [nameStr,modeStr,aliasStr,childStr]
     */
    static HashMap<String, List<String>> eleDict = new HashMap<String, List<String>>();
    /**
     * key: fileName#id
     * value: [chil1,chil2,child3,....] 实际的child 的key值
     */
    static HashMap<String, List<String>> validChild = new HashMap<String, List<String>>();
    /**
     * key: fileName#id
     * value: [weight1,weight2,weight3,....]//存放的是权重对应的分组边界
     */
    static HashMap<String, List<Integer>> validWeight = new HashMap<String, List<Integer>>();
    /**
     * key fileName#id
     */
    static HashMap<String, Integer> totalWeight = new HashMap<String, Integer>();


    /**
     * 配置信息 初始化 <br/>
     * full path jfs://dp/user/hive/common-lib/configxml/yyyy-MM-dd_maintenance.xml
     *
     * @param date yyyy-MM-dd
     */
    public static void initInfo(String date) {
        String fileName = date + suffix;
        String file = path + fileName;
        SAXReader reader = new SAXReader();
        Document document;
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(OrthogonalGroupV2.class.getClassLoader())) {
            Configuration config = new Configuration();
            config.set("fs.AbstractFileSystem.jfs.impl", "com.aliyun.emr.fs.jfs.JFS");
            config.set("fs.jfs.impl", "com.aliyun.emr.fs.jfs.JindoFileSystem");
            try {
                FileSystem fs = FileSystem.get(URI.create(file), config);
                if (!fs.exists(new Path(file))) {
                    throw new Exception(String.format("查询日期 %s 无对应的xml配置文件 %s", date, fileName));
                }
                InputStream is = fs.open(new Path(file));
                document = reader.read(is);

                Element root = document.getRootElement();
                List<Element> eleList = root.elements();

                for (Element ele : eleList) {
                    List<Attribute> attrList = ele.attributes();
                    String childStr = "";
                    List<String> childList = new ArrayList<String>();
                    List<String> vchildList = new ArrayList<String>();
                    List<Integer> wigthList = new ArrayList<Integer>();
                    int weightSum = 0;
                    String nameStr = "";
                    String modeStr = "";
                    String aliasStr = "";
                    String id = "";
                    String dealKey = "";
                    for (Attribute attr : attrList) {
                        String attrKey = attr.getName();
                        if (attrKey.startsWith("child")) {
                            dealKey = attrKey.substring(0, 5) + "000000".substring(0, 3 - attrKey.substring(5).length()) + attrKey.substring(5);
                            childList.add(dealKey + "=" + attrKey + "=" + attr.getData().toString());
                        } else if (attrKey.contentEquals("mode")) {
                            modeStr = attr.getData().toString();
                        } else if (attrKey.contentEquals("name")) {
                            nameStr = attr.getData().toString();
                        } else if (attrKey.contentEquals("id")) {
                            id = attr.getData().toString();
                        }
                    }
                    if (modeStr.indexOf(";") >= 0) {
                        aliasStr = modeStr.split(";")[1];
                    }
                    if (modeStr.startsWith(MODE_KEY_PRE) && childList.size() > 0) {
                        Collections.sort(childList);
                        for (String str : childList) {
                            childStr += str.split("=")[2];
                            int weight = Integer.parseInt(str.split(";")[1]);
                            if (weight > 0) {
                                //权重>0 的原始childN
                                vchildList.add(str.split("=")[1]);
                                //一组权重之和
                                weightSum += weight;
                                //每个有效的权重
                                wigthList.add(weightSum);
                            }
                        }

                        if (weightSum > 0) {
                            List<String> tmpValue = new ArrayList<String>();
                            tmpValue.add(nameStr);
                            tmpValue.add(modeStr);
                            tmpValue.add(aliasStr);
                            tmpValue.add(childStr.replace(",", "+").replace(";", "_"));
                            String tmpKey = fileName + "#" + id;
                            eleDict.put(tmpKey, tmpValue);
                            validChild.put(tmpKey, vchildList);
                            totalWeight.put(tmpKey, weightSum);
                            if (modeStr.toLowerCase().split(";")[0].contentEquals(MODE_KEY_PRE_V2)) {
                                List<Integer> tmWeight = new ArrayList<Integer>();
                                for (Integer i : wigthList) {
                                    //每组的上界不包括
                                    tmWeight.add(10000 * i / weightSum);
                                    validWeight.put(tmpKey, tmWeight);
                                }
                            } else if (modeStr.toLowerCase().split(";")[0].contentEquals(MODE_KEY_PRE)) {
                                //每组的上界不包括
                                validWeight.put(tmpKey, wigthList);
                            }
                        }

                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 获取用户在某日对应线上最后版本的分组信息
     *
     * @param experiment_id 实验id
     * @param uid_str       用户id
     * @param flag_str      param [ value | group ]
     *                      value:计算值，非分组信息    group: 配置文件的分组,注意返回的是分组中childNum 的Num  这样才能保证返回的信息都是数字
     * @param date_str      日期格式  yyyy-MM-dd
     * @return
     */
    @Description(value = "")
    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long maintenance_group(@SqlType(StandardTypes.INTEGER) long experiment_id, @SqlType(StandardTypes.VARCHAR) Slice uid_str, @SqlType(StandardTypes.VARCHAR) Slice flag_str, @SqlType(StandardTypes.VARCHAR) Slice date_str) {

        String date = date_str.toStringUtf8();
        String flag = flag_str.toStringUtf8();
        String uid = uid_str.toStringUtf8();

        if (eleDict.isEmpty()) {
            try {
                //无使用价值  只是防止异常情况eleDict为空的情况下  每次调用evaluate()加载xml文件
                List value_test=new ArrayList();
                eleDict.put("test",value_test);
                initInfo(date);
            } catch (Exception e) {
                e.printStackTrace();
                return -1;
            }
        }

        String fileName = date + suffix;
        String key = fileName + "#" + experiment_id;


        //文件及ID的组合不存在 返回 -1
        if (!eleDict.containsKey(key)) {
            //System.out.println(String.format("experiment_id %s not exist in file %s", experiment_id,fileName));
            return -1;
        } else {
            List<String> eleInfo = eleDict.get(key);
            String nameStr = eleInfo.get(0);
            String modeStr = eleInfo.get(1);
            String aliasStr = eleInfo.get(2);
            String childStr = eleInfo.get(3);


            String result = null;
            if (modeStr.startsWith(MODE_KEY_PRE) && aliasStr.contentEquals("")) {
                result = EncryptionByMD5.getMD5((nameStr + EncryptionByMD5.getMD5(childStr.getBytes()) + uid).getBytes());
            } else if (modeStr.startsWith(MODE_KEY_PRE)) {
                result = EncryptionByMD5.getMD5((aliasStr + uid).getBytes());
            }
            long val = Long.parseLong(result.substring(24), 16);
            long retVal = -1;
            List<Integer> weightList = validWeight.get(key);
            int weightSize = weightList.size();
            List<String> childList = validChild.get(key);
            int totalw = totalWeight.get(key);
            long rr = 100000;
            if (modeStr.toLowerCase().split(";")[0].contentEquals(MODE_KEY_PRE)) {
                rr = val % totalw;
            }
            if (modeStr.toLowerCase().split(";")[0].contentEquals(MODE_KEY_PRE_V2)) {
                rr = val % 10000;
            }

            for (int i = 0; i < weightSize; i++) {
                if (rr < weightList.get(i)) {
                    if (flag.contentEquals("value")) {
                        retVal = rr;
                        return retVal;
                    } else {
                        retVal = Long.parseLong(childList.get(i).substring(5));
                        return retVal;
                    }
                }
            }
            return retVal;
        }
    }
}
