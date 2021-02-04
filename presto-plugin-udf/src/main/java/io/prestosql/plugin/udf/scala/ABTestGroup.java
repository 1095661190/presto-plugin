package io.prestosql.plugin.udf.scala;

import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;

public class ABTestGroup {

    public ABTestGroup(){}
    private static final Charset charset = StandardCharsets.UTF_8;
    private static long lastTime = 0;

    static final int segment = 10000;

    /**
     * 存放分组区间  范围0-9999
     * 从分组百分比中计算所得  原始值 example (50.00,25.00,25.00)
     * 计算后的值和计算方法
     * [10000*50/100d-1,10000*75/100d-1,10000*100/100d-1]
     */
    static HashMap<String, double[]> rangeMap = new HashMap<String, double[]>();
    /**
     * 存放分组信息
     * hash # [group_id1,group_id2,group_id3]
     */
    static HashMap<String, String> groupMap = new HashMap<String, String>();

    /**
     * <p>初始化mysql配置信息 </p>
     * 读取 experiment_id  hash   group_percents display_ids <br/>
     * 拼接 experiment_id和version 作为map 的  kep <br/>
     * group_percents   代表其所在分组的百分比  <br/>
     * display_ids    [group_id1,group_id2,group_id3]<br/>
     * display_ids 和 group_percents 组装为数组后关系一一对应 通过 group_percents[index]查找display_ids[index]
     * @param url
     * @param user
     * @param password
     */
    public static void initInfo(String url, String user, String password) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            String driver = "com.mysql.jdbc.Driver";
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);

            String sql = new String("select\n"
                    + "      ta.experiment_id\n"
                    + "     ,ta.version\n"
                    + "     -- ,ta.group_ids  -- 分组ID\n"
                    + "     -- ,ta.group_names -- 分组Id对应的分组名称\n"
                    + "     ,ta.group_percents   -- 分组Id对应的百分比例\n"
                    + "     ,ta.display_ids -- 分组Id对应的分组名称\n"
                    + "     -- ,ta.sort_ids\n"
                    + "     ,tb.hash -- 实验对应的hash值\n"
                    + "     ,10000 as segment -- 实验对应的组数\n"
                    + "  from\n"
                    + "  (\n"
                    + "      select\n"
                    + "          ta.id as experiment_id\n"
                    + "          ,ta.version\n"
                    + "          ,GROUP_CONCAT(tb.id order by tb.sort_id) as group_ids\n"
                    + "          ,GROUP_CONCAT(tb.group_name order by tb.sort_id) as group_names\n"
                    + "          ,GROUP_CONCAT(tb.traffic_percent order by tb.sort_id )  as group_percents\n"
                    + "          ,GROUP_CONCAT(tb.display_id order by tb.sort_id) as display_ids\n"
                    + "          ,GROUP_CONCAT(tb.sort_id order by tb.sort_id )  as sort_ids\n"
                    + "      from  experiment ta\n"
                    + "      inner join experiment_group tb\n"
                    + "      on ta.id = tb.experiment_id\n"
                    + "      group by\n"
                    + "          ta.id\n"
                    + "          ,ta.version\n"
                    + "  union all\n"
                    + "      select\n"
                    + "          ta.experiment_id\n"
                    + "          ,ta.version\n"
                    + "          ,GROUP_CONCAT(tb.experiment_group_id order by tb.sort_id)\n"
                    + "          ,GROUP_CONCAT(tb.group_name order by tb.sort_id)\n"
                    + "          ,GROUP_CONCAT(tb.traffic_percent order by tb.sort_id )\n"
                    + "          ,GROUP_CONCAT(tb.display_id order by tb.sort_id) as display_ids\n"
                    + "          ,GROUP_CONCAT(tb.sort_id order by tb.sort_id )  as sort_ids\n"
                    + "      from  experiment_history ta\n"
                    + "      inner join experiment_history_group tb\n"
                    + "      on ta.id = tb.experiment_history_id\n"
                    + "      and ta.experiment_id=tb.experiment_id\n"
                    + "      group by\n"
                    + "          ta.experiment_id\n"
                    + "          ,ta.version\n"
                    + "  ) ta inner join\n"
                    + "  hash_key tb\n"
                    + "  on ta.experiment_id = tb.eid\n"
                    + "  and ta.version = tb.version\n");

            //System.out.println(sql.toString());


            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {

                int experiment_id = resultSet.getInt("experiment_id");
                int version = resultSet.getInt("version");
                //记录各组分流的权重   权重之和100
                String group_percent = resultSet.getString("group_percents");
                //与权重对应的 分组名称   已经按照 sort_id 排序 所以group_percent与display_id 按照 分隔符分割后一一对应
                String display_id = resultSet.getString("display_ids");
                String hash = resultSet.getString("hash");
                //segment = resultSet.getInt("segment");

                String tmpKey = experiment_id + "#" + version;

                String[] group_percents=group_percent.split(",");

                /**
                 * 记录分组的区间范围 [0-9999]
                 * example {4999,7499,9999}
                 * 对应数据区间 [0,4999] (4999,7499] (7499,9999]
                 * 第一个区间为闭区间  后面为左开右闭区间
                 */
                double[] group_range =new double[group_percents.length];
                double total=0;
                for (int i=0;i<group_percents.length;i++){
                    double value=Double.valueOf(group_percents[i]);
                    total+=value;
                    double range=10000*(total/100d)-1;
                    group_range[i]=range;
                }

                String group_info=hash+"#"+display_id;
                groupMap.put(tmpKey,group_info);
                rangeMap.put(tmpKey,group_range);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeAll(connection,preparedStatement,resultSet);
        }

    }



    /**
     * 解析abtest用户的分组id
     * @param experiment_id 实验experiment_id
     * @param version       版本号 每变更一次版本号加1
     * @param uid           用户id 不同表中命名不同 gid or user_id or uid
     * @param flag          value|group  value:计算值，非分组信息    group: 取mysql中的分组名称
     */
    @Description(value = "解析abtest用户的分组id")
    @ScalarFunction
    @SqlType(StandardTypes.VARCHAR)
    public static Slice abtest_group(@SqlType(StandardTypes.INTEGER) long experiment_id, @SqlType(StandardTypes.INTEGER) long version, @SqlType(StandardTypes.VARCHAR) Slice uid, @SqlType(StandardTypes.VARCHAR) Slice flag) {
        long currentTime = System.currentTimeMillis();
        if (lastTime == 0 || currentTime - lastTime > 60 * 60 * 1000||groupMap.size()==0) {
            lastTime = currentTime;
            String url = "jdbc:mysql://rm-wz903o9d17p893q75.mysql.rds.aliyuncs.com:3306/experiment?characterEncoding=utf8&useSSL=false";
            String user = "experiment_test";
            String password = "WC5g3d6hB49fgzIh";

        /*String url = "jdbc:mysql://rm-wz979x250y3723a8q123930.mysql.rds.aliyuncs.com:3306/experiment?characterEncoding=utf8&useSSL=false";
        String user = "experiment_reader";
        String password = "hkQeoPRhhiWG5qXHQqtD";
       */
            initInfo(url, user, password);
        }
        String  group="-1";
        String tmpKey = experiment_id + "#" + version;
        String group_info=groupMap.get(tmpKey);

        if(group_info==null){
            return Slices.copiedBuffer( group, charset);
        }

        String hash=group_info.split("#")[0];
        long retVal= getGroup(uid.toStringUtf8(), hash, segment);

        if (flag.toStringUtf8().contentEquals("value")) {
            return Slices.copiedBuffer( retVal+"", charset);
        } else {
            String display_id=group_info.split("#")[1];
            String[] display_arrs=display_id.split(",");
            double[] range=rangeMap.get(tmpKey);

            Arrays.sort(range);
            //如果key在数组中，则返回搜索值的索引
            //否则返回 -1或 -(插入点 + 1)  插入点:索引键将要插入数组的那一点，即第一个大于该key的元素的索引。
            int postion = Arrays.binarySearch(range,retVal);
            int index=postion>=0?postion:-postion-1;
            group =display_arrs[index];
        }
        return Slices.copiedBuffer( group, charset);

    }



    public static void closeAll(Connection connection, PreparedStatement preparedStatement, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static long getGroup(String userId, String suffix, int segment) {
        return hashUid(userId, suffix) % segment;
    }

    static byte[] digest(byte[] src, int offset, int len, String alg) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance(alg);
        } catch (NoSuchAlgorithmException var6) {
            throw new InternalError("not support " + alg);
        }

        md.update(src, offset, len);
        return md.digest();
    }

    public static byte[] md5Bytes(byte[] b) {
        return md5Bytes(b, 0, b.length);
    }

    public static byte[] md5Bytes(byte[] b, int offset, int len) {
        return digest(b, offset, len, "MD5");
    }

    public static byte[] md5Bytes(String str) {
        try {
            return md5Bytes(str.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException var2) {
            return null;
        }
    }

    public static long hashUid(String userId, String suffix) {
        byte[] bytes = md5Bytes(userId + suffix);
        return Math.abs(Longs.fromByteArray(bytes));
    }


}
