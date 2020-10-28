package io.prestosql.plugin.udf.scala;

import java.util.ArrayList;

import java.util.Collections;
import java.util.*;
import java.net.URI;
import java.io.*;

import io.prestosql.plugin.udf.util.EncryptionByMD5;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public  class OrthogonalGroupV2{
	
	/**
	 *  key: fileName#id 
	 *  value: [nameStr,modeStr,aliasStr,childStr]
	 */
	HashMap<String,List<String>> eleDict = new HashMap<String,List<String>>();
	/**
	 *  key: fileName#id 
	 *  value: [chil1,chil2,child3,....] 实际的child 的key值 
	 */
	HashMap<String,List<String>> validChild = new HashMap<String,List<String>>();
	/**
	 *  key: fileName#id 
	 *  value: [weight1,weight2,weight3,....]//存放的是权重对应的分组边界 
	 */
	HashMap<String,List<Integer>> validWeight = new HashMap<String,List<Integer>>();
	/**
	 * 
	 * key fileName#id
	 */
	HashMap<String,Integer> totalWeight = new HashMap<String,Integer>();
	
	//ai_yws
	public void initInfo(String filePath)  {

		Configuration config = new Configuration();

		String file=filePath; //完整的文件路径 hdfs:///abc/abc/abc/aaa.xml 
		String fileName=filePath.split("/")[filePath.split("/").length-1].split("\\.")[0]; //得到文件名aaa 注意文件名由  version_原文件名 组成
		SAXReader reader = new SAXReader();
        Document document;
		try {
			FileSystem fs = FileSystem.get(URI.create(file),config);
			//读取文件
			InputStream is = fs.open(new Path(file));
			//InputStream is = this.getClass().getClassLoader().getResourceAsStream("./"+fileName+".xml");

			document = reader.read(is);  //.read(InputStream);
        
	        Element root=document.getRootElement();
	        List<Element> eleList = root.elements();
	        
	        for(Element ele:eleList) {
	        	List<Attribute> attrList = ele.attributes();
	        	String childStr="";
	        	List<String> childList = new ArrayList<String>();
	        	List<String> vchildList = new ArrayList<String>();
	        	List<Integer> wigthList = new ArrayList<Integer>();
	        	int weightSum = 0;
	        	String nameStr="";
	        	String modeStr="";
	        	String aliasStr="";
	        	String id="";
	        	String dealKey="";
	        	for(Attribute attr:attrList) {
	        		String attrKey=attr.getName();
	        		if(attrKey.startsWith("child")) {
	        			//childStr+=attr.getData().toString();
	        			dealKey=attrKey.substring(0,5) + "000000".substring(0,3-attrKey.substring(5).length()) + attrKey.substring(5);
	        			childList.add(dealKey + "=" + attrKey + "=" + attr.getData().toString());
	        		} else if(attrKey.contentEquals("mode")) {
	        			modeStr=attr.getData().toString();
	        		} else if(attrKey.contentEquals("name")) {
	        			nameStr=attr.getData().toString();
	        		} else if (attrKey.contentEquals("id")) {
	        			id=attr.getData().toString();
	        		}
	        	}
	        	if(modeStr.indexOf(";")>=0) {
	        		aliasStr=modeStr.split(";")[1];
	        	}
	        	if(modeStr.startsWith("orthogonal_group")&&childList.size()>0) {
	        		Collections.sort(childList);
	        		// System.out.println(id+"\t"+ childList.size()+"\t"+ childList);
		        	for(String str :childList) {
		        		childStr += str.split("=")[2];
		        		
		        		int weight=Integer.parseInt(str.split(";")[1]);
		        		
		        		if(weight>0) {
		        			vchildList.add(str.split("=")[1]); //权重>0 的原始childN
		        			weightSum+=weight; //一组权重之和
		        			wigthList.add(weightSum); //每个有效的权重
		        		}
		        	}
		        	
		        	if(weightSum>0) {
			        	List<String> tmpValue= new ArrayList<String>();
			        	tmpValue.add(nameStr);
			        	tmpValue.add(modeStr);
			        	tmpValue.add(aliasStr);
			        	tmpValue.add(childStr.replace(",", "+").replace(";","_"));
			        	
			        	String tmpKey=fileName+"#"+id;
			        	
			        	eleDict.put(tmpKey,tmpValue) ;
			        	
			        	validChild.put(tmpKey, vchildList);
			        	
			        	totalWeight.put(tmpKey, weightSum);
			        	
			        	if(modeStr.toLowerCase().split(";")[0].contentEquals("orthogonal_group_v2")) {
			        		List<Integer> tmWeight = new ArrayList<Integer>();
			        		for( Integer i : wigthList) {
			        			tmWeight.add(10000*i/weightSum);//每组的上界不包括
			        			validWeight.put(tmpKey,tmWeight);
			        		}
			        	} else if(modeStr.toLowerCase().split(";")[0].contentEquals("orthogonal_group")) {
			        		validWeight.put(tmpKey,wigthList);//每组的上界不包括
			        	}
		        	}
		        	
	        	}
	        }
		} catch (Exception e) {
				// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public OrthogonalGroupV2()  {
		Configuration config = new Configuration();
		FileSystem hdfs;
		FileStatus[] fs;
		try {
			//String path = "hdfs://tcdh-name-service-1/user/hive/common-lib/xml_config/";
			//String path = "file:///Users/happyelements/eclipse-workspace/DWUDF/config/";
			String path = "jfs://dp/user/hive/common-lib/xml_config/";
			hdfs = FileSystem.get(URI.create(path),config);
			fs = hdfs.listStatus(new Path(path));
			Path[] listPath = FileUtil.stat2Paths(fs);
			for(Path p : listPath) {
				if(p.getName().endsWith(".xml")) {
					this.initInfo(p.toString());
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * https://config.happyelements.cn/config/list.do?appName=animal_mobile_prod0&fileName=ai_maintenance.xml
	 * https://config.happyelements.cn/config/list.do?appName=animal_mobile_prod0&fileName=maintenance.xml
	 * https://config.happyelements.cn/config/view.do?appName=animal_mobile_dev0&fileName=level_config_group.xml
	 * @param fileName  id 所在的文件名称 现在有3个  ai_maintenance.xml   maintenance.xml  level_config_group.xml（目前还没有具体实验） 
	 * @param id  文件中具体的实验id
	 * @param uid 实际数据中的 gid or user_id or uid 同一个意思只是在不同表中的命名有差异 
	 * @param flag  value:计算值，非分组信息    group: 最新配置文件的分组,注意返回的是分组中childNum 的Num  这样才能保证返回的信息都是数字
	 * @param fileVersion fileName对应的版本  文件的配置信息会发生变更，每变更一次版本号加1
	 * @return  数值型 
	 */
	@Description(value = "")
	@ScalarFunction
	@SqlType(StandardTypes.INTEGER)
	public long orthogonal_group_v2(@SqlType(StandardTypes.VARCHAR)String fileName,@SqlType(StandardTypes.INTEGER)Integer id,@SqlType(StandardTypes.VARCHAR)String uid,@SqlType(StandardTypes.VARCHAR)String flag,@SqlType(StandardTypes.INTEGER)Integer fileVersion) { //
		
		String key = fileVersion+"_"+fileName+"#"+id.toString() ;

		//如果文件及ID的组合不存在，返回 -1 表达不存在
		if(!eleDict.containsKey(key)) {
			return -1 ;
		} else 
		{
			List<String> eleInfo= eleDict.get(key);
			
			//System.out.println(eleInfo); 
			String nameStr = eleInfo.get(0);
			String modeStr = eleInfo.get(1);
			String aliasStr = eleInfo.get(2);
			String childStr = eleInfo.get(3);
			
			String result=null ;
	    	if(modeStr.startsWith("orthogonal_group")&&aliasStr.contentEquals("")) {
	    		result= EncryptionByMD5.getMD5((nameStr+EncryptionByMD5.getMD5(childStr.getBytes())+uid).getBytes());
	    	}else if (modeStr.startsWith("orthogonal_group")) {
	    		result=EncryptionByMD5.getMD5((aliasStr+uid).getBytes());
	    	}
	    	long val = Long.parseLong(result.substring(24),16);
	    	
	    	long retVal=-1;
	    	List<Integer> weightList = validWeight.get(key);
	    	int weightSize=weightList.size();
	    	List<String> childList = validChild.get(key);
	    	
	    	int totalw = totalWeight.get(key);

	    	long rr=100000;
	    	if(modeStr.toLowerCase().split(";")[0].contentEquals("orthogonal_group")) {
	    		 rr = val%totalw; 
	    	} if (modeStr.toLowerCase().split(";")[0].contentEquals("orthogonal_group_v2")) {
	    		 rr = val%10000; 
	    	}
	    	
    		for(int i = 0 ;i<weightSize;i++) {
				if(rr<weightList.get(i)) {
					if(flag.contentEquals("value")) {
						retVal = rr;
						return retVal ;
					}
					else
					{
						retVal= Long.parseLong(childList.get(i).substring(5));
						return retVal ;
					}
				}	
    		}
    		return retVal ;
		}
	}


}
