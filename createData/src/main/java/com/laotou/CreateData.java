package com.laotou;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;
import java.util.UUID;

/**
 * 生成数据
 */
public class CreateData {
    public static void main(String[] args) throws IOException {
        StringBuffer str=new StringBuffer();
        final String SLANT="\t";
        String keywords[]=new String[]{"java","spark","hive","hbase","spring","hibernate","html","css","javascript","ajax","jquery"};
        String clickCategorys[]=new String[]{"search","click","order","pay"};
        Random random=new Random();
        String	date	=getData();
        String	action_time	=null;
        for(int i=0;i<10;i++){
            String	user_id	=null;
            String	session_id	=null;
            user_id= String.valueOf(random.nextInt(10)+1);
            session_id= UUID.randomUUID().toString().replace("-","");
            for(int j=0;j<200;j++){
                String	page_id	=null;
                String	search_keyword	=null;
                String	click_category_id	=null;
                String	click_product_id	=null;
                String	order_category_ids	=null;
                String	order_product_ids	=null;
                String	pay_category_ids	=null;
                String	pay_product_ids	=null;
                String	city_id	=null;
                    page_id=String.valueOf(random.nextInt(30)+1);
                    action_time=getActionTime();
                    click_category_id=clickCategorys[random.nextInt(clickCategorys.length)];
                    if(("search").equals(click_category_id)){
                        search_keyword=keywords[random.nextInt(keywords.length)];
                    }else if(("click").equals(click_category_id)){
                        click_product_id= String.valueOf(random.nextInt(10)+1);
                    }else if(("order").equals(click_category_id)){
                        order_category_ids= String.valueOf(random.nextInt(10000)+1);
                        order_product_ids= String.valueOf(random.nextInt(10)+1);
                    }else if(("pay").equals(click_category_id)){
                        pay_category_ids= String.valueOf(random.nextInt(10000)+1);
                        pay_product_ids= String.valueOf(random.nextInt(10)+1);
                    }
                    city_id= String.valueOf(random.nextInt(31)+1);
                str.append(date).append(SLANT).append(user_id).append(SLANT).append(session_id).append(SLANT).append(page_id).append(SLANT).append(action_time)
                        .append(SLANT).append(search_keyword).append(SLANT).append(click_category_id).append(SLANT).append(click_product_id).append(SLANT).append(order_category_ids)
                        .append(SLANT).append(order_product_ids).append(SLANT).append(pay_category_ids).append(SLANT).append(pay_product_ids).append(SLANT).append(city_id).append("\n");
            }
        }
        //写入本地文件
        String fileTxt="/team02/yangfengbing/data/big_data.txt";
        File file=new File(fileTxt);
        if(!file.getParentFile().exists()){
            file.getParentFile().mkdirs();
        }
        if(!file.exists()){
            file.createNewFile();
            FileWriter fw=new FileWriter(file,false);
            BufferedWriter bw=new BufferedWriter(fw);
            System.out.println("写入完成");
            bw.write(String.valueOf(str));
            bw.flush();
            bw.close();
            fw.close();
        }else {
            FileWriter fw=new FileWriter(file,false);
            BufferedWriter bw=new BufferedWriter(fw);
            System.out.println("追加写入完成");
            bw.write(String.valueOf(str));
            bw.flush();
            bw.close();
            fw.close();
        }
    }
    public static String getActionTime(){
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String s = sdf.format(Calendar.getInstance().getTime());
        return s;
    }
    public static String getData(){
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
        String data = sdf.format(Calendar.getInstance().getTime());
        return data;
    }
}
