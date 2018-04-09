package com.my.test.test3;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class GetData {

    public static void main(String[] args) throws IOException {
        File logFile=new File("F:\\data\\track.log");
        Random random=new Random();
        String[] hosts={"www.taobao.com"};
        String [] session_id={"111111111111","2222222222222222","333333333333333","44444444444444444"
                ,"55555555555555555555555555555","666666666"};
        String [] time ={"2017-1-6 08:40:50","2017-1-6 08:40:53","2017-1-6 08:40:53","2017-1-6 08:46:53","2017-1-6 08:40:13","2017-1-6 08:40:23",
                "2017-1-6 08:40:43","2017-1-6 08:40:33"};

        StringBuffer sb=new StringBuffer();

        for(int i=0;i<50;i++){
            sb.append(hosts[0]+"\t"+session_id[random.nextInt(6)]+"\t"+time[random.nextInt(8)]+"\n");
        }

        if(!logFile.exists()){
            logFile.createNewFile();
        }
        byte[] b=sb.toString().getBytes();

        FileOutputStream fs;
        fs=new FileOutputStream(logFile);
        fs.write(b);
        fs.close();

    }

}
