package club.cleland.spark_learning.spark_sql.spark_sql_analyze;


import club.cleland.spark_learning.spark_sql.core.UDFJava;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class GenerateLog {

    private String date = "2019-05-02";
    private String timeStamp = "1556791117";
    private ArrayList<Integer> userIds = new ArrayList();
    private ArrayList<Integer> pageIds = new ArrayList<>();
    private final ArrayList<String> sections = new ArrayList<String>(
            Arrays.asList("internet", "tv-show", "international", "sport", "carton", "entertainment"));
    private final ArrayList<String> actions = new ArrayList<>(Arrays.asList("view", "register"));

    public GenerateLog() {

        // 初始化user_id
        for(int i=0; i<1000; i++){
            userIds.add(i);
        }

        // 初始化page_id
        for(int i=0; i<100; i++){
            pageIds.add(i);
        }
    }

    /**
     * 获取resousces里的文件
     * @param file
     * @return
     */
    private static final File getResource(String file) {
        return new File(UDFJava.class.getClassLoader().getResource(file).getPath());
    }

    /**
     * 生成一行日志
     * @return
     */
    public String generate_line_log(){
        Random random = new Random();

        String curUserId = String.valueOf(this.userIds.get(random.nextInt(this.userIds.size())));
        String curPageId = String.valueOf(this.pageIds.get(random.nextInt(this.pageIds.size())));
        String curAction = this.actions.get(random.nextInt(this.actions.size()));
        String curSection = this.sections.get(random.nextInt(this.sections.size()));
        String line = this.date + " " + this.timeStamp + " " + curUserId + " " + curPageId + " " + curSection + " " + curAction;
        return line;
    }

    /**
     * 主函数
     * @param args
     */
    public static void main(String[] args){
        GenerateLog log = new GenerateLog();
        int count = 0;

        FileWriter fw = null;
        try{
            fw = new FileWriter(getResource("access.log"));
            BufferedWriter writer = new BufferedWriter(fw);
            while (count < 20000) {
                // date timestamp userid pageid section action
                String line = log.generate_line_log();
                writer.write(line);
                writer.newLine();
                count ++;
            }
            fw.close();
        }catch (Exception e){
            e.printStackTrace();
        }finally {

        }

    }
}
