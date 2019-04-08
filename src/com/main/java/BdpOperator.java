package com.main.java;

/**
 * Created by 肖英峰--Hansell on 2017/5/26.
 */
public class BdpOperator {
    public static void main(String args[]){
        //System.out.println(args[0]+args[1]+args[2]);
        if(args.length==2){
            if( args[0].equals("create_ds")){
                DataOperator da=  new DataOperator();
                da.createDs(args[1]);
            }
            if( args[0].equals("delete_ds")){
                DataOperator da=  new DataOperator();
                da.deleteDs(args[1]);
            }

        }
        if(args.length==3){
            if( args[0].equals("preview")){
                DataOperator da=  new DataOperator();
                da.setDataSource(args[1]);
                da.setTableName(args[2]);
                da.init();
                da.preview();
            }
            if( args[0].equals("update_table")){
                DataOperator da=  new DataOperator();
                da.setDataSource(args[1]);
                da.initDs();
                da.updateTable(args[2]);
             }
            if( args[0].equals("delete_table")){
                DataOperator da=  new DataOperator();
                da.setDataSource(args[1]);
                da.initDs();
                da.deleteTb(args[2]);
            }
            if(args[0].equals("clean_data")){
                DataOperator da=  new DataOperator();
                da.setDataSource(args[1]);
                da.setTableName(args[2]);
                da.init();
                System.out.println(args[1]+"--"+args[2]);
                da.clean(args[1],args[2]);
            }
            if( args[0].equals("commit_table")){
                DataOperator da=  new DataOperator();
                da.setDataSource(args[1]);
                da.setTableName(args[2]);
                da.init();
                long start = System.currentTimeMillis();
                da.commit();
                long end = System.currentTimeMillis();
                System.out.println("Commit Cost time is "+(end-start));
            }
            if(args[0].equals("add_field")){
              // new DataOperator().addField(args[1],args[2]);
            }
        }
        if(args.length==4){
            if(args[0].equals("bulk_del")) {
                DataOperator da=  new DataOperator();
                da.setDataSource(args[1]);
                da.setTableName(args[2]);
                da.init();
                da.bulkDeleteCommit(args[3]);
                System.out.println("bulk_del");
            }
            if(args[0].equals("bulk_time_is")) {
                DataOperator da=  new DataOperator();
                da.setDataSource(args[1]);
                da.setTableName(args[2]);
                da.init();
                da.optimeBulkEqual(args[3]);
                System.out.println("bulk_like_is");
            }
            if(args[0].equals("uu_id_is")) {
                DataOperator da=  new DataOperator();
                da.setDataSource(args[1]);
                da.setTableName(args[2]);
                da.init();
                da.UuIdEqual(args[3]);
                System.out.println("uu_id_is");
            }

            if(args[0].equals("bulk_time_like")) {
                DataOperator da=  new DataOperator();
                da.setDataSource(args[1]);
                da.setTableName(args[2]);
                da.init();
                da.optimeBulkLike(args[3]);
                System.out.println("bulk_like");
            }
            if(args[0].equals("billdate_like")) {
                DataOperator da=  new DataOperator();
                da.setDataSource(args[1]);
                da.setTableName(args[2]);
                da.init();
                da.billdateBulkLike(args[3]);
                System.out.println("billdate_like");
            }
            if(args[0].equals("bulk_time_not_like")) {
                DataOperator da=  new DataOperator();
                da.setDataSource(args[1]);
                da.setTableName(args[2]);
                da.init();
                da.optimeBulkLikeNot(args[3]);
                System.out.println("bulk_not_like");
            }
            if(args[0].equals("commit_bulk_like")) {
                DataOperator da=  new DataOperator();
                da.setDataSource(args[1]);
                da.setTableName(args[2]);
                da.init();
                da.commitTimeBulkDel(args[3]);
                System.out.println("bulk_like");
            }
        }
        if(args.length==5){
            if(args[0].equals("own_delete")) {
                DataOperator da=  new DataOperator();
                da.setDataSource(args[1]);
                da.setTableName(args[2]);
                da.init();
                System.out.println(args[3]);
                if(args[4].equals("auto")) {
                    da.ownBulkDelete("`fk237acd09` not in('收费','免费')");
                    da.updateTable(args[2]);
                }else{
                    da.ownBulkDelete(args[3]);
                    da.updateTable(args[2]);
                }
                System.out.println("own_delete");
            }
            //load hive table data

            // 自己选择匹配删除的字段
            if(args[0].equals("self_str_like")) {
                DataOperator da=  new DataOperator();
                da.setDataSource(args[1]);
                da.setTableName(args[2]);
                da.init();
                da.selfBulkLike(args[3],args[4]);
                System.out.println("self_str_like");
            }
        }
        else{
            System.out.println("iLLegaArgumentException happend,please check your parameters");
        }
    }
}
