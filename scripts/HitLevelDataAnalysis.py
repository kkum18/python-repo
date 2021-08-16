#spark-submit --master yarn --deploy-mode cluster  --driver-memory 20G --executor-memory 20G  --executor-cores 4 --conf spark.yarn.executor.extraClassPath=./ --conf spark.executor.memoryOverhead=1500 --conf spark.dynamicAllocation.enabled=false ./HitLevelDataAnalysis.py --source_file_loc 's3://nike-retail-managed/test/kiran/sample.tsv' --target_file_loc 's3://nike-retail-managed/test/kumar/'
import subprocess
import logging
import argparse
from datetime import datetime
from pyspark.sql import SparkSession

spark= SparkSession.builder.appName("HitLevelDataAnalysis").getOrCreate()

logger = logging.getLogger("HitLevelDataAnalysis")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

def main(argum_dict):
    source_file_loc = argum_dict['source_file_loc']
    target_file_loc = argum_dict['target_file_loc']
    local_file_loc = "/user/hadoop/"
    target_file_name = target_file_loc + datetime.now().strftime('%Y-%m-%d') + '_SearchKeywordPerformance.tab'

    logger.info('source file details : {}'.format(source_file_loc))
    logger.info('target file details : {}'.format(target_file_name))


    # reading from source file and register temporary table 
    spark.read.csv(source_file_loc, sep='\t', header=True).registerTempTable("hit_level_source")

    # get domain name from referer column and page column.
    # split event_list and product_list which are seperated by comma to convert to arrays
    spark.sql("""select  
                    ip, 
                    SPLIT(referrer,'\\\\.')[1] as referrer_domain,
                    SPLIT(page_url,'\\\\.')[1] as page_domain,
                    SPLIT(event_list,',') as event_list, 
                    referrer, 
                    SPLIT(product_list,',') as product_list
                    from hit_level_source""").registerTempTable("hit_level_source_1")

    # get search keyword from referrer column where reference domain is not same as page domain
    # using distinct to handle cases where a user bought more than one product in the same day
    # register as temp table dmn_srch
    spark.sql("""select
                    distinct
                    ip,
                    upper(referrer_domain) as search_domain,
                    (case 
                        when upper(referrer_domain) = 'GOOGLE' THEN upper(replace(SPLIT(substr(referrer,instr(referrer,'&q=') + 3),'&')[0], '+',' '))
                        when upper(referrer_domain) = 'BING' THEN upper(replace(SPLIT(substr(referrer,instr(referrer,'?q=') + 3),'&')[0], '+',' '))
                        when upper(referrer_domain) = 'YAHOO' THEN upper(replace(SPLIT(substr(referrer,instr(referrer,'?p=') + 3),'&')[0], '+',' '))
                    end) as search_keyword
                    from hit_level_source_1
                    where referrer_domain != page_domain""").registerTempTable("dmn_srch")

    # explode product list to convert from columns to rows and register as temp table prod_info
    spark.sql("""select 
                    ip, 
                    event_list, 
                    SPLIT(products_list,';')[0] as product_category,
                    SPLIT(products_list,';')[1] as product_name, 
                    coalesce(SPLIT(products_list,';')[2], 0) as no_of_items,
                    coalesce(SPLIT(products_list,';')[3],0) as product_revenue
                    from hit_level_source_1 lateral view outer explode(product_list) as products_list 
                    where products_list is not null  and
                    array_contains(event_list,'1')""").registerTempTable("prod_info")

    # join both prod_info table and dmn_srch temp tables and write to hdfs location
    spark.sql("""select
                    search_domain,
                    search_keyword,
                    sum(product_revenue) as revenue
                    from prod_info inner join dmn_srch
                    on prod_info.ip = dmn_srch.ip
                    group by search_domain, search_keyword
                    order by revenue desc""").coalesce(1).write.option("header","true").option("sep","\t").mode("overwrite").csv(local_file_loc)

    # copy file from HDFS to S3 and rename the file
    try:
        logger.info("copying file from {0} to s3 location {1}".format(local_file_loc, target_file_name))
        cmd = "hadoop distcp -overwrite {0}part* {1}".format(local_file_loc, target_file_name)
        subprocess.call(cmd, shell=True)
    except:
        logger.info("Error : failed to copy file {0} to s3 location {1}".format(local_file_loc, target_file_name))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='HitLevelDataAnalysis')
    parser.add_argument('--source_file_loc', required=True)
    parser.add_argument('--target_file_loc', required=True)
    argum = parser.parse_args()
    argum_dict = vars(argum)
    main(argum_dict)