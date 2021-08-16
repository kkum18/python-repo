#python3  HitLevelAnalysis.py --source_file_loc '/Users/KKUM18/Downloads/sample.tsv' --target_file_loc '/Users/KKUM18/kiran/adobe/'
import pandas as pd
import logging
import argparse
from datetime import datetime

def get_search_keyword(row):
    if row['search_domain']=='GOOGLE':
        return row['referrer'].split('&q=')[1].split('&')[0].upper()
    elif row['search_domain']=='BING':
        return row['referrer'].split('?q=')[1].split('&')[0].upper()
    elif row['search_domain']=='YAHOO':
        return row['referrer'].split('?p=')[1].split('&')[0].upper()
    else:
        return 'unknown'

logger = logging.getLogger("HitLevelDataAnalysis")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

def main(argum_dict):
    source_file_loc = argum_dict['source_file_loc']
    target_file_loc = argum_dict['target_file_loc']

    df = pd.read_csv(source_file_loc, sep='\t')
    target_file_name = target_file_loc + datetime.now().strftime('%Y-%m-%d') + '_SearchKeywordPerformance.tab'

    logger.info('source file details : {}'.format(source_file_loc))
    logger.info('target file details : {}'.format(target_file_name))
    
    df1 = df[['ip', 'event_list', 'product_list', 'referrer', 'page_url']]
    df1.loc[:,'event_list'] = df1.event_list.apply(lambda x: str(x).split(','))
    df1.loc[:,'product_list'] = df1.product_list.apply(lambda x: str(x).split(','))
    df1.loc[:,'page_domain'] = df1.page_url.apply(lambda x: str(x).split('.')[1])
    df1.loc[:,'referrer_domain'] = df1.referrer.apply(lambda x: str(x).split('.')[1])
    #df1['event_list'] = [str(x).split(',') for x in df1['event_list']]
    #df1['product_list'] = [str(x).split(',') for x in df1['product_list']]
    #df1['page_domain'] = [str(x).split('.')[1] for x in df1['page_url']]
    #df1['referrer_domain'] = [str(x).split('.')[1] for x in df1['referrer']]
    
    df2 = df1.explode('product_list').explode('event_list')[['ip', 'event_list', 'product_list']]
    prod_info = df2.where((df2['event_list'] == '1') & (df2['product_list']!='nan')).dropna(how='all').drop_duplicates()
    prod_info['product_category'] = [str(x).split(';')[0] for x in prod_info['product_list']]
    prod_info['product_name'] = [str(x).split(';')[1] for x in prod_info['product_list']]
    prod_info['no_of_items'] = [str(x).split(';')[2] for x in prod_info['product_list']]
    prod_info['no_of_items'] = pd.to_numeric(prod_info['no_of_items'])
    prod_info['product_revenue'] = [str(x).split(';')[3] for x in prod_info['product_list']]
    prod_info['product_revenue'] = [x if x != '' else 0 for x in prod_info['product_revenue']]
    prod_info['total_revenue'] = prod_info.apply(lambda x: x['no_of_items'] * x['product_revenue'], axis=1)
    prod_info['total_revenue'] = pd.to_numeric(prod_info['total_revenue'])
    
    
    dmn_srch = df1.where(df1['page_domain'] != df1['referrer_domain']).dropna(how='all')[['ip','referrer_domain','referrer']]
    dmn_srch = dmn_srch.rename(columns={"referrer_domain":"search_domain"})
    dmn_srch['search_domain'] = dmn_srch['search_domain'].str.upper()
    dmn_srch['search_keyword'] = dmn_srch.apply(lambda row : get_search_keyword(row), axis=1)
    
    final_df = pd.merge(prod_info,dmn_srch, on=["ip"])[['search_domain','search_keyword','total_revenue']]
    output = final_df.groupby(['search_domain','search_keyword'])['total_revenue'].sum().reset_index(name='revenue')
    logger.info("writing file to target location {0}".format(target_file_name))
    output.to_csv(target_file_name, sep='\t', header=True, index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='HitLevelAnalysis')
    parser.add_argument('--source_file_loc', required=True)
    parser.add_argument('--target_file_loc', required=True)
    argum = parser.parse_args()
    argum_dict = vars(argum)
    main(argum_dict)


