from datetime import datetime as dt, timedelta as td
import querying as q # Custom module for querying Prod DB
import mpanel as mp # Custom module for transforming JSON response of Mixpanel API Library
import pandas as pd
import MySQLdb
import sys
import os

now = dt.now()
s_form = '%Y-%m-%d'
s_form_full = '%Y-%m-%d %H:%M:%S'
s_form_start = '%Y-%m-%d 00:00:00'
s_form_end = '%Y-%m-%d 23:59:59'
history_start = '2017-04-01' # Earliest relevant point in time

#Method to Query the DB for Users that are applicable for the Mixpanel Cohort & time Timeline conditions
def get_user_data(start_point='SignUp', users=None, end_date=now.strftime(s_form)):
    if start_point == 'SignUp':
        statement1 = 'SELECT U.user_id UserId,'\
                     ' CONCAT(MONTHNAME(U.sign_up_date),"-",YEAR(U.sign_up_date)) SignUp,'\
                     ' YEAR(U.sign_up_date) SORT1, MONTH(U.sign_up_date) SORT2'\
                     ' FROM audit.Users U'\
                     ' WHERE U.sdk_merchant = "niki"'\
                     ' AND U.is_high_frequency = FALSE'\
                     ' #USER_CLAUSE'
        statement2 = 'SELECT CONCAT(MONTHNAME(U.sign_up_date),"-",YEAR(U.sign_up_date)) SignUp,'\
                     ' COUNT(DISTINCT U.user_id) AtleastOnce,'\
                     ' YEAR(U.sign_up_date) SORT1, MONTH(U.sign_up_date) SORT2'\
                     ' FROM audit.Users U INNER JOIN audit.Orders O on U.user_id = O.user_id'\
                     ' WHERE U.sdk_merchant = "niki"'\
                     ' AND U.is_high_frequency = FALSE'\
                     ' AND O.status REGEXP "succ|conf|compl"'\
                     ' #USER_CLAUSE'\
                     ' GROUP BY SignUp'
    else:
        statement1 = 'SELECT CONCAT(MONTHNAME(t.FirstOrderD),"-",YEAR(t.FirstOrderD)) FirstOrder, COUNT(DISTINCT t.UserId) UserCount,'\
                    ' YEAR(t.FirstOrderD) SORT1, MONTH(t.FirstOrderD) SORT2 FROM ('\
                    ' SELECT U.user_id UserId, MONTH(MIN(O.order_creation_date)) MonthNo, YEAR(MIN(O.order_creation_date)) YearNo,'\
                    ' MIN(O.order_creation_date) FirstOrderD'\
                    ' FROM audit.Users U LEFT JOIN audit.Orders O'\
                    ' ON U.user_id = O.user_id'\
                    ' WHERE U.sdk_merchant = "niki" AND O.status REGEXP "succ|compl|conf"'\
                    ' AND O.order_creation_date > "'+history_start+'"'\
                    ' AND U.is_high_frequency = FALSE'\
                    '#USER_CLAUSE'\
                    ' GROUP BY U.user_id'\
                    ' ) as t'\
                    ' GROUP BY FirstOrder'\
                    ' ORDER BY YEAR(t.FirstOrderD) ASC, MONTH(t.FirstOrderD) ASC'
    if users:
        user_clause = ' AND U.user_id IN ("#USERIDS")'
        i = 0
        total = len(users)
        jump = 1000
        result1 = pd.DataFrame()
        # result2 = pd.DataFrame()
        print(users[:5])
        while i<total:
            if i+jump <= total:
                end = i+jump
            else:
                end = total
            temp_users = users[i:end]
            i = end
            print('Doing',i,'of',total)
            sys.stdout.write('\033[F')
            sys.stdout.write('\033[K')
            uc = user_clause.replace('#USERIDS','","'.join(temp_users))
            flag = True
            attempt = 0
            while flag and attempt<3:
                try:
                    r1 = q.query(statement1.replace('#USER_CLAUSE', uc))
                    # r2 = q.query(statement2.replace('#USER_CLAUSE', uc))
                    flag = False
                    result1 = result1.append(r1)
                    # result2 = result2.append(r2)
                    print(result1.tail())
                    # print(result2.tail())
                except (MySQLdb.Error, MySQLdb.Warning) as e:
                    print(e)
                    input()
                    flag = True
                    attempt += 1
                    print('DB Query failed',attempt,'time(s)')
        # sort_info1 = result1[['SignUp', 'SORT1', 'SORT2']].drop_duplicates(subset=['SignUp'])
        # result1 = result1.groupby(['SignUp'])['UserCount', 'AtleastOnce'].sum().reset_index()
        # result1 = result1.merge(sort_info, left_on='SignUp', right_on='SignUp', how='left')
        # result1.sort_values(['SORT1', 'SORT2'], ascending=[1,1], inplace=True)
        # result1.reset_index(drop=True, inplace=True)
        # result1.drop(['SORT1', 'SORT2'], axis=1, inplace=True)
        # sort_info2 = result2[['SignUp', 'SORT1', 'SORT2']].drop_duplicates(subset=['SignUp'])
        # result2 = result2.groupby(['SignUp'])['UserCount', 'AtleastOnce'].sum().reset_index()
        # result2 = result2.merge(sort_info, left_on='SignUp', right_on='SignUp', how='left')
        # result2.sort_values(['SORT1', 'SORT2'], ascending=[1,1], inplace=True)
        # result2.reset_index(drop=True, inplace=True)
        # result2.drop(['SORT1', 'SORT2'], axis=1, inplace=True)
    else:
        user_clause = ''
        result1 = q.query(statement1.replace('#USER_CLAUSE', user_clause))
        # result2 = q.query(statement2.replace('#USER_CLAUSE', user_clause))
    result1g = result1.groupby(['FirstOrder', 'SORT1', 'SORT2']).sum().reset_index().sort_values(['SORT1', 'SORT2'], ascending=[1,1]).reset_index(drop=True).drop(['SORT1','SORT2'], axis=1)
    return result1g

# Mehtod to Query the DB for transactional data aggregation for the specific user set
def get_data(start_point='SignUp', users=None, end_date=now.strftime(s_form)):
    if start_point == 'FirstOrder':
        statement = 'SELECT CONCAT(MONTHNAME(t.FirstOrderD),"-",YEAR(t.FirstOrderD)) FirstOrder, COUNT(DISTINCT t.UserId) UserCount,'\
                    ' ((MONTH(OO.order_creation_date) - t.MonthNo)+12*(YEAR(OO.order_creation_date) - t.YearNo)) MonFromFirst,'\
                    ' COUNT(OO.order_id) TxnCount, SUM(P.payAmt) Amount, SUM(P.discount) Discount FROM ('\
                    ' SELECT U.user_id UserId, MONTH(MIN(O.order_creation_date)) MonthNo, YEAR(MIN(O.order_creation_date)) YearNo,'\
                    ' MIN(O.order_creation_date) FirstOrderD'\
                    ' FROM audit.Users U INNER JOIN audit.Orders O'\
                    ' ON U.user_id = O.user_id'\
                    ' WHERE U.sdk_merchant = "niki" AND O.status REGEXP "succ|compl|conf"'\
                    ' AND O.order_creation_date > "'+history_start+'"'\
                    ' AND U.is_high_frequency = FALSE'\
                    '#USER_CLAUSE'\
                    ' GROUP BY U.user_id'\
                    ' ) as t'\
                    ' INNER JOIN audit.Orders OO ON'\
                    ' OO.user_id = t.UserId'\
                    ' INNER JOIN audit.Payment P ON'\
                    ' OO.order_id = P.order_id'\
                    ' WHERE OO.status REGEXP "succ|comp|conf" AND OO.order_creation_date > "'+history_start+'"'\
                    ' GROUP BY FirstOrder, MonFromFirst'\
                    ' ORDER BY YEAR(t.FirstOrderD) ASC, MONTH(t.FirstOrderD) ASC, MonFromFirst ASC'
    else:
        statement = 'SELECT CONCAT(MONTHNAME(t.SignUpD),"-",YEAR(t.SignUpD)) SignUp, COUNT(DISTINCT t.UserId) UserCount,'\
                    ' ((MONTH(OO.order_creation_date) - t.MonthNo)+12*(YEAR(OO.order_creation_date) - t.YearNo)) MonFromFirst,'\
                    ' COUNT(OO.order_id) TxnCount, SUM(P.payAmt) Amount, SUM(P.discount) Discount'\
                    ' FROM ('\
                    ' SELECT U.user_id UserId, MONTH(U.sign_up_date) MonthNo, YEAR(U.sign_up_date) YearNo,'\
                    ' (U.sign_up_date) SignUpD'\
                    ' FROM audit.Users U'\
                    ' WHERE U.sdk_merchant = "niki"'\
                    ' AND U.sign_up_date > "'+history_start+'"'\
                    ' AND U.is_high_frequency = FALSE'\
                    '#USER_CLAUSE'\
                    ' ) as t'\
                    ' INNER JOIN audit.Orders OO ON'\
                    ' OO.user_id = t.UserId'\
                    ' INNER JOIN audit.Payment P ON'\
                    ' OO.order_id = P.order_id'\
                    ' WHERE OO.status REGEXP "succ|comp|conf" AND OO.order_creation_date > "'+history_start+'"'\
                    ' GROUP BY SignUp, MonFromFirst'\
                    ' ORDER BY YEAR(t.SignUpD) ASC, MONTH(t.SignUpD) ASC, MonFromFirst ASC'
    if users:
        user_clause = ' AND U.user_id IN ("#USERIDS")'
        i = 0
        total = len(users)
        jump = 250
        result = pd.DataFrame()
        while i<total:
            if i+jump <= total:
                end = i+jump
            else:
                end = total
            temp_users = users[i:end]
            i = end
            print('Doing',i,'of',total)
            sys.stdout.write('\033[F')
            sys.stdout.write('\033[K')
            uc = user_clause.replace('#USERIDS','","'.join(temp_users))
            flag = True
            attempt = 0
            while flag and attempt<6:
                try:
                    r = q.query(statement.replace('#USER_CLAUSE', uc))
                    result = result.append(r)
                    flag = False
                except (MySQLdb.Error, MySQLdb.Warning) as e:
                    print(e)
                    # input()
                    flag = True
                    attempt += 1
                    print('DB Query failed',attempt,'time(s)')
        result.reset_index(drop=True, inplace=True)
    else:
        user_clause = ''
        result = q.query(statement.replace('#USER_CLAUSE', user_clause))
    return result

# Method to Pivot the extracted data into the Final View as required
def transform_data(df, start_point='SignUp'):
    months_list = df[start_point].unique().tolist()
    offset_columns = df['MonFromFirst'].unique().tolist()
    offset_columns.sort()
    txn_data = {start_point:[]}
    gmv_data = {start_point:[]}
    disc_data = {start_point:[]}
    uniq_data = {start_point:[]}
    for month in months_list:
        temp = df[df[start_point]==month]
        user_count = temp['UserCount'].sum()
        txn_data[start_point].append(month)
        gmv_data[start_point].append(month)
        disc_data[start_point].append(month)
        uniq_data[start_point].append(month)
        for offset_month in offset_columns:
            if not offset_month in txn_data.keys():
                txn_data[offset_month] = []
            if not offset_month in gmv_data.keys():
                gmv_data[offset_month] = []
            if not offset_month in disc_data.keys():
                disc_data[offset_month] = []
            if not offset_month in uniq_data.keys():
                uniq_data[offset_month] = []
            temp2 = temp[temp['MonFromFirst'] == offset_month]
            if temp2.empty:
                txn = 0.0
                gmv = 0.0
                disc = 0.0
                uniq = 0.0
            else:
                # print(temp2)
                # input()
                txn = temp2['TxnCount'].sum()
                gmv = temp2['Amount'].sum()
                disc = temp2['Discount'].sum()
                uniq = temp2['UserCount'].sum()
            txn_data[offset_month].append(txn)
            gmv_data[offset_month].append(gmv)
            disc_data[offset_month].append(disc)
            uniq_data[offset_month].append(uniq)
    txn_data = pd.DataFrame(txn_data)
    gmv_data = pd.DataFrame(gmv_data)
    disc_data = pd.DataFrame(disc_data)
    uniq_data = pd.DataFrame(uniq_data)
    return txn_data, gmv_data, disc_data, uniq_data


# Method to get SPECIFIC User Set from Mixpanel Cohorts
def get_mp_cohort(cohort_id=240759):
    params = {'where':'properties["SdkMerchant"]=="niki"',
              'filter_by_cohort':'{"id":'+str(cohort_id)+'}',
              'output_properties':['$distinct_id'],}
    data = mp.read_people(params=params)
    # user_list = data['$distinct_id'].unique().tolist()
    return data


# Method to get users of a specific set from the PROD DB
def get_bundling_cohort(end_date=now.strftime(s_form_full)):
    statement = 'SELECT temp2.user_id UserId FROM'\
                ' (SELECT count(*) AS distinct_count, user_id FROM'\
                ' (SELECT count(*) AS utility_count, user_id, sub_domain FROM audit.Orders'\
                ' WHERE order_creation_date > "'+history_start+'"'\
                ' AND domain IN ("utilities", "recharge")'\
                ' AND status IN ("success", "successful") GROUP BY user_id, sub_domain)'\
                ' AS temp GROUP BY user_id) AS temp2'\
                ' WHERE distinct_count >= 2'
    flag = True
    attempt = 0
    while flag and attempt<3:
        try:
            users = q.query(statement)
            flag = False
        except:
            flag = True
            attempt += 1
            print('DB Query failed',attempt,'time(s)')
    user_list = users['UserId'].unique().tolist()

    return users


# Method to calculate the month-on-month spend of users starting at either signup month or month of first TXN
def run_analyses(start_point='SignUp', cohort='None'):
    print('Starting GMV analyses for', cohort, 'cohort at', start_point)
    if cohort == 'special':
        filename = 'LTV Data till '+now.strftime(s_form)+' starting at '+start_point+' special cohort.xlsx'
        print('Querying MP for Cohort data...')
        # user_cohort = get_mp_cohort()
        # user_cohort.to_csv('temp_store_users.csv', header=True)
        user_cohort = pd.read_csv('temp_store_users.csv', index_col=0)
        user_cohort = user_cohort['$distinct_id'].unique().tolist()
    elif cohort == 'bundled':
        filename = 'LTV Data till '+now.strftime(s_form)+' starting at '+start_point+' bundled cohort.xlsx'
        print('Querying DB for Bundled Users data...')
        user_cohort = get_bundling_cohort()
    else:
        filename = 'LTV Data till '+now.strftime(s_form)+' starting at '+start_point+'.xlsx'
        user_cohort = None
    print('Querying DB for User Size')
    user_data = get_user_data(start_point=start_point, users=user_cohort)

    print('Querying DB for TXN data')
    ltv_data = get_data(start_point=start_point, users=user_cohort)
    print('Building Views')
    txn, gmv, disc, uniq = transform_data(ltv_data, start_point=start_point)
    txn_data = user_data.merge(txn, left_on=start_point, right_on=start_point, how='outer')
    uniq_data = user_data.merge(uniq, left_on=start_point, right_on=start_point, how='outer')
    gmv_data = user_data.merge(gmv, left_on=start_point, right_on=start_point, how='outer')
    disc_data = user_data.merge(disc, left_on=start_point, right_on=start_point, how='outer')
    print('Writing to File')
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    txn_data.to_excel(writer, sheet_name='TxnCount')
    uniq_data.to_excel(writer, sheet_name='UniqTxnCount')
    gmv_data.to_excel(writer, sheet_name='GMV')
    disc_data.to_excel(writer, sheet_name='Discount')
    writer.save()
    print('Done \a')


def start_main():
    combinations = [
                    # ('SignUp', 'None'),
                    # ('SignUp', 'special')
                    # ('SignUp', 'bundled'),
                    # ('FirstOrder', 'None'),
                    ('FirstOrder', 'special')
                    # ('FirstOrder', 'bundled')
                   ]
    for combination in combinations:
        start_point, user_cohort = combination
        run_analyses(start_point=start_point, cohort=user_cohort)


if __name__ == "__main__":
    start_main()
