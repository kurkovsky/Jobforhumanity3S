import mysql.connector
from tabulate import tabulate
from datetime import datetime, timedelta
from dateutil.relativedelta import *
from collections import OrderedDict

def open_database(hostname, user_name, mysql_pw, database_name):
    global conn
    conn = mysql.connector.connect(host=hostname,
                                   user=user_name,
                                   password=mysql_pw,
                                   database=database_name
                                   )
    global cursor
    cursor = conn.cursor()

def insert(table, values):
    query = "INSERT into " + table + " values (" + values + ")" + ';'
    cursor.execute(query)
    conn.commit()
    
def insert_alt1(table, column, sel_statement):
    query = "INSERT into " + table + " (" + column + ") " + sel_statement
    cursor.execute(query)
    conn.commit()

def create_table(table_name, conditions):
    query = "CREATE TABLE " + table_name + " (" + conditions + ");"
    cursor.execute(query)
    conn.commit()

def copy_table(table_name, original_table):
    query = "CREATE TABLE " + table_name + " LIKE " + original_table + ";"
    cursor.execute(query)
    conn.commit()
    
def dump_table(table_name, original_table):
    query = "INSERT INTO " + table_name + " SELECT * FROM " + original_table + ";"
    cursor.execute(query)
    conn.commit()

def drop(table):
    query = "DROP TABLE " + table + ";"
    cursor.execute(query)
    conn.commit()

def execute(query):
    cursor.execute(query)
    return cursor.fetchone()[0]

def printFormat(result):
    header = []
    for cd in cursor.description:  # get headers
        header.append(cd[0])
    print('')
    print('Query Result:')
    print('')
    return(tabulate(result, headers=header))  # print results in table format

def executeSelect(query):
    cursor.execute(query)
    res = printFormat(cursor.fetchall())
    return res

def execute_noTable(query):
    cursor.execute(query)
    res = cursor.fetchall()
    return res
    
def split2pop(selection):
    selection = selection.split('\n')
    selection.pop(1)
    selection.pop(0)
    result = selection
    return result

def list2string(selection):
    avail = ""
    for i in selection:
        avail += str(i) + ','
    avail = avail[:-1]
    result = avail
    return result

def work_country(work_auth):
    auth = executeSelect("SELECT id FROM candidates_chase WHERE work_auth = '" + work_auth + "';")
    auth = split2pop(auth)
    return auth

def jobs_avail(country):
    work = executeSelect("SELECT id FROM jobs_chase WHERE country = '" + country + "';")
    work = split2pop(work)
    avail = list2string(work)
    return avail
    
def jobs_avail_eu():
    work = executeSelect("SELECT id FROM jobs_chase WHERE country = 'Austria' OR country = 'Belgium' OR country = 'Czech Republic' OR country = 'Germany' OR country = 'Denmark' OR country = 'France' OR country = 'Greece' OR country = 'Croatia' OR country = 'Hungary' OR country = 'Italy' OR country = 'Lithuania' OR country = 'Netherlands' OR country = 'Romania' OR country = 'Sweden' OR country = 'Slovakia';")
    work = split2pop(work)
    avail = list2string(work)
    return avail
    
def jobs_avail_latin():
    work = executeSelect("SELECT id FROM jobs_chase WHERE country = 'Brazil' OR country = 'Chile' OR country = 'Peru';")
    work = split2pop(work)
    avail = list2string(work)
    return avail
    
def jobs_avail_other():
    work = executeSelect("SELECT id FROM jobs_chase WHERE country = 'Canada' OR country = 'Switzerland' OR country = 'Egypt' OR country = 'Israel' OR country = 'Iceland' OR country = 'Japan' OR country = 'Kenya' OR country = 'Morocco' OR country = 'Mexico' OR country = 'Malaysia' OR country = 'Philippines' OR country = 'Puerto Rico' OR country = 'Saudi Arabia' OR country = 'Thailand' OR country = 'Turkey' OR country = 'Vietnam' OR country = 'South Africa';")
    work = split2pop(work)
    avail = list2string(work)
    return avail
    
def jobs_avail_remote():
    work = executeSelect("SELECT id FROM jobs_chase WHERE remote = '1';")
    work = split2pop(work)
    avail = list2string(work)
    return avail

def extra_cand_info(cand_id):
    cand_auth = executeSelect("SELECT work_auth FROM candidates_chase WHERE id = " + str(cand_id) + ";")
    cand_auth = split2pop(cand_auth)
    cand_auth = list2string(cand_auth)
    cand_desire = executeSelect("SELECT desired_function FROM candidates_chase WHERE id = " + str(cand_id) + ";")
    cand_desire = split2pop(cand_desire)
    cand_desire = list2string(cand_desire)
    cand_cause = executeSelect("SELECT jfh_cause FROM candidates_chase WHERE id = " + str(cand_id) + ";")
    cand_cause = split2pop(cand_cause)
    cand_cause = list2string(cand_cause)
    result = "'" + str(cand_auth) + "','" + str(cand_desire) + "','" + str(cand_cause) + "'"
    return result
    
def work_residence():
    auth = work_country("My current country of residence")
    for i in range(len(auth)):
       resd_country = executeSelect("SELECT cand_country FROM candidates_chase WHERE id = " + str(i) + ";")
       resd_country = split2pop(resd_country)
       resd_avail = jobs_avail(str(resd_country))
       resd_data = str(auth[i]) + ",'" + str(resd_avail) + "'," + str(extra_cand_info(auth[i]))
       insert("filter_location", resd_data)
       #work = executeSelect("SELECT id FROM jobs_chase WHERE country = '" + str(resd_country) + "';")

def work_remote():
    auth = work_country("I would only work remotely")
    remote_avail = jobs_avail_remote()
    for i in range(len(auth)):
        remote_data = str(auth[i]) + ",'" + str(remote_avail) + "'," + str(extra_cand_info(auth[i]))
        insert("filter_location", remote_data)

def close_db():  # use this function to close db
    cursor.close()
    conn.close()        

def job_location(list_candidate, table_name):
    job_Australia_country = jobs_avail("Australia")
    job_China_country = jobs_avail("China")
    job_India_country = jobs_avail("India")
    job_UK_country = jobs_avail("United Kingdom")
    job_USA_country = jobs_avail("United States")
    job_EU_country = jobs_avail_eu()
    job_Latin_country = jobs_avail_latin()
    job_Remote_country = jobs_avail_remote()
    for cand_id in list_candidate:
        work_auth = executeSelect(
            "SELECT work_auth FROM candidates_table WHERE id = "+str(cand_id)+";")
        work_auth = split2pop(work_auth)
        work_auth = list2string(work_auth)
        list_work_auth = work_auth.split(',')
        # print(len(list_candidate))
        list_job = ""
        # This loop run iterate each element in the work_auth of a candidate
        # and find a list of job match with the work authorization
        for j in list_work_auth:
            if j == "Australia":
                list_job += job_Australia_country+","
            elif j == "China":
                list_job += job_China_country+","
            elif j == "India":
                list_job += job_India_country+","
            elif j == "United Kingdom":
                list_job += job_UK_country+","
            elif j == "USA":
                list_job += job_USA_country+","
            elif j == "European Union":
                list_job += job_EU_country+","
            elif j == "Latin America":
                list_job += job_Latin_country+","
            elif j == "Other" or j == "null" or j == "I would only work remotely":
                list_job += job_Remote_country+","
            elif j == "My current country of residence":
                cand_country = executeSelect(
                    "SELECT cand_country FROM candidates_table WHERE id ="+str(cand_id)+";")
                cand_country = split2pop(cand_country)
                cand_country = list2string(cand_country)
                job_in_country = jobs_avail(cand_country)
                list_job += job_in_country+","
            else:
                list_job += jobs_avail(j)+","
            list_job = list_job[:-1]
            # OrderedDict.fromkeys to remove the duplicate job_id in the list_job
            list_job = list_job.split(",")
            list_job = list(OrderedDict.fromkeys(list_job))
            list_job = list2string(list_job)
            # ----------------------end remove duplicate------------------------
        data_row = cand_id + ",'" + list_job + "'"
        insert(table_name, data_row)

def job_function(list_candidate, table_name):
    # This segment to get list of job for each function. Data is stored in the dictionary.
    # It cut down running SQL command  in general.
    functions = executeSelect(
        "SELECT job_function FROM jobs_table GROUP BY job_function;")
    functions = split2pop(functions)
    dict_functions = {}
    for x in functions:
        dict_functions[x] = list_job_eachfunction(x)
    # Start a loop for candidate
    for i in list_candidate:
        cand_desired_function = executeSelect(
            "SELECT desired_function FROM candidates_table WHERE id="+i+";")
        cand_desired_function = split2pop(cand_desired_function)
        cand_desired_function = list2string(cand_desired_function)
        cand_desired_function = cand_desired_function.split(",")
        job_list = ""
        for j in cand_desired_function:
            if (j == "null"):
                job_list = "null,"
            else:
                job_list += dict_functions[j]+","
        if (job_list != ""):
            job_list = job_list[:-1]
        data_row = i + ",'" + job_list + "'"
        insert(table_name, data_row)

def job_cause(list_candidate, table_name):
    # This segment to get list of job for each cause. Data is stored in the dictionary.
    # It cut down running SQL command  in general.
    cause_array = ["Black leader", "Blind or Low vision", "Neurodivergent",
                   "Refugee or Forcibly displaced", "Returning Citizen", "Single Mom / Parent"]
    dict_functions = {}
    for x in cause_array:
        if x == "Black leader":
            y = "Blackleaders"
        elif x == "Blind or Low vision":
            y = "Blind"
        elif x == "Neurodivergent":
            y = "Neurodivergent"
        elif x == "Refugee or Forcibly displaced":
            y = "Refugee"
        elif x == "Returning Citizen":
            y == "ReturningCitizen"
        elif x == "Single Mom / Parent":
            y == "Singlemom"
        dict_functions[x] = list_job_cause(y)
    # Start a loop for candidate
    for i in list_candidate:
        cand_cause = executeSelect(
            "SELECT jfh_cause FROM candidates_table WHERE id="+i+";")
        cand_cause = split2pop(cand_cause)
        cand_cause = list2string(cand_cause)
        cand_cause = cand_cause.split(",")
        cause_list = ""
        for j in cand_cause:
            if (j == "null"):
                cause_list = "null,"
            elif (j == "Other"):
                cause_list = "Other,"
            else:
                cause_list += dict_functions[j]+","
        cause_list = cause_list[:-1]
        data_row = i + ",'" + cause_list + "'"
        insert(table_name, data_row)

def common_member3(a, b, c):
    a_set = set(a)
    b_set = set(b)
    c_set = set(c)
    job_list = {}
    if (a_set & b_set & c_set):
        job_list = a_set & b_set & c_set
    return job_list


def common_member2(a, b):
    a_set = set(a)
    b_set = set(b)
    job_list = {}
    if (a_set & b_set):
        job_list = a_set & b_set
    return job_list

def filter_3condition(list_candidate, table_name):
    # Local variable  list job for filter_location
    job_Australia_country = jobs_avail("Australia")
    job_China_country = jobs_avail("China")
    job_India_country = jobs_avail("India")
    job_UK_country = jobs_avail("United Kingdom")
    job_USA_country = jobs_avail("United States")
    job_EU_country = jobs_avail_eu()
    job_Latin_country = jobs_avail_latin()
    job_Remote_country = jobs_avail_remote()
    # Local variable  list job for filter_function
    functions = executeSelect(
        "SELECT job_function FROM jobs_table GROUP BY job_function;")
    functions = split2pop(functions)
    dict_function_job = {}
    for x in functions:
        dict_function_job[x] = list_job_eachfunction(x)
    # Local variable list job for filter_cause
    cause_array = ["Black leader", "Blind or Low vision", "Neurodivergent",
                   "Refugee or Forcibly displaced", "Returning Citizen", "Single Mom / Parent"]
    dict_cause_job = {}
    for x in cause_array:
        if x == "Black leader":
            y = "Blackleaders"
        elif x == "Blind or Low vision":
            y = "Blind"
        elif x == "Neurodivergent":
            y = "Neurodivergent"
        elif x == "Refugee or Forcibly displaced":
            y = "Refugee"
        elif x == "Returning Citizen":
            y == "ReturningCitizen"
        elif x == "Single Mom / Parent":
            y == "Singlemom"
        dict_cause_job[x] = list_job_cause(y)
    for cand_id in list_candidate:
        work_auth = executeSelect(
            "SELECT work_auth FROM candidates_table WHERE id = "+str(cand_id)+";")
        work_auth = split2pop(work_auth)
        work_auth = list2string(work_auth)
        list_work_auth = work_auth.split(',')
        # print(len(list_candidate))
        list_job = ""
        # This loop run iterate each element in the work_auth of a candidate
        # and find a list of job match with the work authorization
        for j in list_work_auth:
            if j == "Australia":
                list_job += job_Australia_country+","
            elif j == "China":
                list_job += job_China_country+","
            elif j == "India":
                list_job += job_India_country+","
            elif j == "United Kingdom":
                list_job += job_UK_country+","
            elif j == "USA":
                list_job += job_USA_country+","
            elif j == "European Union":
                list_job += job_EU_country+","
            elif j == "Latin America":
                list_job += job_Latin_country+","
            elif j == "null" or j == "I would only work remotely":
                list_job += job_Remote_country+","
            elif j == "My current country of residence" or j == "Other":
                cand_country = executeSelect(
                    "SELECT cand_country FROM candidates_table WHERE id ="+str(cand_id)+";")
                cand_country = split2pop(cand_country)
                cand_country = list2string(cand_country)
                job_in_country = jobs_avail(cand_country)
                list_job += job_in_country+","
            else:
                list_job += jobs_avail(j)+","
        list_job = list_job[:-1]
        job_by_filter_location = list_job.split(",")
    # Start a loop for filter_function
        cand_desired_function = executeSelect(
            "SELECT desired_function FROM candidates_table WHERE id="+cand_id+";")
        cand_desired_function = split2pop(cand_desired_function)
        cand_desired_function = list2string(cand_desired_function)
        cand_desired_function = cand_desired_function.split(",")
        job_list = ""
        for j in cand_desired_function:
            if (j == "null"):
                job_list = "null,"
            else:
                job_list += dict_function_job[j]+","
        job_list += dict_function_job['Other']+","
        if (job_list != ""):
            job_list = job_list[:-1]
        job_by_filter_function = job_list.split(",")
    # Start a loop for filter_function
        cand_cause = executeSelect(
            "SELECT jfh_cause FROM candidates_table WHERE id="+cand_id+";")
        cand_cause = split2pop(cand_cause)
        cand_cause = list2string(cand_cause)
        cand_cause = cand_cause.split(",")
        cause_list = ""
        for j in cand_cause:
            if (j == "null"):
                cause_list = "null,"
            elif (j == "Other"):
                cause_list = "Other,"
            else:
                cause_list += dict_cause_job[j]+","
        cause_list = cause_list[:-1]
        job_by_filter_cause = cause_list.split(",")
    # Find job in three job_list by conditions
        jobs = ""
        if (job_by_filter_location[0] != ''):
            #print("location != empty")
            # print(job_by_filter_location)
            if (job_by_filter_function != "null"):
                #print("function !=null")
                # print(job_by_filter_function)
                if (job_by_filter_cause[0] != "null" and job_by_filter_cause[0] != "Other"):
                    #print("cause != null and other ")
                    # print(job_by_filter_cause)
                    jobs = common_member3(
                        job_by_filter_location, job_by_filter_function, job_by_filter_cause)
                    # print(jobs)
                else:
                    #print("cause == null or other")
                    # print(job_by_filter_cause)
                    jobs = common_member2(
                        job_by_filter_location, job_by_filter_function)
                    # print(jobs)
            else:
                #print("function !=null")
                # print(job_by_filter_function)
                if (job_by_filter_cause[0] != "null" and job_by_filter_cause[0] != "Other"):
                    #print("cause != null and other "+job_by_filter_cause)
                    jobs = common_member2(
                        job_by_filter_location, job_by_filter_cause)
                    # print(jobs)
                else:
                    #print("cause == null or other")
                    # print(job_by_filter_cause)
                    jobs = set(job_by_filter_location)
                    # print(jobs)
        # else : print("location is empty")
        jobs = ', '.join(jobs)
        data_row = cand_id + ",'" + jobs + "'"
        insert(table_name, data_row)

def update_cand(table):
    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d %H:%M:%S')
    cand_list = executeSelect(
        "SELECT id FROM candidates_table WHERE create_date >='"+yesterday + "';")
    cand_list = split2pop(cand_list)
    print(cand_list)
    filter_3condition(cand_list, table) 

def list_job_eachfunction(function):
    job_list = executeSelect(
        "SELECT id FROM jobs_table WHERE active=1 AND job_function LIKE '%"+function+"%';")
    job_list = split2pop(job_list)
    job_list = list2string(job_list)
    return job_list

def list_job_cause(cause_column):
    job_list = executeSelect(
        "SELECT id FROM jobs_table WHERE active=1 AND "+cause_column + "=1;")
    job_list = split2pop(job_list)
    job_list = list2string(job_list)
    return job_list

def auth_to_work(work_auth_list, cand_country, job_country, job_remote):
    european_union_countries = ["Austria", "Belgium", "Czech Republic", "Germany", "Denmark", "France", "Greece", "Croatia", "Hungary", "Italy", "Lithuania", "Netherlands", "Romania", "Sweden", "Slovakia"]
    latin_american_countries = ["Brazil", "Chile", "Peru"]
    other_countries = []
    #Check candidate work_auth for matches with the job country or if the job is remote
    for j in work_auth_list: 
            #First checks if candidate only works remotely and if job allows remote work
            if("I would only work remotely" in work_auth_list and job_remote == 0):
                return 0
            elif(j == "I would only work remotely" and job_remote):
                return 1
            elif(j == "European Union" and job_country in european_union_countries):
                return 1
            elif(j == "Latin America" and job_country in european_union_countries):
                return 1
            elif (j == "Other" and job_country == cand_country):
                return 1
            elif(j == "My current country of residence" and job_country == cand_country):
                return 1
            elif(j == "USA" and job_country == "United States"):
                return 1
            elif(j == job_country):
                return 1
    #After looping no matches found then return 0
    return 0

def update_jobs():
    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d %H:%M:%S')
    job_pool = executeSelect("SELECT id FROM jobs_table WHERE create_date >='"+yesterday+ "';")
    job_pool = split2pop(job_pool)
    candidate_pool = executeSelect("SELECT id FROM candidates_table")
    candidate_pool = split2pop(candidate_pool) 
    for job in job_pool:
            #Before looping candidates grab value of job country and if it is remote
            job_country = executeSelect("SELECT country FROM jobs_table WHERE id = " + str(job) + ";")
            job_country = split2pop(job_country)
            job_remote = executeSelect("SELECT remote FROM jobs_table WHERE id = " + str(job) + ";")
            job_remote = split2pop(job_remote)
            for cand in candidate_pool: #For loop of every candidate 
                #Get candidate's work_auth
                work_auth = executeSelect(
                "SELECT work_auth FROM candidates_chase WHERE id = "+str(cand)+";")
                work_auth = split2pop(work_auth)
                work_auth = list2string(work_auth)
                list_work_auth = work_auth.split(',')
                cand_country = executeSelect("SELECT cand_country FROM candidates_table WHERE id = " + str(cand) + ";")
                #Pass to helper function to see if candidates is authorized to work in the job's country
                if(auth_to_work(list_work_auth,cand_country,job_country,job_remote)):
                    #append job_id in filter_location
                    cursor.execute("update filter_location1 set job_id = concat(job_id, ", " + str(job)) where id = " + str(cand) + ";")

def main():
    mysql_username = 'Matching_score_user'
    mysql_password = 'Jfh2022_MS'
    mysql_host = 'database-3.cln6ulf5jgzz.us-east-2.rds.amazonaws.com'
    mysql_db = 'candidates'
    open_database(mysql_host, mysql_username, mysql_password, mysql_db)  # open database
    #update_cand(resulting table for candidates available jobs)
    #update_jobs()
    #update jobs needs to be updated to work with candidate job function and community and the resulting table needs to be updated
    close_db()

if __name__ == "__main__":
    main()