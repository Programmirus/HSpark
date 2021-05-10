import pandas as pd
import pyspark.sql.functions as func
from pyspark.sql import Window
#from pyspark.sql.functions import udf
import pyspark
import multipledispatch as dispatcher
import functools as ft
from pyspark.sql.dataframe import DataFrame 
import traceback
import itertools
import threading as pythr
import queue
import sqlite3
import os
import inspect
import datetime as dt
from types import FunctionType, MethodType

        
def throw_thread(f):
    def output(*args):
        thread=pythr.Thread(target=f,args=args)
        thread.start()
        #print(args)
        return 1
    return output
        


    
    
        
class HiveFrame(DataFrame):
    #@dispatcher.dispatch(pyspark.sql.session.SparkSession, str)
    def __init__(self,spark,tableName):
        #self.__df=spark.table(tableName)
        #self.__partitions=HiveFrame.get_table_partitions(spark,tableName)
        super(HiveFrame,self).__init__(self.__df._jdf,self.__df.sql_ctx)
        
        

        
class HSession(pyspark.sql.session.SparkSession):
    
    @dispatcher.dispatch(pyspark.sql.session.SparkSession,str)
    def __init__(self, spark,UDFSourceTable):
        self.__spark=spark
        self.__UDFSourceTable=UDFSourceTable
        self.__conn=sqlite3.Connection("conf.db")
        self.__cur=self.__conn.cursor()
        try:
            self.__cur.execute("""
                 create table user_functions
                  (
                    code string,
                    username string, 
                    procname string, 
                    version string,
                    commit_date date,
                    description string                   )
                       """)

        except:
            pass
        self.__conn.close()
        super(HSession,self).__init__(spark.sparkContext) 
        try:
            df=self.__spark.sql("select 'code','username','procname','version','2021-01-01' as commit_date,'description'")
            df.write.saveAsTable(self.__UDFSourceTable)
            self.__spark.sql("truncate table {0}".format(self.__UDFSourceTable))
        except:
            pass        
        print("HSession created, External UDFStorage: ",self.__UDFSourceTable)    
    def getUDFList(self):
        conn=sqlite3.Connection("conf.db")
        cur=conn.cursor()
        content=cur.execute("select * from user_functions").fetchall()
        funclist=pd.DataFrame.from_dict(content)
        funclist.columns=['code','username','procname','version','commit_date','description']
        conn.close()
        return funclist
    
    @staticmethod
    @ft.lru_cache()
    @dispatcher.dispatch(pyspark.sql.session.SparkSession, str)
    def get_table_partitions(spark,tableName):
        try:
            pdf=spark.sql("show partitions {tableName}".format(tableName=tableName)).toPandas()
        except:
            return [{}]
        partitions=[]
        for i in pdf['partition']:
                 partition=dict()
                 for part in i.split("/"):
                         partition[part.split("=")[0]]=part.split("=")[1]
    
                 partitions.append(partition)
        return partitions
    
    
    @staticmethod
    @dispatcher.dispatch(pyspark.sql.session.SparkSession, str, dict)
    def get_partition_frame(spark, tableName,partition):
        partitions=HSession.get_table_partitions(spark,tableName)
        df=spark.table(tableName)
        try:
            for i in [i for i in partitions if i==partition][0]:
                df=df.where(func.col(i)==partition[i])
        except:
            df=spark.createDataFrame(data=[],schema=spark.table(tableName).schema)

        
        return df
    
    @staticmethod
    def parallelize_pipeline(f):
        #@dispatcher.dispatch(pyspark.sql.session.SparkSession, str, list)
        def output(spark, tableName,tablePartitions=[]):
                partitions=HSession.get_table_partitions(spark,tableName)
                res=[]
                if partitions==[{}]:
                    print("no partitions for this table")
                    res.append(f(spark.table(tableName)))
                    return res

                if tablePartitions==[]: 
                    tablePartitions=partitions
                else:
                    tablePartitions=[i for i in tablePartitions if i in partitions]
                    #list(set(tablePartitions).intersection(set(partitions)))
                    
                df=spark.createDataFrame(data=[],schema=spark.table(tableName).schema)
                for partition in tablePartitions:
                    print("started running partition : ", partition)
                    try:
                        res.append(dict({'partition':partition,
                                         'value':f(HSession.get_partition_frame(spark,tableName, partition)),
                                         'status':'success'}))
                        print("finished running partition : ", partition)
                    except:
                        res.append(dict({'partition':partition,
                                         'value':traceback.format_exc().splitlines(),
                                         'status':'failed'}))

                        print("failure when running partition : ", partition)
                    
                return res
        return output

        
    @dispatcher.dispatch(str,dict)
    def table(self,tableName,partition):
        return HSession.get_partition_frame(self.__spark,tableName,partition)
       
   
    @dispatcher.dispatch(str,int,str)
    def import_udf(self,procname, version,alias):
        #if version is None:
            #cur.execute
        udf=self.__spark.udf
        self.__conn=sqlite3.Connection("conf.db")
        self.__cur=self.__conn.cursor()
        username=os.getlogin()
        code=self.__cur.execute("""select * from user_functions where procname='{0}' and username='{1}' and version='{2}'"""\
                       .format(procname,username, version))\
                .fetchall()[0][0]
        #procname=self.__cur.execute("""select * from user_functions where procname='{0}' and username='{1}' and version='{2}'"""\
         #                  .format(procname, username,version))\
          #      .fetchall()[0][2]
        src=compile(code,"<string>","exec")
        print(src)
        fctn = FunctionType(src.co_consts[0], globals(), procname)
        udf.register('{alias}'.format(alias=alias),fctn)
        #exec(code+" \n{procname}=func.udf({procname}) \nudf.register('{alias}',{procname})".format(procname=procname,alias=alias))
        self.__conn.close()
        return func.udf(fctn)
    
    @dispatcher.dispatch(str,str,int,str)
    def import_udf(self,procname,username, version,alias):
        #if version is None:
            #cur.execute
        udf=self.__spark.udf

        username=os.getlogin()
        code=self.__spark.sql("""select code from {3} where procname='{0}' and username='{1}' and version='{2}'"""\
                       .format(procname,username, version, self.__UDFSourceTable)).toPandas().to_dict()['code'][0]

        src=compile(code,"<string>","exec")
        print(src)
        fctn = FunctionType(src.co_consts[0], globals(), procname)
        udf.register('{alias}'.format(alias=alias),fctn)
        #exec(code+" \n{procname}=func.udf({procname}) \nudf.register('{alias}',{procname})".format(procname=procname,alias=alias))
        return func.udf(fctn) 
    

    
    
    
    @staticmethod
    def checkoutUDF(f, tests):
        for i in tests:
            try:
                exec("{0}".format(i))
            except:
                print("Failure on test :", i)
                return False
        return True
     
    def register_udf(self,f,description,tests):
        if (HSession.checkoutUDF(f,tests)==False): return
        code=inspect.getsource(f).replace('"',"'")
        username=os.getlogin()
        procname=code.split(" ")[1].replace(" ","").split("(")[0]
        self.__conn=sqlite3.Connection("conf.db")
        self.__cur=self.__conn.cursor()
        try:
            exist=self.__cur.execute("""select max(version),max(case when code='{2}' then version else 0 end) from user_functions 
                                        where procname='{0}' and username='{1}'
                                         group by procname, username
                                         having count(*)>0
                                     """.format(procname,username,code)).fetchall()
            print(exist[0][0],exist[0][1])
            if exist[0][1]==0:
                version=exist[0][0]+1
            else:
                print("function already exists in version {0} of user {1}".format(exist[0][1],username))
                return
        except:
            version=1
        
        commit_date=str(dt.datetime.today())
        
        
        self.__cur.execute("""insert into user_functions values("{code}", "{username}","{procname}","{version}","{commit_date}",
                      "{description}")"""\
                    .format(code=code,
                           username=username,
                           procname=procname,
                           version=version,
                           commit_date=commit_date,
                           description=description)
                       ) 
        self.__conn.commit()
        self.__conn.close()
        print("successfully registered function :", code)
        
    def publish_udf(self,procname,version,description):
        try:
            self.__conn=sqlite3.Connection("conf.db")
            self.__cur=self.__conn.cursor()
            code=self.__cur.execute("""select * from user_functions where procname='{0}' and username='{1}' and version='{2}'"""\
                       .format(procname,os.getlogin(), version))\
                       .fetchall()[0][0]
            self.__conn.close()
        except:
            print("function is out of your sandbox")
            return
        win=Window.orderBy(func.desc('commit_date'))
        is_already=self.__spark.table(self.__UDFSourceTable)\
                    .where(func.col('code')==code)\
                    .withColumn('rn',func.row_number().over(win))\
                    .where(func.col('rn')==1)
        if (is_already.count()==0):
            query="""select '{0}' as code, '{1}' as username, '{2}' as procname, '{3}' as version, '{4}' as commit_date, 
                        '{5}' as description""".format(code,os.getlogin(),procname,
                                                       version,str(dt.datetime.today()), description)
            self.__spark\
                .sql(query).write.mode('append').saveAsTable(self.__UDFSourceTable)
        else:
            print("function already exists. For example:")
            return is_already.toPandas()