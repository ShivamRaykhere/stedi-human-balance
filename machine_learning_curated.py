import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node accelerometer trusted
accelerometertrusted_node1758778428110 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometertrusted_node1758778428110")

# Script generated for node step trainer trusted
steptrainertrusted_node1758778429043 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="steptrainertrusted_node1758778429043")

# Script generated for node SQL Query
SqlQuery0 = '''
select accelerometer_trusted.user,step_trainer_trusted.*,accelerometer_trusted.x,accelerometer_trusted.y,accelerometer_trusted.z
from step_trainer_trusted
INNER JOIN accelerometer_trusted
ON accelerometer_trusted.timeStamp = step_trainer_trusted.sensorReadingTime;
'''
SQLQuery_node1758778490535 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_trusted":accelerometertrusted_node1758778428110, "step_trainer_trusted":steptrainertrusted_node1758778429043}, transformation_ctx = "SQLQuery_node1758778490535")

# Script generated for node machine learning curated
machinelearningcurated_node1758778745109 = glueContext.getSink(path="s3://mybucketstedi/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machinelearningcurated_node1758778745109")
machinelearningcurated_node1758778745109.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
machinelearningcurated_node1758778745109.setFormat("json")
machinelearningcurated_node1758778745109.writeFrame(SQLQuery_node1758778490535)
job.commit()
