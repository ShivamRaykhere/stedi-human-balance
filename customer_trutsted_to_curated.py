import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer trusted
accelerometertrusted_node1758777223829 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometertrusted_node1758777223829")

# Script generated for node customer trusted
customertrusted_node1758777225144 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customertrusted_node1758777225144")

# Script generated for node SQL Query
SqlQuery0 = '''
select DISTINCT customer_trusted.*
from accelerometer_trusted
INNER JOIN customer_trusted
ON accelerometer_trusted.user = customer_trusted.email

'''
SQLQuery_node1758777314096 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_trusted":accelerometertrusted_node1758777223829, "customer_trusted":customertrusted_node1758777225144}, transformation_ctx = "SQLQuery_node1758777314096")

# Script generated for node customer curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758777314096, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758777208803", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customercurated_node1758777505710 = glueContext.getSink(path="s3://mybucketstedi/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customercurated_node1758777505710")
customercurated_node1758777505710.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
customercurated_node1758777505710.setFormat("json")
customercurated_node1758777505710.writeFrame(SQLQuery_node1758777314096)
job.commit()
