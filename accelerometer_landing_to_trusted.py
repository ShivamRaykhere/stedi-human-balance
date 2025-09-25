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

# Script generated for node accelerometer landing
accelerometerlanding_node1758787481163 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mybucketstedi/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometerlanding_node1758787481163")

# Script generated for node customer trusted
customertrusted_node1758787484001 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mybucketstedi/customer/trusted/"], "recurse": True}, transformation_ctx="customertrusted_node1758787484001")

# Script generated for node SQL Query
SqlQuery0 = '''
select DISTINCT accelerometer_landing.*
FROM accelerometer_landing
INNER JOIN customer_trusted
ON accelerometer_landing.user = customer_trusted.email;
'''
SQLQuery_node1758776757138 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_landing":accelerometerlanding_node1758787481163, "customer_trusted":customertrusted_node1758787484001}, transformation_ctx = "SQLQuery_node1758776757138")

# Script generated for node accelerometer trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758776757138, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758776675325", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometertrusted_node1758776932153 = glueContext.getSink(path="s3://mybucketstedi/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometertrusted_node1758776932153")
accelerometertrusted_node1758776932153.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
accelerometertrusted_node1758776932153.setFormat("json")
accelerometertrusted_node1758776932153.writeFrame(SQLQuery_node1758776757138)
job.commit()
