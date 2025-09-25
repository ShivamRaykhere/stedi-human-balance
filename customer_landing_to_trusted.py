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

# Script generated for node customer landing
customerlanding_node1758776026454 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="customerlanding_node1758776026454")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from customer_landing
where shareWithResearchasOfDate IS NOT NULL;

'''
SQLQuery_node1758776059235 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_landing":customerlanding_node1758776026454}, transformation_ctx = "SQLQuery_node1758776059235")

# Script generated for node customer trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758776059235, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758776013983", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customertrusted_node1758776205358 = glueContext.getSink(path="s3://mybucketstedi/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customertrusted_node1758776205358")
customertrusted_node1758776205358.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
customertrusted_node1758776205358.setFormat("json")
customertrusted_node1758776205358.writeFrame(SQLQuery_node1758776059235)
job.commit()
