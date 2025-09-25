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

# Script generated for node step_trainer_landing
step_trainer_landing_node1758787856379 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mybucketstedi/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1758787856379")

# Script generated for node customer curated
customercurated_node1758787858428 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mybucketstedi/customer/curated/"], "recurse": True}, transformation_ctx="customercurated_node1758787858428")

# Script generated for node SQL Query
SqlQuery0 = '''
select step_trainer_landing.*
from step_trainer_landing
INNER JOIN customer_curated
ON customer_curated.serialNumber = step_trainer_landing.serialNumber;
'''
SQLQuery_node1758777836214 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_landing":step_trainer_landing_node1758787856379, "customer_curated":customercurated_node1758787858428}, transformation_ctx = "SQLQuery_node1758777836214")

# Script generated for node step trainer trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758777836214, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758777761265", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
steptrainertrusted_node1758778113491 = glueContext.getSink(path="s3://mybucketstedi/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="steptrainertrusted_node1758778113491")
steptrainertrusted_node1758778113491.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
steptrainertrusted_node1758778113491.setFormat("json")
steptrainertrusted_node1758778113491.writeFrame(SQLQuery_node1758777836214)
job.commit()
