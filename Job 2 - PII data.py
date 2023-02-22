import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from pyspark.sql.types import StringType
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node MySQL - Customers
MySQLCustomers_node1677029003448 = glueContext.create_dynamic_frame.from_catalog(
    database="gluestudio",
    table_name="gluestudio_customers",
    transformation_ctx="MySQLCustomers_node1677029003448",
)

# Script generated for node Aggregate - Sales
AggregateSales_node1677030461398 = glueContext.create_dynamic_frame.from_catalog(
    database="gluestudio",
    table_name="sales-aggregation-report",
    transformation_ctx="AggregateSales_node1677030461398",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1677029070654 = ApplyMapping.apply(
    frame=MySQLCustomers_node1677029003448,
    mappings=[
        ("zip", "string", "zip", "string"),
        ("country", "string", "country", "string"),
        ("address", "string", "address", "string"),
        ("gender", "string", "gender", "string"),
        ("city", "string", "city", "string"),
        ("prefix", "string", "prefix", "string"),
        ("dob", "date", "dob", "string"),
        ("last_name", "string", "last_name", "string"),
        ("customer_id", "int", "customer_id", "int"),
        ("first_name", "string", "first_name", "string"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1677029070654",
)

# Script generated for node SQL Query
SqlQuery0 = """
select zip as customer_zip, sum_total_sales from myDataSource
where product_type like 'Exercise Pen%' and sum_total_sales >= 2000

"""
SQLQuery_node1677030523756 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": AggregateSales_node1677030461398},
    transformation_ctx="SQLQuery_node1677030523756",
)

# Script generated for node Detect PII - Names
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(
    ChangeSchemaApplyMapping_node1677029070654, [], 0.55, 0.85
)


def maskDf(df, keys):
    if not keys:
        return df
    df_to_mask = df.toDF()
    for key in keys:
        df_to_mask = df_to_mask.withColumn(key, lit("Current resident"))
    return DynamicFrame.fromDF(df_to_mask, glueContext, "updated_masked_df")


DetectPIINames_node1677029124013 = maskDf(
    ChangeSchemaApplyMapping_node1677029070654, list(classified_map.keys())
)

# Script generated for node Detect PII - Dates
entity_detector = EntityDetector()
detected_df = entity_detector.detect(
    DetectPIINames_node1677029124013, ["DateOfbirth"], "DetectedEntities"
)


def replace_cell(original_cell_value, sorted_reverse_start_end_tuples):
    if sorted_reverse_start_end_tuples:
        for entity in sorted_reverse_start_end_tuples:
            to_mask_value = original_cell_value[entity[0] : entity[1]]
            original_cell_value = original_cell_value.replace(
                to_mask_value, "Current resident"
            )
    return original_cell_value


def row_pii(column_name, original_cell_value, detected_entities):
    if column_name in detected_entities.keys():
        entities = detected_entities[column_name]
        start_end_tuples = map(
            lambda entity: (entity["start"], entity["end"]), entities
        )
        sorted_reverse_start_end_tuples = sorted(
            start_end_tuples, key=lambda start_end: start_end[1], reverse=True
        )
        return replace_cell(original_cell_value, sorted_reverse_start_end_tuples)
    return original_cell_value


row_pii_udf = udf(row_pii, StringType())


def recur(df, remaining_keys):
    if len(remaining_keys) == 0:
        return df
    else:
        head = remaining_keys[0]
        tail = remaining_keys[1:]
        modified_df = df.withColumn(
            head, row_pii_udf(lit(head), head, "DetectedEntities")
        )
        return recur(modified_df, tail)


keys = DetectPIINames_node1677029124013.toDF().columns
updated_masked_df = recur(detected_df.toDF(), keys)
updated_masked_df = updated_masked_df.drop("DetectedEntities")

DetectPIIDates_node1677030205574 = DynamicFrame.fromDF(
    updated_masked_df, glueContext, "updated_masked_df"
)

# Script generated for node Join - Agg Sales/PII
JoinAggSalesPII_node1677030718697 = Join.apply(
    frame1=SQLQuery_node1677030523756,
    frame2=DetectPIIDates_node1677030205574,
    keys1=["customer_zip"],
    keys2=["zip"],
    transformation_ctx="JoinAggSalesPII_node1677030718697",
)

# Script generated for node Amazon S3
AmazonS3_node1677030919840 = glueContext.getSink(
    path="s3://glue-workshop-us-east-1-983991953101/glue-output/mailing-list/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1677030919840",
)
AmazonS3_node1677030919840.setCatalogInfo(
    catalogDatabase="gluestudio", catalogTableName="mailing-list"
)
AmazonS3_node1677030919840.setFormat("glueparquet")
AmazonS3_node1677030919840.writeFrame(JoinAggSalesPII_node1677030718697)
job.commit()
