import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node MySQL - Sales
MySQLSales_node1677023397166 = glueContext.create_dynamic_frame.from_catalog(
    database="gluestudio",
    table_name="gluestudio_sales",
    transformation_ctx="MySQLSales_node1677023397166",
)

# Script generated for node MySQL - Customers
MySQLCustomers_node1677023550844 = glueContext.create_dynamic_frame.from_catalog(
    database="gluestudio",
    table_name="gluestudio_customers",
    transformation_ctx="MySQLCustomers_node1677023550844",
)

# Script generated for node MYSQL - Products
MYSQLProducts_node1677023622921 = glueContext.create_dynamic_frame.from_catalog(
    database="gluestudio",
    table_name="gluestudio_products",
    transformation_ctx="MYSQLProducts_node1677023622921",
)

# Script generated for node Customers - Resolve Column Conflict
CustomersResolveColumnConflict_node1677023866630 = ApplyMapping.apply(
    frame=MySQLCustomers_node1677023550844,
    mappings=[
        ("zip", "string", "zip", "string"),
        ("country", "string", "country", "string"),
        ("address", "string", "address", "string"),
        ("gender", "string", "gender", "string"),
        ("city", "string", "city", "string"),
        ("prefix", "string", "prefix", "string"),
        ("dob", "date", "dob", "date"),
        ("last_name", "string", "last_name", "string"),
        ("customer_id", "int", "customers_customer_id", "int"),
        ("first_name", "string", "first_name", "string"),
    ],
    transformation_ctx="CustomersResolveColumnConflict_node1677023866630",
)

# Script generated for node Join - Sales/Customers
JoinSalesCustomers_node1677024108427 = Join.apply(
    frame1=MySQLSales_node1677023397166,
    frame2=CustomersResolveColumnConflict_node1677023866630,
    keys1=["customer_id"],
    keys2=["customers_customer_id"],
    transformation_ctx="JoinSalesCustomers_node1677024108427",
)

# Script generated for node Join - Sales/Customers/Product
JoinSalesCustomersProduct_node1677024444831 = Join.apply(
    frame1=JoinSalesCustomers_node1677024108427,
    frame2=MYSQLProducts_node1677023622921,
    keys1=["product_id"],
    keys2=["product_key"],
    transformation_ctx="JoinSalesCustomersProduct_node1677024444831",
)

# Script generated for node Filter out zero sales
Filteroutzerosales_node1677024609636 = Filter.apply(
    frame=JoinSalesCustomersProduct_node1677024444831,
    f=lambda row: (row["quantity"] > 0),
    transformation_ctx="Filteroutzerosales_node1677024609636",
)

# Script generated for node Aggregate
Aggregate_node1677024806919 = sparkAggregate(
    glueContext,
    parentFrame=Filteroutzerosales_node1677024609636,
    groups=["zip", "product_type", "category"],
    aggs=[["total_sales", "sum"]],
    transformation_ctx="Aggregate_node1677024806919",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1677025801018 = ApplyMapping.apply(
    frame=Aggregate_node1677024806919,
    mappings=[
        ("zip", "string", "zip", "string"),
        ("product_type", "string", "product_type", "string"),
        ("category", "string", "category", "string"),
        ("`sum(total_sales)`", "decimal", "sum_total_sales", "double"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1677025801018",
)

# Script generated for node Amazon S3
AmazonS3_node1677025897307 = glueContext.getSink(
    path="s3://glue-workshop-us-east-1-983991953101/glue-output/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1677025897307",
)
AmazonS3_node1677025897307.setCatalogInfo(
    catalogDatabase="gluestudio", catalogTableName="sales-aggregation-report"
)
AmazonS3_node1677025897307.setFormat("glueparquet")
AmazonS3_node1677025897307.writeFrame(ChangeSchemaApplyMapping_node1677025801018)
job.commit()
