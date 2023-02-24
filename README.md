# AWS-Glue-Workshop
Building ETL jobs using AWS Glue

In the quest to learn and explore about data engineering and building ETL pipelines, I stumbled upon this workshop. It was an amazing workshop in the sense that I got to learn a ton and was able to build ETL pipeline on although a fictitious data but a real-world scenario of an online pet product retailer. 
Also the best part was that I was able to do the hands-on without using my own AWS credentials, and instead a temporary account created by the Event Engine itself, which I was to login using the OTP received on my email.

# The following below is the problem statement as taken directly from the workshop lab steps:

“Company ABC is an online pet product retailer. They have recently launched a new product line for pets. They were expecting it to be a great success but sales from some regions were below their expectation. Marketing research suggests that limited awareness about the product in those regions is the main reason for inadequate sales. To address this problem, they want to start a targeted mail campaign in those regions that shows potential for increased sales. The team needs a fast turnaround, they don’t have much time to write code and manage infrastructure. At the same time, to avoid using any Personal Identifiable Information (PII) in their mail campaign, any customer PII data need to be redacted from their mailing list.”

# As a Data Engineer I was required to perform the following tasks:

“Extract customer, product, and sales data. Following the extract, you transform that data and create a dataset that has sales performance data for pet product categories by zip codes where specific product types need to be marketed. Sensitive data also needs to be redacted before allowing downstream analytics.”

# ETL Job 1:
At the end of creating the first ETL job 1, I was able to learn the following:
1.	Extracting Data from MySQL DB that was created as an instance on RDS.
2.	Visually transforming data by performing multiple actions of changing data types, selecting specific columns, joining tables, filtering data, and performing aggregations. 
3.	Created the sales performance dataset for ABC's product lines by Region, Category and Product Type where mail marketing campaign analysis can be done

Now the next ETL job was to redact or anonymize the PII data before the dataset is handed over to downstream analytics.

# ETL Job 2:
At the end of creating the first ETL job 2, I was able to learn the following:
•	To redact sensitive PII data from the dataset by using Glue built-in PII feature without writing code.


