e<b><h1>Data Warehouse and Analytics Project</h1></b>
Welcome to the <b>Data Warehouse</b> and <b>Analytics Project </b>repository<br>
This projects demonstrates the comprehensive data warehousing and analytics solution ,from building a data warehouse to gaining actionable insights.


<h1>🏗️ Data Architecture</h1>
The Data Architecture for the project follows the Medallion Architecture that has <b>Bronze</b>,<b>Silver</b> and <b>Gold</b>layers.

![Data Architecture](https://raw.githubusercontent.com/Pradhyum-git/sql-datawarehouse-project/main/docs/data_architecture.drawio.png)


<b>1.Bronze :</b> Stores raw data as is from source .Using Python sqlalchemy and pandas library csv file data is ingested to tabels.<br>
<b>2.Silver :</b> Transformed raw data into cleaned and structured data ,techniques performed on raw data is Data Normalisation and data standardization for data analysis.<br>
<b>3.Gold :</b>  This layer creates new Data Model followed by Star Schema required for business reports.
