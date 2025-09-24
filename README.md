# MySQL to Elasticsearch CDC Application

This project is a Flink-based application designed to perform **Change Data Capture (CDC)** from MySQL databases and synchronize the data into Elasticsearch. The application monitors multiple MySQL databases, captures changes from the `t_user` table, and writes those changes to a unified Elasticsearch index.

## Features

* **Capture Changes**: The application uses Flink's CDC capabilities to monitor changes in MySQL.
* **Synchronize with Elasticsearch**: Captured changes are written to an Elasticsearch index.
* **Environment Configurable**: Connection details for MySQL and Elasticsearch can be configured through environment variables.
* **Multiple Databases**: The application supports multiple MySQL databases and dynamically creates CDC jobs for each.

## Requirements

* **Java 17**
* **Apache Flink 1.20.0** (with `mysql-cdc` and `elasticsearch-7` connectors)
* **MySQL** (with CDC-enabled tables)
* **Elasticsearch 7.x**

## Setup and Configuration

### Environment Variables

Before running the application, you need to set the following environment variables to configure the connections to MySQL and Elasticsearch:

* **MYSQL_DATA_HOST**: The hostname of the MySQL instance. Default: `mysql-data`
* **MYSQL_DATA_PORT**: The port for MySQL. Default: `3306`
* **MYSQL_DATA_USER_NAME**: MySQL username. Default: `root`
* **MYSQL_ROOT_PASSWORD**: MySQL password. Default: `password`
* **MYSQL_DATABASES**: Comma-separated list of databases to monitor. Default: `space_cloud_default,space_cloud_tenant1,space_cloud_tenant2`
* **ES_HOST**: The hostname of the Elasticsearch instance. Default: `elasticsearch`
* **ES_PORT**: The port for Elasticsearch. Default: `9200`
* **ELASTIC_USER_NAME**: Elasticsearch username. Default: `elastic`
* **ELASTIC_PASSWORD**: Elasticsearch password. Default: `password`

## Running the Application

### Locally

1. Set the necessary environment variables based on your configuration.
2. Run the Flink job with your Flink setup, ensuring the necessary connectors are available.

### Docker (Optional)

You can use Docker to run the application. You may need to create a Docker Compose file to orchestrate MySQL, Elasticsearch, and Flink.

## Troubleshooting

* Ensure your MySQL server has CDC enabled and the `t_user` table is properly configured for CDC.
* Verify that the MySQL and Elasticsearch servers are accessible from your Flink job.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
