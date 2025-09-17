/*
 * Copyright (c) 2025 the original author or authors.
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the “Software”), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package org.spaceframework.cloud.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * A Flink Table API job that performs Change Data Capture (CDC) from multiple MySQL databases and
 * synchronizes the data to Elasticsearch.
 *
 * <p>This application reads configuration from environment variables for connecting to MySQL and
 * Elasticsearch. It dynamically creates a separate Flink SQL job for each specified MySQL database
 * to capture changes from the `t_user` table and insert them into a unified Elasticsearch index.
 *
 * <p>The job sets up:
 *
 * <ul>
 *   <li>A MySQL CDC table for each database using the 'mysql-cdc' connector.
 *   <li>A unified Elasticsearch sink table using the 'elasticsearch-7' connector.
 *   <li>An {@code INSERT INTO} query to move data from the CDC source to the Elasticsearch sink.
 * </ul>
 *
 * <p>
 *
 * <h3>Environment Variables:</h3>
 *
 * <ul>
 *   <li>{@code MYSQL_DATA_HOST}: MySQL hostname (default: `mysql-data`)
 *   <li>{@code MYSQL_DATA_PORT}: MySQL port (default: `3306`)
 *   <li>{@code MYSQL_DATA_USER_NAME}: MySQL username (default: `root`)
 *   <li>{@code MYSQL_ROOT_PASSWORD}: MySQL password (default: `password`)
 *   <li>{@code MYSQL_DATABASES}: Comma-separated list of database names to monitor (default:
 *       `space_cloud_default,space_cloud_tenant1,space_cloud_tenant2`)
 *   <li>{@code ES_HOST}: Elasticsearch hosts, in the format `hostname` (default: `elasticsearch`)
 *   <li>{@code ES_PORT}: Elasticsearch port (default: `9200`)
 *   <li>{@code ELASTIC_USER_NAME}: Elasticsearch username (default: `elastic`)
 *   <li>{@code ELASTIC_PASSWORD}: Elasticsearch password (default: `password`)
 * </ul>
 *
 * @author ZetoHkr
 */
public class MySqlElasticsearchChangeDataCaptureTableApi {

  /**
   * The main entry point for the Flink job.
   *
   * @param args Command line arguments (not used)
   */
  public static void main(String[] args) {

    String mysqlHost = System.getenv().getOrDefault("MYSQL_DATA_HOST", "mysql-data");
    String mysqlPort = System.getenv().getOrDefault("MYSQL_DATA_PORT", "3306");
    String mysqlUser = System.getenv().getOrDefault("MYSQL_DATA_USER_NAME", "root");
    String mysqlPassword = System.getenv().getOrDefault("MYSQL_ROOT_PASSWORD", "password");
    String mysqlDatabasesStr =
        System.getenv()
            .getOrDefault(
                "MYSQL_DATABASES", "space_cloud_default,space_cloud_tenant1,space_cloud_tenant2");
    String[] mysqlDatabases = mysqlDatabasesStr.split(",");

    String esHost = System.getenv().getOrDefault("ES_HOST", "elasticsearch");
    String esPort = System.getenv().getOrDefault("ES_PORT", "9200");
    String esUser = System.getenv().getOrDefault("ELASTIC_USER_NAME", "elastic");
    String esPassword = System.getenv().getOrDefault("ELASTIC_PASSWORD", "password");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    env.enableCheckpointing(5000);

    for (String db : mysqlDatabases) {

      // Create MySQL CDC table
      tableEnv.executeSql(
          String.format(
              """
                  CREATE TABLE `t_user_cdc_%s` (
                      `id` STRING,
                      `username` STRING,
                      `email` STRING,
                      `phone` STRING,
                      `source_db` STRING METADATA FROM 'database_name',
                      PRIMARY KEY (`id`) NOT ENFORCED
                  ) WITH (
                    'connector' = 'mysql-cdc',
                    'hostname' = '%s',
                    'port' = '%s',
                    'username' = '%s',
                    'password' = '%s',
                    'database-name' = '%s',
                    'table-name' = 't_user'
                  );""",
              db, mysqlHost, mysqlPort, mysqlUser, mysqlPassword, db));

      // Create Elasticsearch sink table
      tableEnv.executeSql(
          String.format(
              """
                  CREATE TABLE `t_user_es_%s` (
                      `id` STRING,
                      `username` STRING,
                      `email` STRING,
                      `phone` STRING,
                      `source_db` STRING,
                      PRIMARY KEY (`id`) NOT ENFORCED
                  ) WITH (
                    'connector' = 'elasticsearch-7',
                    'hosts' = '%s',
                    'index' = 't_user_es_space_cloud',
                    'format' = 'json',
                    'username' = '%s',
                    'password' = '%s'
                  );""",
              db, esHost + ":" + esPort, esUser, esPassword));

      // Insert data from MySQL to Elasticsearch
      tableEnv.executeSql(
          String.format(
              "INSERT INTO `t_user_es_%s` "
                  + "SELECT id, username, email, phone, source_db FROM t_user_cdc_%s",
              db, db));
    }
  }
}
