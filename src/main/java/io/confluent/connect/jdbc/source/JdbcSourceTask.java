/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.util.StringUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.JdbcUtils;
import io.confluent.connect.jdbc.util.Version;

/**
 * JdbcSourceTask is a Kafka Connect SourceTask implementation that reads from JDBC databases and
 * generates Kafka Connect records.
 */
public class JdbcSourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);

  private Time time;
  private JdbcSourceTaskConfig config;
  private CachedConnectionProvider cachedConnectionProvider;
  private PriorityQueue<TableQuerier> tableQueue = new PriorityQueue<TableQuerier>();
  private AtomicBoolean stop;

  public JdbcSourceTask() {
    this.time = new SystemTime();
  }

  public JdbcSourceTask(Time time) {
    this.time = time;
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) {
    try {
      config = new JdbcSourceTaskConfig(properties);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start JdbcSourceTask due to configuration error", e);
    }

    final String dbUrl = config.getString(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG);
    final String dbUser = config.getString(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG);
    final Password dbPassword = config.getPassword(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG);
    final Integer connectionAttempts = config.getInt(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG);
    final Long connectionBackoff = config.getLong(JdbcSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG);
    final int networkTimeout = config.getInt(JdbcSourceConnectorConfig.CONNECTION_NETWORK_TIMEOUT_CONFIG);

    cachedConnectionProvider = new CachedConnectionProvider(dbUrl, dbUser, dbPassword == null ? null : dbPassword.value(),
        connectionAttempts, connectionBackoff, networkTimeout);


    List<String> tables = config.getList(JdbcSourceTaskConfig.TABLES_CONFIG);
    String query = config.getString(JdbcSourceTaskConfig.QUERY_CONFIG);
    String namespacePrefix = config.getString(JdbcSourceTaskConfig.SCHEMA_NAMESPACE_PREFIX);
    namespacePrefix = namespacePrefix.isEmpty() ? StringUtils.EMPTY_STRING : namespacePrefix.concat(".");
    if ((tables.isEmpty() && query.isEmpty()) || (!tables.isEmpty() && !query.isEmpty())) {
      throw new ConnectException("Invalid configuration: each JdbcSourceTask must have at "
                                        + "least one table assigned to it or one query specified");
    }
    TableQuerier.QueryMode queryMode = !query.isEmpty() ? TableQuerier.QueryMode.QUERY :
                                       TableQuerier.QueryMode.TABLE;
    List<String> tablesOrQuery = queryMode == TableQuerier.QueryMode.QUERY ?
                                 Collections.singletonList(query) : tables;

    String mode = config.getString(JdbcSourceTaskConfig.MODE_CONFIG);
    Map<Map<String, String>, Map<String, Object>> offsets = null;
    if (mode.equals(JdbcSourceTaskConfig.MODE_INCREMENTING) ||
        mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP) ||
        mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {
      List<Map<String, String>> partitions = new ArrayList<>(tables.size());
      switch (queryMode) {
        case TABLE:
          for (String table : tables) {
            Map<String, String> partition =
                Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, table);
            partitions.add(partition);
          }
          break;
        case QUERY:
          partitions.add(Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                                                  JdbcSourceConnectorConstants.QUERY_NAME_VALUE));
          break;
      }
      offsets = context.offsetStorageReader().offsets(partitions);
    }

    String schemaPattern
        = config.getString(JdbcSourceTaskConfig.SCHEMA_PATTERN_CONFIG);
    String keyColumn
        = config.getString(JdbcSourceTaskConfig.KEY_COLUMN_NAME_CONFIG);
    String incrementingColumn
        = config.getString(JdbcSourceTaskConfig.INCREMENTING_COLUMN_NAME_CONFIG);
    String timestampColumn
        = config.getString(JdbcSourceTaskConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
    Long timestampDelayInterval
        = config.getLong(JdbcSourceTaskConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG);
    boolean validateNonNulls
        = config.getBoolean(JdbcSourceTaskConfig.VALIDATE_NON_NULL_CONFIG);

    for (String tableOrQuery : tablesOrQuery) {
      final Map<String, String> partition;
      String name;
      switch (queryMode) {
        case TABLE:
          if (validateNonNulls) {
            validateNonNullable(mode, schemaPattern, tableOrQuery, incrementingColumn, timestampColumn);
          }
          partition = Collections.singletonMap(
              JdbcSourceConnectorConstants.TABLE_NAME_KEY, tableOrQuery);
          name = namespacePrefix + tableOrQuery;
          break;
        case QUERY:
          partition = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                                               JdbcSourceConnectorConstants.QUERY_NAME_VALUE);
          name = tableOrQuery;
          break;
        default:
          throw new ConnectException("Unexpected query mode: " + queryMode);
      }
      Map<String, Object> offset = offsets == null ? null : offsets.get(partition);

      String topicPrefix = config.getString(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG);
      boolean mapNumerics = config.getBoolean(JdbcSourceTaskConfig.NUMERIC_PRECISION_MAPPING_CONFIG);
      int fetchSize = config.getInt(JdbcSourceTaskConfig.CONNECTION_FECTH_SIZE_CONFIG);

      if (mode.equals(JdbcSourceTaskConfig.MODE_BULK)) {
        tableQueue.add(new BulkTableQuerier(queryMode, tableOrQuery, name, schemaPattern,
                topicPrefix, keyColumn, mapNumerics, fetchSize));
      } else if (mode.equals(JdbcSourceTaskConfig.MODE_INCREMENTING)) {
        tableQueue.add(new TimestampIncrementingTableQuerier(
            queryMode, tableOrQuery, name, topicPrefix, null, incrementingColumn, keyColumn, offset,
                timestampDelayInterval, schemaPattern, mapNumerics, fetchSize));
      } else if (mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP)) {
        tableQueue.add(new TimestampIncrementingTableQuerier(
            queryMode, tableOrQuery, name, topicPrefix, timestampColumn, null, keyColumn, offset,
                timestampDelayInterval, schemaPattern, mapNumerics, fetchSize));
      } else if (mode.endsWith(JdbcSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {
        tableQueue.add(new TimestampIncrementingTableQuerier(
            queryMode, tableOrQuery, name, topicPrefix, timestampColumn, incrementingColumn, keyColumn,
                offset, timestampDelayInterval, schemaPattern, mapNumerics, fetchSize));
      }
    }

    stop = new AtomicBoolean(false);
  }

  @Override
  public void stop() throws ConnectException {
    if (stop != null) {
      stop.set(true);
    }
    if (cachedConnectionProvider != null) {
      cachedConnectionProvider.closeQuietly();
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    log.trace("{} Polling for new data");

    final int maxConnectionAttempts = config.values().containsKey(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG)
        ? config.getInt(JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG)
        : JdbcSourceConnectorConfig.CONNECTION_ATTEMPTS_DEFAULT;
    int attempts = 0;

    while (!stop.get()) {
      final TableQuerier querier = tableQueue.peek();

      log.debug("Is {} querying? {}", querier, querier.querying());
      if (!querier.querying()) {
        // If not in the middle of an update, wait for next update time
        Integer pollInterval = config.getInt(JdbcSourceTaskConfig.POLL_INTERVAL_MS_CONFIG);
        long lastUpdate = querier.getLastUpdate();
        final long nextUpdate = lastUpdate + pollInterval;
        final long untilNext = nextUpdate - time.milliseconds();

        log.debug("pollInterval: {}, lastUpdate: {}, nextUpdate: {}, untilNext: {}", pollInterval, lastUpdate,
            nextUpdate, untilNext);

        if (untilNext > 0) {
          log.debug("Waiting {} ms to poll {} next", untilNext, querier.toString());
          time.sleep(untilNext);
          continue; // Re-check stop flag before continuing
        }
      }

      final List<SourceRecord> results = new ArrayList<>();
      try {
        log.debug("Checking for next block of results from {}", querier.toString());
        querier.maybeStartQuery(cachedConnectionProvider.getValidConnection());

        int batchMaxRows = config.getInt(JdbcSourceTaskConfig.BATCH_MAX_ROWS_CONFIG);

        log.debug("batchMaxRows: {}", batchMaxRows);

        boolean hadNext = true;
        while (results.size() < batchMaxRows && (hadNext = querier.next())) {
          results.add(querier.extractRecord());
        }

        log.debug("May be reset ? results: {}, hadNext: {} : {}", results.size(), hadNext, querier.toString());

        if (!hadNext) {
          // If we finished processing the results from the current query, we can reset and send the querier to the tail of the queue
          resetAndRequeueHead(querier);
        }

        if (results.isEmpty()) {
          log.debug("No updates for {}", querier.toString());
          continue;
        }

        log.info("Returning {} records for {}", results.size(), querier.toString());
        return results;
      } catch (Exception e) {
        resetAndRequeueHead(querier);
        log.warn("No worries, we've got exception, skipping further processing and we'll reset connection on next poll.", e);
        if (!results.isEmpty()) {
          return results;
        }
        attempts++;
        if (attempts < maxConnectionAttempts) {
          log.warn("No worries, we failed but we'll retry {} more time to re-run query for table {}: {}",
              maxConnectionAttempts - attempts, querier.toString(), e);
          // there may be duplicates here but we don't worry about it 'cause we handle them on consumer side.
        } else {
          log.error("Failed to run query for table {}: {}", querier.toString(), e);
          return null;
        }
      }
    }

    // Only in case of shutdown
    return null;
  }

  private void resetAndRequeueHead(TableQuerier expectedHead) {
    log.debug("Resetting querier {}", expectedHead.toString());
    TableQuerier removedQuerier = tableQueue.poll();
    assert removedQuerier == expectedHead;
    expectedHead.reset(time.milliseconds());
    tableQueue.add(expectedHead);
  }

  private void validateNonNullable(String incrementalMode, String schemaPattern, String table, String incrementingColumn, String timestampColumn) {
    try {
      final Connection connection = cachedConnectionProvider.getValidConnection();
      // Validate that requested columns for offsets are NOT NULL. Currently this is only performed
      // for table-based copying because custom query mode doesn't allow this to be looked up
      // without a query or parsing the query since we don't have a table name.
      if ((incrementalMode.equals(JdbcSourceConnectorConfig.MODE_INCREMENTING) ||
           incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING)) &&
          JdbcUtils.isColumnNullable(connection, schemaPattern, table, incrementingColumn)) {
        throw new ConnectException("Cannot make incremental queries using incrementing column " +
                                   incrementingColumn + " on " + table + " because this column is "
                                   + "nullable.");
      }
      if ((incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP) ||
           incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING)) &&
          JdbcUtils.isColumnNullable(connection, schemaPattern, table, timestampColumn)) {
        throw new ConnectException("Cannot make incremental queries using timestamp column " +
                                   timestampColumn + " on " + table + " because this column is "
                                   + "nullable.");
      }
    } catch (SQLException e) {
      throw new ConnectException("Failed trying to validate that columns used for offsets are NOT"
                                 + " NULL", e);
    }
  }
}
