
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerException;

public class BigQueryToSQL3 {

	public interface MyOptions extends PipelineOptions {

	}

	private static final Logger LOG = LoggerFactory.getLogger(BigQueryToSQL.class);

	/**
	 * Main entry point for executing the pipeline.
	 * @param args The command-line arguments to the pipeline.
	 * @throws SQLException
	 * @throws SQLServerException
	 */
	public static void main(String[] args) throws SQLServerException, SQLException {

		BigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BigQueryOptions.class);
		options.setTempLocation("gs://demo_dataflow/temp");
		options.setGcpTempLocation("gs://demo_dataflow/temp");
		run(options);
	}

	/**
	 * Runs the pipeline with the supplied options.
	 *
	 * @param options The execution parameters to the pipeline.
	 * @return The result of the pipeline execution.
	 * @throws SQLException
	 * @throws SQLServerException
	 */
	private static void run(PipelineOptions options) throws SQLServerException, SQLException {
		// Create the pipeline

		Pipeline pipeline = Pipeline.create(options);

		SQLServerDataSource ds = new SQLServerDataSource();
		ds.setServerName("demo-dataflow.database.windows.net"); // Replace with your server name
		ds.setDatabaseName("demodb"); // Replace with your database
		ds.setAADSecurePrincipalId("XXXXXXX"); // Replace with your user name
		ds.setAADSecurePrincipalSecret("XXXXX"); // Replace wi
		ds.setAuthentication("ActiveDirectoryServicePrincipal");
		DatabaseMetaData databaseMetaData = ds.getConnection().getMetaData();
		ResultSet resultSet = databaseMetaData.getTables(null, null, "DEMOTABLE", new String[] { "TABLE" });
		boolean tablexists = resultSet.next();
		System.out.println("Table Exists or not:" + tablexists);

		PCollection<TableRow> tableRowPCollection = pipeline.apply("Read from BigQuery",
				BigQueryIO.readTableRowsWithSchema()
						.fromQuery(
								"SELECT * FROM projectid.datatable.demotable limit 100000")
						.withoutValidation().usingStandardSql().withMethod(TypedRead.Method.DIRECT_READ));

		Schema schema = tableRowPCollection.getSchema();
		String inputFields = "";
		String columnFields = "";
		if (!tablexists) {
			String createTable = "CREATE TABLE demodb.dbo.demotable (";
			for (int i = 0; i < schema.getFieldCount(); i++) {
				String type = schema.getField(i).getType().toString();
				String sqlType = "nvarchar(MAX)";
				switch (type) {
				case "INT64":
					sqlType = "bigint";
					break;
				case "STRING":
					sqlType = "nvarchar(MAX)";
					break;
				case "TIMESTAMP":
					sqlType = "datetime";
					break;
				case "BOOLEAN":
					sqlType = "bit";
					break;
				}
				if (i < schema.getFieldCount() - 1) {
					createTable = createTable.concat(schema.nameOf(i) + " " + sqlType + ",");
				} else {
					createTable = createTable.concat(schema.nameOf(i) + " " + sqlType + ')');
				}
			}
			System.out.println(createTable);
			Statement stmt = ds.getConnection().createStatement();
			stmt.executeUpdate(createTable);
			System.out.println("Created table in given database...");

		}
		for (int i = 0; i < schema.getFieldCount(); i++) {
			if (i < schema.getFieldCount() - 1) {
				inputFields = inputFields.concat("?,");
			} else {
				inputFields = inputFields.concat("?");
			}
		}
		for (int i = 0; i < schema.getFieldCount(); i++) {
			System.out.println(schema.getField(i).getType());
			if (i < schema.getFieldCount() - 1) {

				columnFields = columnFields.concat(schema.nameOf(i) + ",");
			} else {
				columnFields = columnFields.concat(schema.nameOf(i));
			}
		}
		String insertquery = "Insert into demodb.demotable (" + columnFields + ") values(" + inputFields
				+ ")";
		System.out.println(insertquery);
		System.out.println(schema.getFieldNames().toString());

		tableRowPCollection

				.apply(MapElements.into(TypeDescriptor.of(TableRow.class))
						// Use TableRow to access individual fields in the row.
						.via((TableRow row) -> {
							return row;
						}))
				.apply(JdbcIO.<TableRow>write().withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(ds))
						.withStatement(insertquery)
						.withAutoSharding()
						.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<TableRow>() {
							public void setParameters(TableRow element, PreparedStatement query) throws SQLException {
								// System.out.println("Total Columns" + schema.getFieldCount());

								for (int i = 0; i < schema.getFieldCount(); i++) {

									String type = schema.getField(i).getType().toString();
									// System.out.println(type);
									// System.out.println(element.get(schema.nameOf(i)));
									switch (type) {
									case "INT64":
										if (element.get(schema.nameOf(i)) != null) {
										query.setInt(i + 1, Integer.parseInt(element.get(schema.nameOf(i)).toString()));
										} else {
											query.setString(i + 1, null);
										}
										break;
									case "STRING":
										query.setString(i + 1, (String) element.get(schema.nameOf(i)));
										break;
									case "TIMESTAMP":
										query.setTimestamp(i + 1, (Timestamp) element.get(schema.nameOf(i)));
										break;
									case "BOOLEAN":
										if (element.get(schema.nameOf(i)) != null) {
										query.setBoolean(i + 1,
												Boolean.parseBoolean(element.get(schema.nameOf(i)).toString()));
										} else {
											query.setString(i + 1, null);
										}
										break;
									default:
										query.setString(i + 1, (String) element.get(schema.nameOf(i)));
									}

								}
							}
						}));

		pipeline.run().waitUntilFinish();

	}

}
