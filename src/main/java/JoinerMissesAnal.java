import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.*;
import java.util.Calendar;
import java.util.function.Function;

/**
 * Created by pawnow on 8/28/17.
 */
public class JoinerMissesAnal {

	public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
//		jdbcDriver jdbcDriver = (org.hsqldb.jdbcDriver) Class.forName("org.hsqldb.jdbcDriver").newInstance();
////		Class.forName("org.h2.Driver");
//		Connection connexion = DriverManager.getConnection("jdbc:hsqldb:test", "sa",  "");
//		connexion.prepareCall("CREATE CACHED TABLE cachedTable(pvId UUID, dataDodania TIMESTAMP , hittimestamp TIMESTAMP ); ").execute();
		Connection connection = null;

		{
			try {

				connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/pawnow", "pawnow", "1234");

			} catch (SQLException e) {

				System.out.println("Connection Failed! Check output console");
				e.printStackTrace();

			}
		}
		Timestamp timestamp ;
				ResultSet rs = connection.prepareCall("SELECT MAX(dataDodania) FROM pvstats2").executeQuery();//

		rs.next();
				timestamp=rs.getTimestamp(1);
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DAY_OF_MONTH,-0);
		KafkaWorker kafkaWorker = new KafkaWorker("JoinerMissesAnal", KafkaWorker.Evn.PROD, "ParsedDataCaptureComponents");

		try {

			Class.forName("org.postgresql.Driver");

		} catch (ClassNotFoundException e) {

			System.out.println("Where is your PostgreSQL JDBC Driver? "
					+ "Include in your library path!");
			e.printStackTrace();
			return;
		}
		System.out.println("PostgreSQL JDBC Driver Registered!");
			kafkaWorker.workWithWorker(new Function<ConsumerRecord<String, JsonNode>, Boolean>() {
				Connection connection = null;

				{
					try {

						connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/pawnow", "pawnow", "1234");

					} catch (SQLException e) {

						System.out.println("Connection Failed! Check output console");
						e.printStackTrace();

					}
				}

				CallableStatement callableStatement = connection.prepareCall("INSERT INTO pvStats2 VALUES(?, ? ,?)");
				@Override
				public Boolean apply(ConsumerRecord<String, JsonNode> record) {
					String guid = record.value().get("pageviewid").asText();
					long hittimestamp = record.value().get("hittimestamp").asLong();


//					UUID idUuid = UUID.fromString(guid);
					try {
						callableStatement.setString(1, guid);
						callableStatement.setTimestamp(2, new Timestamp(record.timestamp()));
						callableStatement.setTimestamp(3, new Timestamp(hittimestamp));
						callableStatement.execute();
					} catch (SQLException e) {
						e.printStackTrace();
					}
					return false;
				}
			}, timestamp!=null?timestamp.getTime():calendar.getTimeInMillis());



//			connection.prepareCall("INSERT INTO cachedTable VALUES('5730e1ba-160f-4fc3-8341-2702f3ea15aa',TIMESTAMP '2011-05-16 15:36:38',TIMESTAMP '2011-05-16 15:36:38')").execute();


	}
}
