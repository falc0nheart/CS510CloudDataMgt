package cass_driver2.cass_driver2;


import com.datastax.driver.core.*;

import java.util.*;


public class Query3 {

	public static void main(String[] args) {
		Cluster cluster = Cluster.builder()
				.addContactPoints("54.186.36.251")
				.build();
		Session session = cluster.connect();

		// Find length_mid for each NB station.
		ResultSet stationID_lengthMidResults = getNBStations(session);
		List<Row> stationID_lengthMidList = new ArrayList<Row>();
		for (Row row : stationID_lengthMidResults) {
			stationID_lengthMidList.add(row);
		}

		// Find total length of 205 NB, just for the heck of it.
		double highwayLength = 0;
		for (Row row : stationID_lengthMidList) {
			highwayLength += row.getDouble(1);
		}
		System.out.println("\nTotal NB 205 length:  " + highwayLength + " miles\n");

		// Query "LoopData" columnFamily, for each relevant (NB) StationID.
		List<List<Row>> loopDataFilteredList = new ArrayList<List<Row>>(6);
		List<Row> newList;
		List<Row> listRow;
		for (int i = 0; i < 6; i++) {
			newList = new ArrayList<Row>();
			loopDataFilteredList.add(i, newList);
		}
		for (Row stationRow : stationID_lengthMidList) {
			String cqlQueryLoopData = "SELECT \"StationID\", \"StartHour\", \"StartMinute\", \"DayOfWeek\", \"Speed\""
					+ " FROM \"CloudDataMgt\".\"LoopData\" WHERE \"StationID\" = " + stationRow.getInt(0) + " LIMIT 100000;";
			ResultSet loopDataResults = session.execute(cqlQueryLoopData);

			// Filter rows for specific rush hours and days of week, and create a List of 6 Lists
			// holding all of the rush-hour/day-of-week combinations.
			for (Row row : loopDataResults) {
				if (row.getInt(1) >= 7 && row.getInt(1) < 9) {
					if (row.getString(3).equals("Tuesday")) {
						listRow = loopDataFilteredList.remove(0);
						listRow.add(row);
						loopDataFilteredList.add(0, listRow);
					}
					else if (row.getString(3).equals("Wednesday")) {
						listRow = loopDataFilteredList.remove(1);
						listRow.add(row);
						loopDataFilteredList.add(1, listRow);
					}
					else if (row.getString(3).equals("Thursday")) {
						listRow = loopDataFilteredList.remove(2);
						listRow.add(row);
						loopDataFilteredList.add(2, listRow);
					}
				}
				else if (row.getInt(1) >= 16 && row.getInt(1) < 18) {
					if (row.getString(3).equals("Tuesday")) {
						listRow = loopDataFilteredList.remove(3);
						listRow.add(row);
						loopDataFilteredList.add(3, listRow);
					}
					else if (row.getString(3).equals("Wednesday")) {
						listRow = loopDataFilteredList.remove(4);
						listRow.add(row);
						loopDataFilteredList.add(4, listRow);
					}
					else if (row.getString(3).equals("Thursday")) {
						listRow = loopDataFilteredList.remove(5);
						listRow.add(row);
						loopDataFilteredList.add(5, listRow);
					}
				}
			}
		}
		//System.out.println(loopDataFilteredList.get(2).get(0));

		// Calculate and print average travel times for each of the 6 time periods of interest.
		List<Double> avgTravelTimes = new ArrayList<Double>(6);
		Double periodTravelTimeSum;
		Integer periodTravelTimeCount;
		int periodRowStationID;
		for (int i = 0; i < 6; i++) {
			periodTravelTimeSum = 0.0;
			periodTravelTimeCount = 0;
			for (Row periodRow : loopDataFilteredList.get(i)) {
				//System.out.println(loopDataFilteredList.get(i));
				//System.out.println(periodRow);
				periodRowStationID = periodRow.getInt(0);
				//System.out.println(stationID_lengthMidList);
				for (Row stationRow : stationID_lengthMidList) {
					//System.out.println(stationRow.getInt(0) + " " + periodRowStationID + " " + periodRow.getInt(4));
					if (stationRow.getInt(0) == periodRowStationID) {
						if (periodRow.getInt(4) != 0) {
							periodTravelTimeSum += (stationRow.getDouble(1) / (double)periodRow.getInt(4));
							periodTravelTimeCount += 1;
						}
						break;
					}
				}
			}
			if (periodTravelTimeCount != 0)
				avgTravelTimes.add(i, periodTravelTimeSum / (double)periodTravelTimeCount);
			else
				avgTravelTimes.add(i, 0.0);
			switch (i) {
			case 0:  System.out.println("Tuesday AM rush:  " + avgTravelTimes.get(0) * 60 + " minutes");
			break;
			case 1:  System.out.println("Wednesday AM rush:  " + avgTravelTimes.get(1) * 60 + " minutes");
			break;
			case 2:  System.out.println("Thursday AM rush:  " + avgTravelTimes.get(2) * 60 + " minutes");
			break;
			case 3:  System.out.println("Tuesday PM rush:  " + avgTravelTimes.get(3) * 60 + " minutes");
			break;
			case 4:  System.out.println("Wednesday PM rush:  " + avgTravelTimes.get(4) * 60 + " minutes");
			break;
			case 5:  System.out.println("Thursday PM rush:  " + avgTravelTimes.get(5) * 60 + " minutes");
			break;
			default:  System.out.println("This case should not be reached.");
			break;
			}
		}
		System.exit(0);
	}

	/*
	 * method: getNBStations
	 * argument: Session session - current Cassandra session
	 *
	 * This method will return all northbound StationIDs and their LengthMid in the form of a ResultSet.
	 */
	public static ResultSet getNBStations (Session session) {
		// Query "Stations" columnFamily.
		String cqlQueryStations = "SELECT \"StationID\", \"LengthMid\" FROM \"CloudDataMgt\".\"Stations\""
				+ " WHERE \"ShortDirection\" = 'N' LIMIT 1000000000;";
		return session.execute(cqlQueryStations);
	}
}
