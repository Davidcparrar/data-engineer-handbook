-- Purpose: Create the table host_cumulated.
CREATE TABLE host_cumulated(
	host TEXT, 
	host_activity_datelist DATE[],
	date DATE,
	PRIMARY KEY (host, date)
);

