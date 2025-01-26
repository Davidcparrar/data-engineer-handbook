CREATE TABLE user_devices_cumulated (
	user_id NUMERIC, 
	device_id NUMERIC,
	browser_type TEXT,
	device_activity_datelist INTEGER[],
	PRIMARY KEY (user_id, device_id, browser_type)
);

