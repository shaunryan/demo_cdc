CREATE TABLE {{ database_name }}.{{ table_name }}
(
	`key` integer,
	`id` integer,
	`first_name` string  ,
	`last_name` string  ,
	`email` string  ,
	`gender` string  ,
	`job_title` string  ,
	`amount` double  ,
	`start_date` date  ,
	`end_date` date  ,
	`is_current` string  ,
	`_corrupt_record` string  ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_dataset_id` string NOT NULL 
)
USING DELTA LOCATION '{{ path }}'
;