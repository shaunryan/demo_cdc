CREATE TABLE {{ database_name }}.{{ table_name }}
(
  	`flag` string  ,
	`id` integer  ,
	`first_name` string  ,
	`last_name` string  ,
	`email` string  ,
	`gender` string  ,
	`job_title` string  ,
	`amount` double  ,
	`_timeslice` timestamp  ,
	`_filepath_filename` string NOT NULL ,
	`_dataset_id` string NOT NULL 
)
USING DELTA LOCATION '{{ path }}'
;