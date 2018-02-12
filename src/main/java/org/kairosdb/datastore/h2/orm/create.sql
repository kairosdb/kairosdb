CREATE CACHED TABLE metric (
	"id" VARCHAR  NOT NULL,
	"name" VARCHAR  NULL,
	"type" VARCHAR  NULL,
	PRIMARY KEY ("id")
	);

CREATE CACHED TABLE service_modification (
	"service" VARCHAR  NOT NULL,
	"service_key" VARCHAR  NOT NULL,
	"modification_time" TIMESTAMP  NULL,
	PRIMARY KEY ("service", "service_key")
	);

CREATE CACHED TABLE service_index (
	"service" VARCHAR  NOT NULL,
	"service_key" VARCHAR  NOT NULL,
	"key" VARCHAR  NOT NULL,
	"value" VARCHAR  NULL,
	"modification_time" TIMESTAMP  NULL,
	PRIMARY KEY ("service", "service_key", "key")
	);

CREATE CACHED TABLE tag (
	"name" VARCHAR  NOT NULL,
	"value" VARCHAR  NOT NULL,
	PRIMARY KEY ("name", "value")
	);

CREATE CACHED TABLE data_point (
	"metric_id" VARCHAR  NOT NULL,
	"timestamp" TIMESTAMP  NOT NULL,
	"value" BINARY  NULL,
	PRIMARY KEY ("metric_id", "timestamp"),
	CONSTRAINT data_point_metric_id_fkey FOREIGN KEY ("metric_id")
		REFERENCES metric ("id") 
	);

CREATE CACHED TABLE metric_tag (
	"metric_id" VARCHAR  NOT NULL,
	"tag_name" VARCHAR  NOT NULL,
	"tag_value" VARCHAR  NOT NULL,
	PRIMARY KEY ("metric_id", "tag_name", "tag_value"),
	CONSTRAINT metric_tag_metric_id_fkey FOREIGN KEY ("metric_id")
		REFERENCES metric ("id") ON DELETE CASCADE,
	CONSTRAINT metric_tag_tag_name_fkey FOREIGN KEY ("tag_name", "tag_value")
		REFERENCES tag ("name", "value") 
	);

