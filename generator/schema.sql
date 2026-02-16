-- qianxiaoying-dashboard MySQL schema (v0)

CREATE TABLE IF NOT EXISTS daily_snapshot (
  trade_date DATE NOT NULL,
  json_data  LONGTEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS job_runs (
  id BIGINT NOT NULL AUTO_INCREMENT,
  job_name VARCHAR(64) NOT NULL,
  trade_date DATE NULL,
  status VARCHAR(16) NOT NULL,
  started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  finished_at TIMESTAMP NULL,
  error_text TEXT NULL,
  meta_json JSON NULL,
  PRIMARY KEY (id),
  KEY idx_job_date (job_name, trade_date, started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
