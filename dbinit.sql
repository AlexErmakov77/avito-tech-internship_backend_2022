-- Creation of user table
--CREATE DATABASE IF NOT EXISTS account_keeper;

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

CREATE TABLE IF NOT EXISTS users (
  user_id INT NOT NULL,
  amount INT NOT NULL,
  PRIMARY KEY (user_id)
);

CREATE TABLE IF NOT EXISTS history (
  id INT NOT NULL GENERATED ALWAYS AS IDENTITY,
  user_id INT NOT NULL,
  amount INT NOT NULL,
  is_debit BOOL NOT NULL,
  time TIME NOT NULL,
  PRIMARY KEY (id)
);