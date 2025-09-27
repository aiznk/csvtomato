CREATE TABLE users IF NOT EXISTS (id INTEGER PRIMARY KEY AUTOINCREMENT, age INTEGER, name TEXT);
DELETE FROM users;
INSERT INTO users (age, name) VALUES (10, "Aho"), (20, "Boke"), (30, "Manuke");
SELECT * FROM users;
