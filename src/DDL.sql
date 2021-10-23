
CREATE DATABASE db_metodo_prod;

DROP TABLE db_metodo_prod.latest_results;
CREATE TABLE db_metodo_prod.latest_results(
`home_team` varchar(255) DEFAULT NULL,
`away_team` varchar(255) DEFAULT NULL,
`home_odd` float DEFAULT NULL,
`draw_odd` float DEFAULT NULL,
`away_odd` float DEFAULT NULL,
`home_goals` int(11) DEFAULT NULL,
`away_goals` int(11) DEFAULT NULL,
`match_date` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
`competition_cod` varchar(10) DEFAULT NULL,
`match_cod` varchar(30) DEFAULT NULL
);

DROP TABLE db_metodo_prod.next;
CREATE TABLE db_metodo_prod.next(
`home_team` varchar(255) DEFAULT NULL,
`away_team` varchar(255) DEFAULT NULL,
`home_odd` float DEFAULT NULL,
`draw_odd` float DEFAULT NULL,
`away_odd` float DEFAULT NULL,
`match_date` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
`competition_cod` varchar(10) DEFAULT NULL,
`match_cod` varchar(30) DEFAULT NULL
);

DROP TABLE db_metodo_prod.next_metodo;
CREATE TABLE db_metodo_prod.next_metodo (
`home_team` varchar(255) DEFAULT NULL,
`away_team` varchar(255) DEFAULT NULL,
`home_odd` float DEFAULT NULL,
`draw_odd` float DEFAULT NULL,
`away_odd` float DEFAULT NULL,
`match_date` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
`competition_cod` varchar(10) DEFAULT NULL,
`match_cod` varchar(30) DEFAULT NULL,
`last_lost_odd` float DEFAULT NULL,
`last_lost_home` boolean DEFAULT NULL,
`last_competition_cod` varchar(10) DEFAULT NULL,
`last_diff_goals` int DEFAULT NULL,
`tip` varchar(2) DEFAULT NULL
);

DROP TABLE db_metodo_prod.metodo_results;
CREATE TABLE db_metodo_prod.metodo_results (
`home_team` varchar(255) DEFAULT NULL,
`away_team` varchar(255) DEFAULT NULL,
`home_odd` float DEFAULT NULL,
`draw_odd` float DEFAULT NULL,
`away_odd` float DEFAULT NULL,
`match_date` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
`tip` varchar(2) DEFAULT NULL,
`competition_cod` varchar(10) DEFAULT NULL,
`home_goals` int DEFAULT NULL,
`away_goals` int DEFAULT NULL,
`last_lost_home` boolean DEFAULT NULL,
`last_diff_goals` int DEFAULT NULL,
`last_lost_odd` float DEFAULT NULL,
`last_competition_cod` varchar(10) DEFAULT NULL,
`match_cod` varchar(30)
);

CREATE TABLE db_metodo_prod.hash_table (
`competition_cod` varchar(5) DEFAULT NULL,
`hash` varchar(32) DEFAULT NULL);

DROP TABLE db_metodo_prod.standings;
CREATE TABLE db_metodo_prod.standings (
`team` varchar(255) DEFAULT NULL,
`competition_cod` varchar(10) DEFAULT NULL,
`rank` int DEFAULT NULL,
`points` int DEFAULT NULL,
`hash` varchar(32) DEFAULT NULL);
