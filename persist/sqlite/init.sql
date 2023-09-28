CREATE TABLE hourly_contract_stats (
	date_created DATETIME PRIMARY KEY,
	active_contracts INTEGER NOT NULL,
	valid_contracts INTEGER NOT NULL,
	missed_contracts INTEGER NOT NULL,
	total_payouts BLOB NOT NULL,
	estimated_revenue BLOB NOT NULL
);

CREATE TABLE blocks (
	id INTEGER PRIMARY KEY,
	block_id BLOB UNIQUE NOT NULL,
	height INTEGER UNIQUE NOT NULL,
	date_created DATETIME NOT NULL
);

CREATE TABLE market_data (
	date_created DATETIME PRIMARY KEY,
	usd_rate BLOB NOT NULL,
	eur_rate BLOB NOT NULL,
	btc_rate BLOB NOT NULL
);

CREATE TABLE active_contracts (
	id INTEGER PRIMARY KEY,
	block_id INTEGER NOT NULL REFERENCES blocks (id),
	contract_id BLOB UNIQUE NOT NULL,
	initial_valid_revenue BLOB NOT NULL,
	initial_missed_revenue BLOB NOT NULL,
	initial_valid_payout_value BLOB NOT NULL,
	initial_missed_payout_value BLOB NOT NULL,
	valid_payout_value BLOB NOT NULL,
	missed_payout_value BLOB NOT NULL,
	expiration_height INTEGER NOT NULL,
	proof_block_id INTEGER REFERENCES blocks (id)
);
CREATE INDEX active_contracts_expiration_height_proof_block_id ON active_contracts (expiration_height, proof_block_id);

CREATE TABLE global_settings (
	id INTEGER PRIMARY KEY NOT NULL DEFAULT 0 CHECK (id = 0), -- enforce a single row
	db_version INTEGER NOT NULL, -- used for migrations
	contracts_last_processed_change BLOB, -- last processed consensus change for the contract manager
	contracts_height INTEGER -- height of the contract manager as of the last processed change
);

-- initialize the global settings table
INSERT INTO global_settings (id, db_version) VALUES (0, 0); -- should not be changed