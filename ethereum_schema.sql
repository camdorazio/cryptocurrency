CREATE TABLE ethereum (
	block_timestamp Timestamp,
	transaction_value Numeric,
	receipt_gas_used Numeric,
	gas_price VARCHAR(40),
	gwei_unit_transaction_cost VARCHAR(40),
	ethereum_transaction_cost Numeric
);

SELECT * FROM ethereum LIMIT 100;