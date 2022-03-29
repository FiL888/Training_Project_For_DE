CREATE TABLE fil8_STG_all_data as
SELECT 
	transactions.trans_id,
	transactions.trans_date,
	transactions.amt,
	transactions.card_num,
	transactions.oper_type,
	transactions.oper_result,
	transactions.terminal,
	cards.account,
	cards.valid_to,
	cards.client,
	cards.fio,
	cards.date_of_birth,
	cards.passport_num,
	cards.passport_valid_to,
	cards.phone,
	terminals.terminal_type,
	terminals.terminal_city,
	terminals.terminal_address
FROM DE3TN.FIL8_DWH_FACT_TRANSACTIONS transactions
LEFT JOIN 
	(SELECT 
		cards.card_num, 
		cards.account,
		accounts.valid_to,
		accounts.client,
		accounts.fio,
		accounts.date_of_birth,
		accounts.passport_num,
		accounts.passport_valid_to,
		accounts.phone
	FROM DE3TN.FIL8_DWH_DIM_CARDS_HIST cards
	LEFT JOIN 
		(SELECT
			accounts.account,
			accounts.valid_to,
			accounts.client,
			clients.fio,
			clients.date_of_birth,
			clients.passport_num,
			clients.passport_valid_to,
			clients.phone
		FROM DE3TN.FIL8_DWH_DIM_ACCOUNTS_HIST accounts
		LEFT JOIN
			(SELECT
				clients.client_id,
				clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic fio,
				clients.date_of_birth,
				clients.passport_num,
				CASE 
				WHEN clients.passport_valid_to IS NULL 
				THEN TO_DATE('2999-12-31 23:59:59','YYYY-MM-DD HH24:MI:SS')
				ELSE clients.passport_valid_to
				END passport_valid_to,
				clients.phone
			FROM DE3TN.FIL8_DWH_DIM_CLIENTS_HIST clients
			WHERE clients.deleted_flg = 0 
		   		AND SYSDATE BETWEEN clients.effective_from AND clients.effective_to) clients
		ON accounts.client = clients.client_id
		WHERE accounts.deleted_flg = 0 
		   	AND SYSDATE BETWEEN accounts.effective_from AND accounts.effective_to) accounts
ON cards.account = accounts.account
WHERE cards.deleted_flg = 0 
   	AND SYSDATE BETWEEN cards.effective_from AND cards.effective_to) cards
ON transactions.CARD_NUM = cards.CARD_NUM
LEFT JOIN
	(SELECT
		terminals.terminal_id,
		terminals.terminal_type,
		terminals.terminal_city,
		terminals.terminal_address
	FROM DE3TN.FIL8_DWH_DIM_TERMINALS_HIST terminals
	WHERE terminals.deleted_flg = 0 
   		AND SYSDATE BETWEEN terminals.effective_from AND terminals.effective_to) terminals
ON transactions.terminal = terminals.terminal_id
ORDER BY transactions.trans_date