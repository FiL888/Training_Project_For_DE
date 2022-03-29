SELECT DISTINCT
	event_dt,
	passport,
	fio,
	phone,
	event_type,
	SYSDATE report_dt
FROM (	
		-- Секция с недействительными паспортами
		SELECT
			trans_date event_dt,
			passport_num passport,
			fio,
			phone,
			'Not valid or blocked passport' event_type
		FROM fil8_STG_all_data
		WHERE trans_date > passport_valid_to
			AND oper_result = 'SUCCESS'
		UNION
		-- Секция с паспортами из черного списка
		SELECT 
			all_data.trans_date,
			all_data.passport_num,
			all_data.fio,
			all_data.phone,
			'Not valid or blocked passport'
		FROM fil8_STG_all_data all_data
		INNER JOIN 
			(SELECT 
				all_data.trans_id,
				all_data.trans_date,
				CASE 
				WHEN blacklist.entry_dt IS NULL 
				THEN 0
				ELSE 
					CASE 
					WHEN all_data.trans_date > blacklist.entry_dt
					THEN 1
					ELSE 0
					END 
				END passport_is_blocked
			FROM FIL8_STG_ALL_DATA all_data
			LEFT JOIN 
				(SELECT *
				FROM DE3TN.FIL8_DWH_FACT_PSSPRT_BLCKLST) blacklist
			ON all_data.passport_num = blacklist.passport_num) blacklist
		ON all_data.trans_id = blacklist.trans_id 
			AND all_data.trans_date = blacklist.trans_date
		WHERE blacklist.passport_is_blocked = 1
			AND all_data.oper_result = 'SUCCESS'
		UNION
		-- Секция с недействующим договором
		SELECT
			trans_date,
			passport_num passport,
			fio,
			phone,
			'Not valid contract'
		FROM fil8_STG_all_data
		WHERE trans_date > valid_to
			AND oper_result = 'SUCCESS'
		UNION
		-- Секция В разных городах в течении часа 
		select 
			event_1.trans_date,
			event_1.passport_num,
			event_1.fio,
			event_1.phone,
			'Different cities into 1 hour' 
		FROM DE3TN.FIL8_STG_ALL_DATA event_1
		INNER JOIN DE3TN.FIL8_STG_ALL_DATA event_2
			ON event_1.passport_num = event_2.passport_num
		WHERE NOT (event_1.terminal_city = event_2.terminal_city)
			AND ((event_1.trans_date - event_2.trans_date) * 24) between 0.000000001 AND 1
			AND event_1.oper_result = 'SUCCESS'
			AND event_2.oper_result = 'SUCCESS'
		UNION 
		-- Секция Попытка подбора суммы
		SELECT 
			trans_date,
			passport_num passport,
			fio,
			phone,
			'Selection amount'
		FROM FIL8_STG_ALL_DATA
		WHERE trans_id IN (SELECT
								trans_id_3 trans_id
							FROM
								(SELECT
									passport_num,
									trans_date date_1,
									lead(trans_date, 1) over(PARTITION BY passport_num ORDER BY trans_date) date_2,
									lead(trans_date, 2) over(PARTITION BY passport_num ORDER BY trans_date) date_3,
									amt amt_1,
									lead(amt, 1) over(PARTITION BY passport_num ORDER BY trans_date) amt_2,
									lead(amt, 2) over(PARTITION BY passport_num ORDER BY trans_date) amt_3,
									oper_result oper_result_1,
									lead(oper_result, 1) over(PARTITION BY passport_num ORDER BY trans_date) oper_result_2,
									lead(oper_result, 2) over(PARTITION BY passport_num ORDER BY trans_date) oper_result_3,
									trans_id trans_id_1,
									lead(trans_id, 1) over(PARTITION BY passport_num ORDER BY trans_date) trans_id_2,
									lead(trans_id, 2) over(PARTITION BY passport_num ORDER BY trans_date) trans_id_3
								FROM FIL8_STG_ALL_DATA
								ORDER BY passport_num, trans_date, amt desc) t_agregator
							WHERE (CASE 
								WHEN oper_result_1 != 'SUCCESS' AND oper_result_2 != 'SUCCESS' AND oper_result_3 = 'SUCCESS' AND oper_result_2 IS NOT NULL AND oper_result_3 IS NOT NULL
								THEN 1
								ELSE 0
								END) = 1
								AND 
								(CASE 
								WHEN AMT_3 < AMT_2 AND AMT_2 < AMT_1 AND AMT_2 IS NOT NULL AND AMT_3 IS NOT NULL
								THEN 1
								ELSE 0
								END) = 1
								AND ((date_3 - date_1) * 24 * 60) BETWEEN 0.0000000001 AND 20)
	) all_steps
ORDER BY event_dt