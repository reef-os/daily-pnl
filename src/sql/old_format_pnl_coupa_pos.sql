SELECT
	uc. "vessel name",
	uc.country,
	CASE WHEN cpr.location_code LIKE '___-___-____'
		OR cpr.location_code LIKE '___-___-__' THEN
		upper(
		left(cpr.location_code, 8) ||
	right(cpr.location_code, 2))
	ELSE
		cpr.location_code
	END AS vessel_code,
	(uc. "vessel code" IS NOT NULL
		AND uc. "stage" = 'Active'
		AND uc. "active date"::date <= CURRENT_DATE - 1) AS is_ulysses,
	coalesce(
		CASE WHEN split_part(upper(cpr.location_code), '-', 1)
		in('NYC')
			OR regexp_replace(upper(cpr.location_code), '.+-(..)..$', '$1')
			in('MF', 'PL') THEN
			'Ignore'
		END, nullif(dpam.pl_mapping_1, ''), CASE WHEN split_part(cpr.location_code, '-', 2)
		in('000', '900') THEN
			'L3 Expense'
		WHEN split_part(cpr.location_code, '-', 1)
		in('HO', 'CC', 'CPU')
			OR split_part(cpr.location_code, '-', 3) = 'FC' THEN
			'L4 Expense'
		WHEN
	right(cpr.location_code, 2) = '00' THEN
			'L2 Expense'
		ELSE
			'L1 Expense'
		END) AS expense_level,
	cpr.*,
	dpam.gl_account IS NULL AS missing_mapping,
	dpam.*,
	round(cpr.pnl_contribution_daily_usd, 2) AS pnl_contribution_daily_usd_rounded
FROM
	happy_orders.coupa_po_receipts cpr
	LEFT JOIN scratch.daily_pnl_account_mapping dpam ON cpr.order_gl_account = dpam.gl_account::varchar
	LEFT JOIN monday.ulysses_crm uc ON
	left(cpr.location_code, 8) ||
	right(cpr.location_code, 2) = uc. "vessel code"
WHERE
	start_date <= '{start_date}'
	AND end_date >= '{start_date}';