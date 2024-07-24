select uc."vessel name", uc.country, case when cpr.location_code like '___-___-____' or cpr.location_code like '___-___-__'
            then upper(left(cpr.location_code, 8) || right(cpr.location_code, 2)) else cpr.location_code end as vessel_code, 
       (uc."vessel code" is not null and uc."stage" = 'Active' and uc."active date"::date <= current_date - 1) as is_ulysses,
       coalesce(case when split_part(upper(cpr.location_code), '-', 1) in ('NYC')
                         or regexp_replace(upper(cpr.location_code), '.+-(..)..$', '$1') in ('MF', 'PL') then 'Ignore' end,
                nullif(dpam.pl_mapping_1, ''),
                case when split_part(cpr.location_code, '-', 2) in ('000', '900') then 'L3 Expense'
                     when split_part(cpr.location_code, '-', 1) in ('HO', 'CC', 'CPU') or split_part(cpr.location_code, '-', 3) = 'FC' then 'L4 Expense'
                     when right(cpr.location_code, 2) = '00' then 'L2 Expense'
                     else 'L1 Expense' end) as expense_level,
       cpr.*, dpam.gl_account is null as missing_mapping, dpam.*,
       round(cpr.pnl_contribution_daily_usd, 2) as pnl_contribution_daily_usd_rounded
from happy_orders.coupa_po_receipts cpr
    left join scratch.daily_pnl_account_mapping dpam on cpr.order_gl_account = dpam.gl_account::varchar
    left join monday.ulysses_crm uc on left(cpr.location_code, 8) || right(cpr.location_code, 2) = uc."vessel code"
where start_date <= '{start_date}' and end_date >= '{start_date}';