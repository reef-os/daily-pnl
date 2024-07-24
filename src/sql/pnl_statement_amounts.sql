with conv as (select coalesce(from_currency_country_code, 'ES') from_country, to_currency, max(conversion_date) as last_date 
              from "conversion".currency_rates where to_currency = 'USD' group by 1, 2),
     rates as (select c.from_country, cr.to_currency, avg(cr.rate) as rate
               from "conversion".currency_rates cr join conv c on coalesce(cr.from_currency_country_code, 'ES') = c.from_country
                   and cr.to_currency = c.to_currency and cr.conversion_date = c.last_date group by 1, 2)
select uc."vessel name", uc."vessel code", uc.country, ss.item,
       case when country = 'US' then ss.us when country = 'CA' then ss.ca when country = 'GB' then ss.gb
            when country = 'ES' then ss.es when country = 'AE' then ss.ae end as monthly_standard_amount_local,
       coalesce(se.amount, monthly_standard_amount_local) * 12 / 365 as daily_amount_local,
       daily_amount_local * coalesce(r.rate, 1.0) as daily_amount_usd
from monday.ulysses_crm uc
    cross join monday.statement_standards ss
    left join monday.statement_exceptions se on uc."vessel code" = se."vessel id" and ss.item = se.item and functionality = 'Swap'
        and se."start date" <= '{start_date}' and se."end date" >= '{start_date}'
    left join rates r on uc.country = r.from_country
where ss.formulation = 'Amount*12/365*7' and uc.stage = 'Active' and uc."active date" <= '{start_date}'
order by 1, 3;