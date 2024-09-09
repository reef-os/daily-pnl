with bmt as (select integration_id, brand_name, mdm_brand_id from dna_bmt.storefronts group by 1, 2, 3),
     conv as (select coalesce(from_currency_country_code, 'ES') from_country, to_currency, max(conversion_date) as last_date 
              from "conversion".currency_rates where to_currency = 'USD' group by 1, 2),
     rates as (select c.from_country, cr.to_currency, avg(cr.rate) as rate
               from "conversion".currency_rates cr join conv c on coalesce(cr.from_currency_country_code, 'ES') = c.from_country
                   and cr.to_currency = c.to_currency and cr.conversion_date = c.last_date group by 1, 2),
     standards as (select 'US' as country, ss."us %%" / 100 as discount, ss2."us %%" / 100 as refund
			    from monday.statement_standards ss cross join monday.statement_standards ss2 where ss.item = 'Discounts' and ss2.item = 'Refunds'
				union
				select 'CA' as country, ss."ca %%" / 100 as discount, ss2."ca %%" / 100 as refund
				from monday.statement_standards ss cross join monday.statement_standards ss2 where ss.item = 'Discounts' and ss2.item = 'Refunds'
				union
				select 'GB' as country, ss."gb %%" / 100 as discount, ss2."gb %%" / 100 as refund
				from monday.statement_standards ss cross join monday.statement_standards ss2 where ss.item = 'Discounts' and ss2.item = 'Refunds'
				union
				select 'ES' as country, ss."es %%" / 100 as discount, ss2."es %%" / 100 as refund
				from monday.statement_standards ss cross join monday.statement_standards ss2 where ss.item = 'Discounts' and ss2.item = 'Refunds'
				union
				select 'AE' as country, ss."ae %%" / 100 as discount, ss2."ae %%" / 100 as refund
				from monday.statement_standards ss cross join monday.statement_standards ss2 where ss.item = 'Discounts' and ss2.item = 'Refunds')
select left(oo.location_name, 8) || right(oo.location_name, 2) as vessel, 
       uc."vessel name" as vessel_name,
       (uc."vessel code" is not null) as is_ulysses,
       oo.ol_order_id, 
       oo.dsp_order_id, 
       oo.order_timestamp_utc, 
       oo.order_timestamp_local, 
       oo.business_date_local, 
       oo.integration_id, 
       coalesce(oo.mdm_brand_id, b.mdm_brand_id) as mdm_brand_id, 
       oo.brand_name, 
       oo.location_name, 
       oo.delivery_platform, 
       oo.country, 
       oo.currency_local, 
       oo.cancelled, 
       oo.food_price_local, 
       oo.vat_rate, 
       coalesce(oo.tax_amount_local, 0) as tax_amount_local, 
       oo.subtotal_local, 
       oo.total_local,
       case when oo.delivery_platform in ('Careem', 'Deliveroo', 'PointOfSale', 'Reef UK', 'UberEats') then true 
            else false end as actual_discount,
       coalesce(case when actual_discount then od.discount_total else oo.food_price_local * st.discount end, 0) as discount_gross,
       case when oo.country in ('US', 'CA') then 0::float else discount_gross * oo.vat_rate end as vat_on_discount, 
       discount_gross - vat_on_discount as discount_subtotal_local, 
       coalesce(oo.food_price_local * st.refund, 0) as refund_gross,
       case when oo.country in ('US', 'CA') then 0::float else refund_gross * oo.vat_rate end as vat_on_refund,
       refund_gross - vat_on_refund as refund_subtotal_local,
       oo.subtotal_local - discount_subtotal_local - refund_subtotal_local as net_sales_local,
       coalesce((gcb.delivery_commission::float + gcb.subscription_commission::float) / 2, 
                (gcg.delivery_commission::float + gcg.subscription_commission::float) / 2) 
                + coalesce(gcb.transaction_fee::float, gcg.transaction_fee::float, 0.0) as gtm_est_commission_rate,
       case when coalesce(gcb.include_vat, gcg.include_vat) = 'YES' then gtm_est_commission_rate * (1+oo.vat_rate) 
            else gtm_est_commission_rate end as gtm_est_effective_commission_rate,
       coalesce(gcb.fixed_commission::float, gcg.fixed_commission::float, 0) + 
           coalesce(gcb.transaction_fee::float, gcg.transaction_fee::float, 0) as gtm_est_fixed_commission,
       gtm_est_fixed_commission + ((oo.subtotal_local - discount_subtotal_local - refund_subtotal_local) * 
                                    coalesce(gtm_est_effective_commission_rate, 0)) as est_commission_local,
       case when cc.legal_entity is null and cc2.legal_entity is null then true else false end as royalty_is_estimate,
       net_sales_local * coalesce(cc."royalty %%", cc2."royalty %%", 5) / 100 as royalty_amount_local,
       round(oo.food_price_usd, 2) as gross_sales_usd,
       round(discount_gross * coalesce(r.rate, 1.0), 2) as discount_usd,
       round(refund_gross * coalesce(r.rate, 1.0), 2) as refund_usd,
       round((tax_amount_local - vat_on_discount - vat_on_refund) * coalesce(r.rate, 1.0), 2) as vat_usd,
       round(net_sales_local * coalesce(r.rate, 1.0), 2) as net_sales_usd,
       round(est_commission_local * coalesce(r.rate, 1.0), 2) as commission_usd,
       round(royalty_amount_local * coalesce(r.rate, 1.0), 2) as royalty_usd,
       oo.load_date
from dna_kitchens_retail.orderlord_orders oo 
    left join monday.ulysses_crm uc on left(oo.location_name, 8) || right(oo.location_name, 2) = uc."vessel code" 
        and uc."stage" = 'Active' and oo.business_date_local >= uc."active date"::date
    left join dna_kitchens_retail_stg.ol_dedup od on oo.ol_order_id = od.id
    left join bmt b on oo.integration_id = b.integration_id
    left join dna_bmt.brand_mapping bm on od.brand_id = bm.ol_brand_id
    left join monday.content_contracts cc on coalesce(oo.mdm_brand_id, b.mdm_brand_id, bm.mdm_brand_id) = cc."brand id" 
                                              and (oo.country = cc.country or cc.country = 'GLOBAL')
    left join monday.content_contracts cc2 on (oo.country = cc2.country or cc2.country = 'GLOBAL') and
        regexp_replace(lower(b.brand_name), '[^a-z]', '') = regexp_replace(lower(cc2."content list"), '[^a-z]', '')
    left join monday.gtm_commission gcg on gcg.active = 'Yes' and (gcg.brand = 'General' or gcg.brand is null) and oo.country = gcg.country
        and (oo.delivery_platform ilike (gcg.gtm || '%') or (oo.delivery_platform = 'Flyt' and gcg.gtm in ('SkipTheDishes', 'JustEat')))
    left join monday.gtm_commission gcb on oo.country = gcb.country and gcb.active = 'Yes' and gcb.brand ilike oo.brand_name
        and (oo.delivery_platform ilike (gcb.gtm || '%') or (oo.delivery_platform = 'Flyt' and gcb.gtm in ('SkipTheDishes', 'JustEat')))
    left join rates r on oo.country = r.from_country
    left join standards st on oo.country = st.country 
where oo.business_date_local >= date '{start_date}' and oo.business_date_local <= date '{end_date}';

