{{
    config(
        enabled=True
    )
}}



WITH LOAD_DATES AS (
        SELECT customer_pk, date_trunc ('minute',LOAD_DATE) s_LOAD_DATE FROM dbt.sat_customer_details
        union 
        SELECT customer_pk, date_trunc ('minute',LOAD_DATE) s_LOAD_DATE FROM dbt.sat_customer_crm_details
), 

Pit_customers as (
                    SELECT
                    LD.customer_pk,
                    LD.s_LOAD_DATE,
                    MAX(date_trunc ('minute',S1.LOAD_DATE)) DET_LOAD_DATE,
                    MAX(date_trunc ('minute',S2.LOAD_DATE)) CRM_LOAD_DATE

                    FROM LOAD_DATES LD
                    LEFT JOIN dbt.sat_customer_details S1
                    ON (S1.customer_pk = LD.customer_pk AND cast (date_trunc ('day',S1.LOAD_DATE)as date) <= LD.s_LOAD_DATE)

                    LEFT JOIN dbt.sat_customer_crm_details S2
                    ON (S2.customer_pk = LD.customer_pk AND cast (date_trunc ('day',S2.LOAD_DATE)as date) <= LD.s_LOAD_DATE)

                    ---where LD.customer_pk = 'ec8956637a99787bd197eacd77acce5e'
                    GROUP BY LD.customer_pk, LD.s_LOAD_DATE
),

final as (
select 
    *
from Pit_customers 

)

select * from final