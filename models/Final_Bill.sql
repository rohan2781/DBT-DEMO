{{ config(
    materialized='table',
    alias='final_bill_data'
) }}

select * from
{{ ref ('Bill_data') }}

UNION DISTINCT

select * from
{{ ref ('Receipt_Data') }}