{{ config(materialized='incremental'
, incremental_strategy='append')}}

with flatten_outputs as

(select

tx.hash_key,
tx.block_number,
tx.block_timestamp,
tx.is_coinbase,
f.value:address::VARCHAR as output_address,
f.value:value::FLOAT as output_value

from {{ref('stg_btc')}} tx,

LATERAL -- select what is coming from main table
FLATTEN(input=> outputs) f

WHERE f.value:address is not null

)

select
hash_key,
block_number,
block_timestamp,
is_coinbase,
output_address,
output_value
from flatten_outputs fo

{% if is_incremental() %}

HAVING fo.block_timestamp >= (select max(block_timestamp) from {{this}})

{% endif %}
