-- Создаем временную таблицу, если она еще не существует

query = 
CREATE TEMP TABLE IF NOT EXISTS temp_finishings AS
SELECT
    nb.building_uuid,
    unnest(string_to_array(tc.finishing, ', ')) AS finishing, --отделка
    unnest(string_to_array(tc.payment, ', ')) AS payment, --тип оплаты
    unnest(string_to_array(tc.contract, ', ')) AS contract -- тип договора
FROM
    norm_data.norm_buildings nb
JOIN
    trend_raw_data.blocks_links tc ON nb.building_uuid = tc.cat_blocks_uuid;

    
    
-- Обновляем поле finishings, contract, payment  в основной таблице

query = 
UPDATE norm_data.norm_buildings nb
set
    queue = tc.queue::integer, --очередь
    escrow = case --эскроу
    when  tc.escrow = 'True' then true 
    when tc.escrow = 'False' then false 
    else null
end,
    sales_start_at = case -- старт продаж
    when tc.sales_start_at = '' then null
    else  tc.sales_start_at::date
end,
    address = tc.address, --адрес
    building_type = tc.building_type,
    facade_type = tc.facade_type, -- фасад
    finishing = ARRAY(
        SELECT f.name
        FROM trend_raw_data.flat_finishing_types f
        WHERE f.name_trend = ANY(string_to_array(tc.finishing, ', ')) --отделка
    ),
    contract = ARRAY(
        SELECT ct.name
        FROM trend_raw_data.contract_deal_type ct 
        WHERE ct.name_trend = ANY(string_to_array(tc.contract, ', ')) --тип договора
    ),
    payment = ARRAY(
        SELECT pt.name
        FROM trend_raw_data.payment_types pt 
        WHERE pt.name_trend = ANY(string_to_array(tc.payment, ', ')) --тип оплаты
    ),
    installment = 'Рассрочка' = ANY(string_to_array(tc.payment, ', ')),
    mortgage = 'Ипотека' = ANY(string_to_array(tc.payment, ', ')),
    mortgage_voen = 'Военная ипотека' = ANY(string_to_array(tc.payment, ', ')),
    subsidy = 'Субсидия' = ANY(string_to_array(tc.payment, ', '))
FROM
    temp_finishings tf
JOIN
    trend_raw_data.blocks_links tc ON building_uuid = tc.cat_blocks_uuid
WHERE
    nb.building_uuid = tf.building_uuid;
   

-- складываем uuid значений в массив JSON используя справочники схемы demo_dict

query = 
UPDATE norm_data.norm_buildings nb
SET 
   payment_type_uuid = 
    (SELECT json_agg(ptt.uuid::uuid)  
     FROM demo_dict.payment_types ptt, unnest(nb.payment::text[]) as item
     WHERE item = ptt.name),     
   contract_deal_type_uuid = 
    (SELECT jsonb_agg(ct.uuid::uuid) 
     FROM demo_dict.contract_deal_type ct, unnest(nb.contract::text[]) as item
     WHERE item = ct.name);

-- добавляем uuid по одиночным признакам, используя справочники схемы demo_dict
query =   
UPDATE norm_data.norm_buildings AS nb
set
    facade_type_uuid = (
        SELECT ft.uuid 
        FROM demo_dict.facade_types ft 
        WHERE nb.facade_type = ft.name),
    building_types_uuid = (
        SELECT pd.uuid
        FROM demo_dict.building_types AS pd
        WHERE pd.name = nb.building_type
    ),
    subsidy_uuid = (
        SELECT pd.uuid
        FROM demo_dict.payment_types AS pd
        WHERE pd.name = 'Субсидии' AND nb.subsidy = true
    ),
    installment_uuid = (
        SELECT pd.uuid
        FROM demo_dict.payment_types AS pd
        WHERE pd.name = 'Рассрочка' AND nb.installment = true
    ),
    mortgage_uuid = (
        SELECT pd.uuid
        FROM demo_dict.payment_types AS pd
        WHERE pd.name = 'Ипотека' AND nb.mortgage = true
    ),
    mortgage_military_uuid = (
        SELECT pd.uuid
        FROM demo_dict.payment_types AS pd
        WHERE pd.name = 'Военная ипотека' AND  nb.mortgage_voen = true
    );

-- заполняем дополнительное поле по оплате, '100% оплата'

query = 
UPDATE norm_data.norm_buildings AS nb
SET cash_full_uuid = 
    (SELECT pd.uuid
        FROM demo_dict.payment_types AS pd
        WHERE pd.name = '100% оплата');