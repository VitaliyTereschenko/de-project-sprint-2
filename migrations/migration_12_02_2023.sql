DROP TABLE IF EXISTS public.shipping_country_rates CASCADE;

CREATE TABLE public.shipping_country_rates(
   shipping_country_id  		SERIAL,
   shipping_country   			TEXT,
   shipping_country_base_rate   NUMERIC(14,3),
   PRIMARY KEY  (shipping_country_id)
);


INSERT INTO public.shipping_country_rates
(shipping_country, shipping_country_base_rate)
SELECT  
	t.shipping_country,
	t.shipping_country_base_rate
FROM (
    SELECT DISTINCT shipping_country AS shipping_country,
    shipping_country_base_rate AS shipping_country_base_rate
    FROM public.shipping s) t;

--//////////////////////////////////////////////////////////////////////////////////////////////////////--

DROP TABLE IF EXISTS public.shipping_agreement CASCADE ;

CREATE TABLE public.shipping_agreement (
   agreementid  		BIGINT,
   agreement_number   	TEXT,
   agreement_rate   	NUMERIC(14,2),
   agreement_commission	NUMERIC(14,2),
   PRIMARY KEY  (agreementid)
);


INSERT INTO public.shipping_agreement
(agreementid, agreement_number, agreement_rate, agreement_commission)
SELECT   
	CAST(t2.vendor_agreement_description[1] AS INT) AS agreementid,
	t2.vendor_agreement_description[2] AS agreement_number,
	CAST(t2.vendor_agreement_description[3] AS NUMERIC(14,2)) AS agreement_rate,
	CAST(t2.vendor_agreement_description[4] AS NUMERIC(14,2)) AS agreement_commission
FROM(SELECT DISTINCT
          	(regexp_split_to_array(vendor_agreement_description, E'\\:+')) AS vendor_agreement_description
      FROM shipping) t2;

--//////////////////////////////////////////////////////////////////////////////////////////////////////--

DROP TABLE IF EXISTS public.shipping_transfer CASCADE ;

CREATE TABLE public.shipping_transfer (
	transfer_type_id		SERIAL,
   	transfer_type  			TEXT,
   	transfer_model   		TEXT,
   	shipping_transfer_rate  NUMERIC(14,3),
   	PRIMARY KEY  (transfer_type_id)
);


INSERT INTO public.shipping_transfer 
(transfer_type, transfer_model, shipping_transfer_rate)
SELECT   
	t3.shipping_transfer_description[1] AS transfer_type,
	t3.shipping_transfer_description[2] AS transfer_model,
	shipping_transfer_rate
	FROM (SELECT DISTINCT
          	(regexp_split_to_array(shipping_transfer_description, E'\\:+')) AS shipping_transfer_description,
          	shipping_transfer_rate AS shipping_transfer_rate
          FROM shipping) t3;
           
--//////////////////////////////////////////////////////////////////////////////////////////////////////--
         
DROP TABLE IF EXISTS public.shipping_info;

CREATE TABLE public.shipping_info(
   shippingid				BIGINT,
   vendorid      			BIGINT,
   payment_amount   		NUMERIC(14, 2),
   shipping_plan_datetime	TIMESTAMP,
   transfer_type_id 		BIGINT,
   shipping_country_id		BIGINT,
   agreementid				BIGINT,
   PRIMARY KEY (shippingid),
   FOREIGN KEY (transfer_type_id) REFERENCES shipping_transfer(transfer_type_id),
   FOREIGN KEY (shipping_country_id) REFERENCES shipping_country_rates(shipping_country_id),
   FOREIGN KEY (agreementid) REFERENCES shipping_agreement(agreementid)
);

INSERT INTO public.shipping_info
(shippingid, vendorid, payment_amount, shipping_plan_datetime, transfer_type_id, shipping_country_id, agreementid)	
SELECT DISTINCT shippingid,
				vendorid,
				payment_amount,
				shipping_plan_datetime,
				st.transfer_type_id AS transfer_type_id,
				scr.shipping_country_id AS shipping_country_id,
				sa.agreementid AS agreementid				
FROM public.shipping s
LEFT JOIN public.shipping_country_rates scr ON
scr.shipping_country = s.shipping_country
LEFT JOIN public.shipping_agreement sa ON(
sa.agreement_number  = (regexp_split_to_array(s.vendor_agreement_description, E'\\:+'))[2]
AND
sa.agreementid = CAST((regexp_split_to_array(s.vendor_agreement_description, E'\\:+'))[1] AS BIGINT)
)
LEFT JOIN public.shipping_transfer st ON (
(regexp_split_to_array(s.shipping_transfer_description, E'\\:+'))[1] = st.transfer_type 
AND
(regexp_split_to_array(s.shipping_transfer_description, E'\\:+'))[2] = st.transfer_model
);

--//////////////////////////////////////////////////////////////////////////////////////////////////////--

DROP TABLE IF EXISTS public.shipping_status;
CREATE TABLE public.shipping_status(
   shippingid 						BIGINT,
   status      						TEXT,
   state   							TEXT,
   shipping_start_fact_datetime 	TIMESTAMP,
   shipping_end_fact_datetime 		TIMESTAMP,
   PRIMARY KEY (shippingid)
);

INSERT INTO public.shipping_status
(shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
WITH max_t AS(
SELECT
		shippingid, max(state_datetime) AS state_datetime
FROM public.shipping
WHERE state = 'recieved'
GROUP BY shippingid
),
min_t AS(
SELECT
		shippingid, min(state_datetime)
FROM public.shipping
GROUP BY shippingid
),
main AS(
SELECT * FROM (
	SELECT 	*,
			ROW_NUMBER() OVER (PARTITION BY shippingid ORDER BY state_datetime DESC) row_num
   	FROM public.shipping) t 
WHERE row_num = 1
)


SELECT 	s.shippingid AS shippingid,
		s.status AS status,
		s.state AS state,
		min_t.min AS shipping_start_fact_datetime,
		max_t.state_datetime AS shipping_end_fact_datetime
FROM main s
LEFT JOIN max_t ON max_t.shippingid = s.shippingid
LEFT JOIN min_t ON min_t.shippingid = s.shippingid
;

--//////////////////////////////////////////////////////////////////////////////////////////////////////--

CREATE OR REPLACE VIEW public.shipping_datamart AS
SELECT
	ss.shippingid AS shippingid,
	si.vendorid AS vendorid, 
	st.transfer_type AS transfer_type,
	DATE_PART('day', ss.shipping_end_fact_datetime - ss.shipping_start_fact_datetime) AS full_day_at_shipping,
	CASE 
		WHEN (ss.shipping_end_fact_datetime > si.shipping_plan_datetime) THEN 1
		ELSE 0
	END AS is_delay,
	CASE 
		WHEN ss.status = 'finished' THEN 1
		ELSE 0
	END AS is_shipping_finish,
	CASE 
		WHEN (ss.shipping_end_fact_datetime > si.shipping_plan_datetime) 
			THEN DATE_PART('day', ss.shipping_end_fact_datetime - si.shipping_plan_datetime)
		ELSE 0
	END AS delay_day_at_shipping,
	si.payment_amount AS payment_amount,
	si.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) AS vat,
	si.payment_amount * sa.agreement_commission	AS profit
FROM shipping_status ss
LEFT JOIN shipping_info si ON
	si.shippingid = ss.shippingid
LEFT JOIN shipping_transfer st ON
	st.transfer_type_id = si.transfer_type_id
LEFT JOIN shipping_country_rates scr ON
	scr.shipping_country_id = si.shipping_country_id
LEFT JOIN shipping_agreement sa ON
	sa.agreementid = si.agreementid