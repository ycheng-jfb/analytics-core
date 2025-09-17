USE [analytic]
GO

/****** Object:  StoredProcedure [dbo].[usp_store_force]    Script Date: 12/13/2023 9:41:49 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[usp_store_force]

AS
BEGIN

	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;

	DECLARE @realtime_start_date   DATE
    SET @realtime_start_date  =  CONVERT(DATE,GETDATE())
	--SELECT @realtime_start_date
    -------------------------
    -- create a base table --
    -------------------------
    IF OBJECT_ID('tempdb..#orders_base') IS NOT NULL
        DROP TABLE #orders_base

    CREATE TABLE #orders_base (order_id BIGINT,customer_id BIGINT)
    INSERT INTO #orders_base
    -- new rows
    SELECT DISTINCT
    ORDER_ID,
    CUSTOMER_ID
    FROM (
    SELECT
        o.ORDER_ID,
        o.CUSTOMER_ID
    FROM ultramerchant.dbo.[ORDER] AS o with(nolock)
    LEFT JOIN ultramerchant.dbo.order_classification AS oc WITH(NOLOCK)
        ON o.order_id = oc.order_id AND oc.order_type_id IN (48, 49) -- Amazon Today orders
    WHERE o.datetime_added >= @realtime_start_date
        AND oc.order_id IS NULL -- exclude Amazon Today orders
    UNION ALL
    SELECT
        R.ORDER_ID,
        O.CUSTOMER_ID
    FROM ultramerchant.dbo.REFUND AS r with(nolock)
    JOIN ultramerchant.dbo."ORDER" AS o with(nolock)
        ON o.ORDER_ID = r.ORDER_ID
    JOIN ultramerchant.dbo.STATUSCODE AS sc with(nolock)
        ON sc.STATUSCODE = r.STATUSCODE
    LEFT JOIN ultramerchant.dbo.order_classification AS oc WITH(NOLOCK)
        ON o.order_id = oc.order_id AND oc.order_type_id IN (48, 49) -- Amazon Today orders
    WHERE R.DATETIME_TRANSACTION >= @realtime_start_date
        AND oc.order_id IS NULL -- exclude Amazon Today orders
    ) AS A

    OPTION (RECOMPILE)
    CREATE CLUSTERED INDEX idx_order_id on #orders_base (order_id)

    ---------------------------------------------------
    -- capture new customers from membership and order base --
    -----------------------------------------------------
    IF OBJECT_ID('tempdb..#customers_to_process') IS NOT NULL
        DROP TABLE #customers_to_process

    CREATE  TABLE #customers_to_process(customer_id BIGINT)
    INSERT INTO #customers_to_process
    SELECT DISTINCT CUSTOMER_ID
    FROM (
    SELECT customer_id
    FROM ultramerchant.dbo.MEMBERSHIP m with(nolock)
    WHERE DATETIME_ADDED >= @realtime_start_date
        OR DATETIME_ACTIVATED >= @realtime_start_date
    UNION ALL
    SELECT customer_id
    FROM #orders_base with(nolock)
    ) AS A
        CREATE CLUSTERED INDEX idx_customer_id on #customers_to_process (customer_id)
    ---------------------------------------------------
    -- capture employeee info from workday --
    -----------------------------------------------------

   IF OBJECT_ID('tempdb..#workday_employee') IS NOT NULL
       DROP TABLE #workday_employee

   CREATE TABLE #workday_employee
   (
   associate_id BIGINT,
   workday_id BIGINT,
   start_datetime DATETIME,
   end_datetime DATETIME
   )

CREATE NONCLUSTERED INDEX [ix_workday_employee_1]
ON #workday_employee ([associate_id],[start_datetime],[end_datetime]) include ([workday_id])

   INSERT INTO #workday_employee
   SELECT
   associate_id,
   TRY_CONVERT(INT,workday_id) AS workday_id,
   start_date AS start_datetime,
   COALESCE(LEAD(start_date) OVER (PARTITION BY associate_id ORDER BY start_date), CURRENT_TIMESTAMP) AS end_datetime
	FROM (
		SELECT DISTINCT
		  a.administrator_id AS associate_id,
		  COALESCE(e.employee_id, e2.employee_id, t2.workday_id) AS workday_id,
		  COALESCE(a.DATE_ADDED, e.START_DATE, t2.start_datetime) AS START_DATE
		FROM analytic.dbo.evolve_ultraidentity_administrator a WITH(NOLOCK)
		LEFT JOIN analytic.dbo.employees e WITH(NOLOCK)
		    ON a.login = e.employee_id
		    AND e.company in ('170 JF Retail Services','190 Savage X Fenty','192 Savage X Retail')
		LEFT JOIN analytic.dbo.employees e2 WITH(NOLOCK)
		    ON a.login != e2.employee_id
			AND lower(TRIM(a.firstname)) = lower(TRIM(e2.FIRST_NAME))
			AND lower(TRIM(a.lastname))= lower(TRIM(e2.last_NAME))
			AND e2.company in ('170 JF Retail Services','190 Savage X Fenty','192 Savage X Retail')
			AND ((a.login!='NRamos') or (a.login='NRamos' and year(e2.start_date) = '2021'))
		LEFT JOIN (
			SELECT DISTINCT
			  CAST(REPLACE(REPLACE(up_for_euid.value, '"','') ,'null','-2')AS INT) AS associate_id,
			 COALESCE(REPLACE(up_for_wdid.value, '"',''),emp.employee_id) AS workday_id,
				up_for_wdid.datetime_added AS start_datetime
			FROM analytic.dbo.evolve_ultraidentity_user_property AS up_for_euid WITH(NOLOCK)
			LEFT JOIN analytic.dbo.evolve_ultraidentity_user_property AS up_for_wdid WITH(NOLOCK)
				ON up_for_euid.user_id = up_for_wdid.user_id
				AND up_for_wdid.[key] = 'workdayEmployeeId'
			JOIN analytic.dbo.evolve_ultraidentity_user AS u WITH(NOLOCK)
				ON u.user_id = up_for_euid.user_id
			LEFT JOIN analytic.dbo.EMPLOYEES emp WITH(NOLOCK)
				ON TRIM(emp.first_name)=TRIM(u.firstname)
				AND TRIM(emp.last_name)=TRIM(u.lastname)
				AND emp.company in ('170 JF Retail Services','190 Savage X Fenty','192 Savage X Retail')
			WHERE up_for_euid.[key] = 'ecomUserId'
			) t2
			ON t2.associate_id = a.administrator_id
	) abc
    ---------------------------------------------------
    -- pull retail store --
    -----------------------------------------------------
    IF OBJECT_ID('tempdb..#stores') IS NOT NULL
		DROP TABLE #stores

	SELECT DISTINCT
        Store_ID INTO #stores
    FROM analytic.dbo.store with(nolock)
    WHERE store_type = 'Retail'
       -- AND store_sub_type = 'Store'

	IF OBJECT_ID('tempdb..#order_tracking_id_null') IS NOT NULL
		DROP TABLE #order_tracking_id_null

    SELECT DISTINCT
        ORDER_ID
    INTO #order_tracking_id_null
    FROM (
        select o.order_id
        from ultramerchant.dbo."ORDER" o with(nolock)
        LEFT join ultramerchant.dbo.[RETURN] r with(nolock)
            on r.order_id = o.order_id
        left join (
            SELECT DISTINCT
                ORDER_ID,
                VALUE
            FROM ultramerchant.dbo.ORDER_DETAIL with(nolock)
            WHERE NAME = 'retail_store_id'
            ) AS od
            on od.ORDER_ID = o.ORDER_ID
        WHERE COALESCE(od.VALUE, o.STORE_ID) IN (SELECT DISTINCT STORE_ID FROM #stores with(nolock))
          and O.ORDER_TRACKING_ID is null
          and (r.datetime_transaction >= @realtime_start_date
            OR o.datetime_modified >= @realtime_start_date)
        ) AS A

    IF OBJECT_ID('tempdb..#missing_administrator_ids') IS NOT NULL
		DROP TABLE #missing_administrator_ids

    select DISTINCT cw.administrator_id, o.order_id
    INTO #missing_administrator_ids
    from ultramerchant.dbo."ORDER" o with(nolock)
    join #order_tracking_id_null otn with(nolock)
        on o.ORDER_ID = otn.order_id
    JOIN  ultramerchant.dbo.CART c with(nolock)
        on o.SESSION_ID = c.SESSION_ID
    JOIN  ultramerchant.dbo.CART_WAREHOUSE cw with(nolock)
        on c.CART_ID = cw.cart_id

	IF OBJECT_ID('tempdb..#store_timezone') IS NOT NULL
		DROP TABLE #store_timezone

	select coalesce(slt.sf_timezone,
					CASE WHEN STORE_TIME_ZONE IN ('America/Los_Angeles', 'America/Barbados', 'America/Vancouver') THEN 'Pacific Standard Time'
						 WHEN STORE_TIME_ZONE IN ('Europe/Amsterdam', 'Europe/Berlin', 'Europe/Copenhagen', 'Europe/Paris', 'Europe/Rome', 'Europe/Stockholm', 'Europe/Tallinn', 'Indian/Cocos') THEN 'Central European Standard Time'
						 WHEN STORE_TIME_ZONE IN ('Europe/London') THEN 'GMT Standard Time'
						 WHEN STORE_TIME_ZONE IN ('Europe/Madrid') THEN 'W. Europe Standard Time'
						 ELSE STORE_TIME_ZONE END
					) as sf_timezone,ds.*
	INTO #store_timezone
    FROM analytic.dbo.STORE ds with(nolock)
    left join (
		select
		distinct st.STORE_ID, rc.CONFIGURATION_VALUE,_sf_tz.sf_timezone
		from ultramerchant.dbo.store st with(nolock)
		join analytic.dbo.evolve_ultrawarehouse_RETAIL_LOCATION rl with(nolock) on st.STORE_ID = rl.STORE_ID
		join analytic.dbo.evolve_ultrawarehouse_RETAIL_LOCATION_CONFIGURATION rc with(nolock) on rl.RETAIL_LOCATION_ID = rc.RETAIL_LOCATION_ID
		join (SELECT 'HST' as timezone,'Hawaiian Standard Time' as sf_timezone union
				SELECT 'GMT' as timezone,'GMT Standard Time' as sf_timezone union
				SELECT 'EST' as timezone,'Eastern Standard Time' as sf_timezone union
				SELECT 'PST' as timezone,'Pacific Standard Time' as sf_timezone union
				SELECT 'CST' as timezone,'Central Standard Time' as sf_timezone union
				SELECT 'MST' as timezone,'Mountain Standard Time' as sf_timezone  union
				SELECT 'CET' AS timezone, 'Central European Standard Time' as sf_timezone
				) _sf_tz ON _sf_tz.timezone = rc.CONFIGURATION_VALUE
		where RETAIL_CONFIGURATION_ID = 15
	) slt on slt.store_id = ds.store_id

	DELETE FROM #store_timezone with(updlock) WHERE NOT (store_brand IN ('Fabletics','Savage X') )

	--SELECT * FROM ultramerchant.dbo.store st
	--   where st.label like 'RTL%'
	--      and ALIAS = 'Fabletics'
	--WHERE sf_timezone='Europe/London'
	--SELECT* FROM  sys.time_zone_info
	----------------------------------------------
	IF OBJECT_ID('tempdb..#customer_classification_from_source') IS NOT NULL
		DROP TABLE #customer_classification_from_source

	SELECT
		COALESCE(m.STORE_ID,cust.STORE_ID) AS CUSTOMER_STORE_ID,
		o.ORDER_ID AS ACTIVATING_ORDER_ID,
		COALESCE(workday_id,-1) AS EMPLOYEE_NUMBER,
		COALESCE(TRY_CONVERT(INT,od.VALUE), m.STORE_ID) AS VIP_STORE_ID,
		m.datetime_added AS datetime_pst_registered,
		CAST( m.DATETIME_ADDED AT TIME ZONE 'Pacific Standard Time' AT TIME ZONE st.sf_timezone AS DATETIME) AS datetime_local_registered,
		CAST( m.DATETIME_ACTIVATED AT TIME ZONE 'Pacific Standard Time' AT TIME ZONE st.sf_timezone AS DATETIME) AS datetime_local_activated,
		cust.CUSTOMER_ID,
		CASE
			WHEN st.STORE_BRAND IN ('Fabletics','FabKids') AND fl.customer_id IS NOT NULL THEN 'M'
			WHEN st.store_brand IN ('Fabletics','FabKids') THEN 'F'
			ELSE 'Unknown' END AS CUSTOMER_GENDER,
		CASE
			WHEN st.STORE_BRAND = 'FabKids' AND fk.CUSTOMER_ID IS NOT NULL THEN 'TRUE'
			ELSE 'FALSE' END AS FK_CROSS_PROMO,
		CASE
			WHEN m.CUSTOMER_ID = 503623531 THEN 'BF'
			WHEN cc.country_code = 'AU' AND m.store_id = 52 THEN 'AU'
			WHEN cc.country_code = 'CA' AND m.store_id IN (46, 55) THEN 'CA'
			WHEN cc.country_code = 'AT' AND m.store_id IN (36, 65) THEN 'AT'
			WHEN cc.country_code = 'BE' AND m.store_id IN (48, 59) THEN 'BE'
			ELSE 'None' END
		AS FINANCE_SUB_STORE,
		CASE
			WHEN cust.email NOT LIKE '%@test%'
			AND cust.email NOT LIKE '%@example%'
			AND cust.email NOT LIKE '%@fkqa%'
			THEN 'FALSE'
		ELSE 'TRUE' END AS is_test_account
	INTO #customer_classification_from_source
	 FROM (
		SELECT
			C.CUSTOMER_ID,
			LOWER(C.EMAIL) AS EMAIL,
			C.STORE_ID
		FROM  ultramerchant.dbo.CUSTOMER c WITH(NOLOCK)
		WHERE C.CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM #customers_to_process)
		) AS CUST
	LEFT JOIN ULTRAMERCHANT.dbo.MEMBERSHIP AS M WITH(NOLOCK)
		ON M.CUSTOMER_ID = CUST.customer_id
	LEFT JOIN #store_timezone st WITH(NOLOCK)
		ON st.STORE_ID = m.STORE_ID
	LEFT JOIN (
		SELECT o.*,otd.object_id
		FROM ultramerchant.dbo."ORDER" AS O WITH(NOLOCK)
			LEFT JOIN ultramerchant.dbo.ORDER_TRACKING_DETAIL AS otd WITH(NOLOCK)
				ON o.order_tracking_id = otd.order_tracking_id
		WHERE O.processing_statuscode NOT IN (2200, 2202, 2205)
			AND OTD.object = 'administrator'
		) AS o
		ON o.ORDER_ID = m.ORDER_ID
		AND m.DATETIME_ACTIVATED IS NOT NULL
	LEFT JOIN #missing_administrator_ids ma WITH(NOLOCK)
		ON o.order_id = ma.order_id
	LEFT JOIN #workday_employee w WITH(NOLOCK)
		ON COALESCE(o.OBJECT_ID, ma.administrator_id ) = w.associate_id
		AND m.datetime_activated >= w.start_datetime
		AND m.datetime_activated < w.end_datetime
	LEFT JOIN (
		SELECT *
		FROM ultramerchant.dbo.ORDER_DETAIL WITH(NOLOCK)
		WHERE NAME = 'retail_store_id'
		) od
		ON od.ORDER_ID = o.ORDER_ID
	LEFT JOIN (
		SELECT DISTINCT customer_id
		FROM ultramerchant.dbo.CUSTOMER_DETAIL WITH(NOLOCK)
		WHERE name = 'origin'
			AND VALUE LIKE '%free%'
			AND CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM #customers_to_process)
		) AS fk
		ON fk.CUSTOMER_ID = m.CUSTOMER_ID
	LEFT JOIN (
		SELECT DISTINCT customer_id
		FROM ultramerchant.dbo.CUSTOMER_DETAIL WITH(NOLOCK)
		WHERE name = 'gender'
			AND VALUE IN ('M','m')
			AND CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM #customers_to_process)
		) AS fl
		ON fl.CUSTOMER_ID = m.CUSTOMER_ID
	LEFT JOIN (
		SELECT
			customer_id,
			MAX(VALUE) AS country_code
		FROM ULTRAMERCHANT.dbo.CUSTOMER_DETAIL WITH(NOLOCK)
		WHERE name = 'country_code'
			AND VALUE IN ('AT','AU','BE','CA')
			AND CUSTOMER_ID IN (SELECT CUSTOMER_ID FROM #customers_to_process)
		GROUP BY customer_id
		) AS cc
		ON cc.CUSTOMER_ID = m.CUSTOMER_ID

	IF OBJECT_ID('tempdb..#order_types') IS NOT NULL
		DROP TABLE #order_types

	SELECT DISTINCT * into #order_types
	FROM (
		SELECT
			oc.order_id,
			SUM(CASE WHEN ORDER_TYPE_ID = 23 THEN 1 ELSE 0 END) as activating,
			SUM(CASE WHEN ORDER_TYPE_ID =34 THEN 1 ELSE 0 END) as subscription,
			SUM(CASE WHEN ORDER_TYPE_ID = 33 THEN 1 ELSE 0 END) as first_Guest,
			SUM(CASE WHEN ORDER_TYPE_ID = 13 THEN 1 ELSE 0 END) as non_subscription,
			SUM(CASE WHEN ORDER_TYPE_ID = 32 THEN 1 ELSE 0 END) as borderfree,
			SUM(CASE WHEN ORDER_TYPE_ID IN (10,39)THEN 1 ELSE 0 END) as billing
		FROM ultramerchant.dbo.ORDER_CLASSIFICATION oc with(nolock)
		JOIN #orders_base otp with(nolock)
			ON oc.ORDER_ID = otp.order_id
		WHERE order_type_id IN (10, 13, 23, 32, 33, 34, 39)
			AND DATETIME_ADDED >= '2021-01-01'
		GROUP BY oc.order_id
		UNION ALL
		-- pretty sure to get to complete nonactivating there are orders not in order_classification to be considered
		SELECT
			otp.ORDER_ID,
			0 as activating,
			0 as subscription,
			0 as first_guest,
			1 as non_subscription,
			0 as borderfree,
			0 as billing
		FROM #orders_base otp with(nolock)
		LEFT JOIN ultramerchant.dbo.ORDER_CLASSIFICATION oc with(nolock)
			on oc.ORDER_ID = otp.order_id
			and oc.order_type_id IN (10, 13, 23, 32, 33, 34, 39)
		WHERE oc.ORDER_ID is null
		) AS A

	IF OBJECT_ID('tempdb..#orders_base_final') IS NOT NULL
		DROP TABLE #orders_base_final

	SELECT
		CASE WHEN DATEADD(SECOND,3600,CAST(o.DATETIME_ADDED  AT TIME ZONE 'Pacific Standard Time' AT TIME ZONE st.sf_timezone AS DATETIME)) > cs.DATETIME_LOCAL_ACTIVATED
		 THEN cs.vip_store_id
		ELSE -1 END AS VIP_STORE_ID,
			COALESCE(TRY_CONVERT(INT,od.VALUE), o.STORE_ID) AS EVENT_STORE_ID,
		CUSTOMER_GENDER,
		FK_CROSS_PROMO,
		FINANCE_SUB_STORE,
		CASE WHEN billing > 0 AND o.payment_statuscode IN (2651,2650, 2600) THEN 'Successful Billing'
			WHEN billing > 0 THEN 'Pending Billing'
			WHEN first_guest > 0 THEN 'First Guest'
			WHEN activating > 0 AND first_guest = 0 THEN 'Activating VIP'
			WHEN SUBSCRIPTION > 0 THEN 'Repeat VIP'
			ELSE 'Repeat Guest' END
		AS  order_type,
		o.ORDER_ID,
		COALESCE(workday_id,-1) AS EMPLOYEE_NUMBER,
		o.SHIPPING_ADDRESS_ID,
	   CASE WHEN DATEPART(MINUTE,CAST(o.DATETIME_ADDED  AT TIME ZONE 'Pacific Standard Time' AT TIME ZONE st.sf_timezone AS DATETIME)) < 30
				THEN DATEADD(HOUR, DATEDIFF(HOUR, 0, CAST(o.DATETIME_ADDED  AT TIME ZONE 'Pacific Standard Time' AT TIME ZONE st.sf_timezone AS DATETIME)), 0)
			ELSE DATEADD(MINUTE,30,DATEADD(HOUR, DATEDIFF(HOUR, 0, CAST(o.DATETIME_ADDED  AT TIME ZONE 'Pacific Standard Time' AT TIME ZONE st.sf_timezone AS DATETIME)), 0))
			END AS SLOT,
		COALESCE(o.SUBTOTAL, 0)  AS subtotal_gross_vat_local,
		COALESCE(o.DISCOUNT, 0) AS discount_gross_vat_local,
		COALESCE(o.SHIPPING, 0) AS shipping_rev_gross_vat_local,
		(subtotal - discount + shipping) AS revenue_gross_vat_local,
		COALESCE(units.units, 0)  AS units,
		CAST(o.DATETIME_ADDED  AT TIME ZONE 'Pacific Standard Time' AT TIME ZONE st.sf_timezone AS DATETIME)  AS max_refresh_time
		INTO #orders_base_final
	FROM ultramerchant.dbo."ORDER" o WITH(NOLOCK)
		JOIN #customer_classification_from_source cs WITH(NOLOCK)
			ON cs.CUSTOMER_ID = o.CUSTOMER_ID
		JOIN #order_types ot WITH(NOLOCK)
			ON ot.order_id = o.ORDER_ID
		JOIN ultramerchant.dbo.STATUSCODE sc WITH(NOLOCK)
			ON sc.statuscode = o.processing_statuscode
		LEFT JOIN (
			SELECT DISTINCT ol.order_id
			FROM ultramerchant.dbo.ORDER_LINE  ol WITH(NOLOCK)
			JOIN #orders_base p WITH(NOLOCK) ON p.order_id = ol.order_id
			WHERE product_type_id IN (5,20)
			) AS gift_orders
			ON o.order_id = gift_orders.order_id
		LEFT JOIN ultramerchant.dbo.ORDER_TRACKING_DETAIL otd WITH(NOLOCK)
			ON o.order_tracking_id = otd.order_tracking_id
			AND otd.OBJECT = 'administrator'
		LEFT JOIN #missing_administrator_ids m WITH(NOLOCK)
			ON o.order_id = m.order_id
		LEFT JOIN #workday_employee w WITH(NOLOCK)
			ON COALESCE(otd.OBJECT_ID,m.administrator_id )= w.associate_id
			AND o.DATETIME_ADDED >= w.start_datetime
			AND o.DATETIME_ADDED < w.end_datetime
		LEFT JOIN ultramerchant.dbo.ORDER_DETAIL od WITH(NOLOCK)
			ON od.ORDER_ID = o.ORDER_ID
			AND od.NAME = 'retail_store_id'
		JOIN #store_timezone st WITH(NOLOCK)
			ON st.store_id = COALESCE(TRY_CONVERT(int,od.VALUE), o.STORE_ID)
		LEFT JOIN ultramerchant.dbo.ORDER_CLASSIFICATION oct WITH(NOLOCK)
			ON oct.order_id = o.order_id
			AND oct.order_type_id IN (3,40,11,26)
		LEFT JOIN(
			SELECT
				ol.order_id,
				COUNT(*) AS units
			FROM ultramerchant.dbo.ORDER_LINE ol WITH(NOLOCK)
			JOIN ultramerchant.dbo.PRODUCT_TYPE pt WITH(NOLOCK)
				ON pt.product_type_id = ol.product_type_id
			JOIN ultramerchant.dbo."ORDER" o WITH(NOLOCK)
				ON o.order_id = ol.order_id
			JOIN #store_timezone st WITH(NOLOCK)
				ON st.STORE_ID = o.store_id
			JOIN ultramerchant.dbo.STATUSCODE sc WITH(NOLOCK)
				ON sc.statuscode = ol.statuscode
			WHERE ol.product_type_id != 14
				AND pt.is_free = 0
				AND sc.label != 'Cancelled'
				AND  CAST(o.DATETIME_ADDED AT TIME ZONE 'Pacific Standard Time' AT TIME ZONE st.sf_timezone AS DATETIME) >= @realtime_start_date
			GROUP BY ol.order_id
		) units
		ON units.order_id = o.order_id
	WHERE is_test_account = 'FALSE'
	  AND oct.order_id IS NULL
	  AND gift_orders.order_id IS NULL
	  AND (
			COALESCE(o.datetime_shipped, o.date_shipped) IS NOT NULL
			OR sc.label IN ('Shipped', 'Complete')
			OR sc.label IN ('Placed', 'FulFillment (Batching)', 'FulFillment (In Progress)', 'Ready For Pickup', 'Hold (Manual Review - Group 1)')
			OR (--order_type_id = 36 AND
				sc.label ='Hold (Preorder)')
		  )

	IF OBJECT_ID('tempdb..#orders_base_convert_eu') IS NOT NULL
		DROP TABLE #orders_base_convert_eu

	select o.*,
		COALESCE(ocr.EXCHANGE_RATE,1) as USD_EXCHANGE_RATE,
		COALESCE(vrh.RATE,0) as VAT_RATE,
		(COALESCE(subtotal_gross_vat_local, 0) / (1 + COALESCE(vrh.rate, 0))) * COALESCE(ocr.EXCHANGE_RATE, 1) as subtotal_net_vat_usd,
		(COALESCE(discount_gross_vat_local, 0) / (1 + COALESCE(vrh.rate, 0))) * COALESCE(ocr.EXCHANGE_RATE, 1) as discount_net_vat_usd,
		(COALESCE(shipping_rev_gross_vat_local, 0) / (1 + COALESCE(vrh.rate, 0))) * COALESCE(ocr.EXCHANGE_RATE, 1) as shipping_rev_net_vat_usd,
		(COALESCE(revenue_gross_vat_local,0)/(1+COALESCE(vrh.rate,0))) * COALESCE(ocr.EXCHANGE_RATE,1) as revenue_net_vat_usd
	INTO #orders_base_convert_eu
	FROM #orders_base_final o with(nolock)
	JOIN analytic.dbo.store ds with(nolock) on ds.STORE_ID = o.EVENT_STORE_ID
	LEFT JOIN ultramerchant.dbo.[address] AS a with(nolock) ON a.address_id = o.shipping_address_id --used to remove eu vat from backed in order values
	LEFT JOIN analytic.dbo.currency_exchange_rate ocr with(nolock)
		on ocr.SRC_CURRENCY = ds.STORE_CURRENCY --used to convert eu transactions to usd
		and CAST(o.SLOT AS DATE) >= cast(ocr.effective_start_datetime as date) and CAST(o.SLOT AS DATE) < cast(ocr.effective_end_datetime as date)
		and DEST_CURRENCY = 'USD'
	LEFT JOIN analytic.dbo.vat_rate_history AS vrh with(nolock)
		ON REPLACE(vrh.country_code, 'GB', 'UK') = REPLACE(a.COUNTRY_CODE, 'GB', 'UK')
		AND CAST(o.SLOT AS DATE) BETWEEN vrh.START_DATE AND vrh.EXPIRES_DATE

	IF OBJECT_ID('tempdb..#orders_final') IS NOT NULL
		DROP TABLE #orders_final

	SELECT
		VIP_STORE_ID,
		EVENT_STORE_ID,
		CUSTOMER_GENDER,
		FK_CROSS_PROMO,
		FINANCE_SUB_STORE,
		SLOT,
		EMPLOYEE_NUMBER,
		MAX(max_refresh_time) AS max_refresh_time,
		SUM(IIF(order_type = 'Successful Billing',revenue_net_vat_usd,0)) as successful_billing_revenue_net_vat_usd,
		COUNT(IIF(order_type = 'Activating VIP', order_id, NULL)) AS activating_order_count,
		SUM(IIF(order_type = 'Activating VIP', units, 0)) AS activating_unit_count,
		SUM(IIF(order_type = 'Activating VIP', revenue_net_vat_usd, 0)) AS activating_revenue_net_vat_usd,
		COUNT(IIF(order_type in ('Repeat VIP', 'First Guest', 'Repeat Guest'), order_id, NULL)) AS nonactivating_order_count,
		SUM(IIF(order_type in ('Repeat VIP', 'First Guest', 'Repeat Guest'), units, 0)) AS nonactivating_unit_count,
		SUM(IIF(order_type in ('Repeat VIP', 'First Guest', 'Repeat Guest'), revenue_net_vat_usd, 0)) AS nonactivating_revenue_net_vat_usd,
		-- the next three are subsets of 'Non-Activating'
		COUNT(IIF(order_type = 'First Guest', order_id, NULL)) AS first_guest_order_count,
		SUM(IIF(order_type = 'First Guest', units, 0)) AS first_guest_unit_count,
		SUM(IIF(order_type = 'First Guest', revenue_net_vat_usd, 0)) AS first_guest_revenue_net_vat_usd,
		COUNT(IIF(order_type = 'Repeat Guest', order_id, NULL)) AS repeat_guest_order_count,
		SUM(IIF(order_type = 'Repeat Guest', units, 0)) AS repeat_guest_unit_count,
		SUM(IIF(order_type = 'Repeat Guest', revenue_net_vat_usd, 0)) AS repeat_guest_revenue_net_vat_usd,
		COUNT(IIF(order_type = 'Repeat VIP', order_id, NULL)) AS repeat_vip_order_count,
		SUM(IIF(order_type = 'Repeat VIP', units, 0)) AS repeat_vip_unit_count,
		SUM(IIF(order_type = 'Repeat VIP', revenue_net_vat_usd, 0)) AS repeat_vip_revenue_net_vat_usd
	INTO #orders_final
	FROM #orders_base_convert_eu with(nolock)
	where order_type != 'Pending Billing'
		and SLOT >= @realtime_start_date
	GROUP BY VIP_STORE_ID,
		EVENT_STORE_ID,
		CUSTOMER_GENDER,
		FK_CROSS_PROMO,
		FINANCE_SUB_STORE,
		SLOT,
		EMPLOYEE_NUMBER


	IF OBJECT_ID('tempdb..#refund_base') IS NOT NULL
		DROP TABLE #refund_base

	SELECT
		ofin.EVENT_STORE_ID,
		ofin.VIP_STORE_ID,
		ofin.FK_CROSS_PROMO,
		ofin.CUSTOMER_GENDER,
		ofin.FINANCE_SUB_STORE,
		ofin.order_type,
		ofin.EMPLOYEE_NUMBER,
		CASE WHEN DATEPART(MINUTE,CAST(r.DATETIME_TRANSACTION AT TIME ZONE 'Pacific Standard Time' AT TIME ZONE st.sf_timezone AS DATETIME)) < 30
				THEN DATEADD(HOUR, DATEDIFF(HOUR, 0, CAST(r.DATETIME_TRANSACTION AT TIME ZONE 'Pacific Standard Time' AT TIME ZONE st.sf_timezone AS DATETIME)), 0)
			ELSE DATEADD(MINUTE,30,DATEADD(HOUR, DATEDIFF(HOUR, 0, CAST(r.DATETIME_TRANSACTION AT TIME ZONE 'Pacific Standard Time' AT TIME ZONE st.sf_timezone AS DATETIME)), 0))
			END AS SLOT, -- refund hour
		COUNT(IIF(ofin.order_type IN ('Activating VIP','Repeat VIP', 'First Guest', 'Repeat Guest'),COALESCE(r.order_id,0),0)) AS orders,
		SUM(IIF(ofin.order_type IN ('Activating VIP','Repeat VIP', 'First Guest', 'Repeat Guest'),COALESCE(units.units,0),0)) AS units,
		SUM(COALESCE(r.PRODUCT_REFUND,0) + COALESCE(r.shipping_refund,0)) AS refund_gross_vat_local
	INTO #refund_base
	FROM ultramerchant.dbo.refund r WITH(NOLOCK)
	JOIN ultramerchant.dbo.STATUSCODE sc WITH(NOLOCK)
		ON sc.STATUSCODE = r.STATUSCODE
	JOIN #orders_base_convert_eu ofin WITH(NOLOCK)
		ON ofin.order_id = r.ORDER_ID
	JOIN #store_timezone st WITH(NOLOCK)
		ON st.STORE_ID = ofin.EVENT_STORE_ID
	LEFT JOIN(
		SELECT
			R.REFUND_ID,
			COALESCE(SUM(RL.QUANTITY), 0) AS units
		FROM ultramerchant.dbo.[return] r WITH(NOLOCK)
		JOIN ultramerchant.dbo.return_line rl WITH(NOLOCK)
			ON r.return_id = rl.return_id
		WHERE R.REFUND_ID IS NOT NULL
		GROUP BY R.REFUND_ID
		) units
		ON units.refund_id = r.refund_id
	WHERE sc.LABEL = 'Refunded'
		AND r.DATETIME_TRANSACTION >= @realtime_start_date
	GROUP BY ofin.EVENT_STORE_ID,
		ofin.VIP_STORE_ID,
		ofin.FK_CROSS_PROMO,
		ofin.CUSTOMER_GENDER,
		ofin.FINANCE_SUB_STORE,
		ofin.order_type,
		ofin.EMPLOYEE_NUMBER,
		CASE WHEN DATEPART(MINUTE,CAST(r.DATETIME_TRANSACTION AT TIME ZONE 'Pacific Standard Time' AT TIME ZONE st.sf_timezone AS DATETIME)) < 30
				THEN DATEADD(HOUR, DATEDIFF(HOUR, 0, CAST(r.DATETIME_TRANSACTION AT TIME ZONE 'Pacific Standard Time' AT TIME ZONE st.sf_timezone AS DATETIME)), 0)
			ELSE DATEADD(MINUTE,30,DATEADD(HOUR, DATEDIFF(HOUR, 0, CAST(r.DATETIME_TRANSACTION AT TIME ZONE 'Pacific Standard Time' AT TIME ZONE st.sf_timezone AS DATETIME)), 0))
			END


	-------------------------------------

	-- group refunds from source & refunds from warehouse together in one output
	IF OBJECT_ID('tempdb..#refunds') IS NOT NULL
		DROP TABLE #refunds

	select
		r.EVENT_STORE_ID,
		r.VIP_STORE_ID,
		r.FK_CROSS_PROMO,
		r.CUSTOMER_GENDER,
		r.FINANCE_SUB_STORE,
		r.SLOT,
		r.EMPLOYEE_NUMBER,
		COUNT(COALESCE(orders,0)) as orders,
		SUM(COALESCE(units,0)) as units,
		SUM(IIF(r.order_type = 'Successful Billing', (refund_gross_vat_local/(1 + COALESCE(vrh.RATE,0))) * COALESCE(ocr.EXCHANGE_RATE,1),0)) as successful_billing_refund_net_vat_usd,
		SUM(IIF(r.order_type = 'Activating VIP', (refund_gross_vat_local/(1 + COALESCE(vrh.RATE,0))) * COALESCE(ocr.EXCHANGE_RATE,1),0)) as activating_product_refund_net_vat_usd,
		SUM(IIF(r.order_type in ('Repeat VIP', 'First Guest', 'Repeat Guest'), (refund_gross_vat_local/(1 + COALESCE(vrh.RATE,0))) * COALESCE(ocr.EXCHANGE_RATE,1),0)) as nonactivating_product_refund_net_vat_usd
	INTO #refunds
	FROM #refund_base r with(nolock)
	JOIN analytic.dbo.STORE ds with(nolock) on ds.STORE_ID = r.EVENT_STORE_ID
	LEFT JOIN analytic.dbo.currency_exchange_rate ocr with(nolock)
		on ocr.SRC_CURRENCY = ds.STORE_CURRENCY --used to convert eu transactions to usd
		and CAST(r.SLOT AS DATE) >= cast(ocr.effective_start_datetime AS DATE) AND CAST(r.SLOT AS DATE) < CAST(ocr.effective_end_datetime AS DATE)
		and DEST_CURRENCY = 'USD'
	LEFT JOIN analytic.dbo.vat_rate_history AS vrh with(nolock)
		ON REPLACE(vrh.country_code, 'GB', 'UK') = ds.STORE_COUNTRY
		AND CAST(r.SLOT AS DATE) BETWEEN vrh.START_DATE AND vrh.EXPIRES_DATE
	where r.order_type != 'Pending Billing'
	group by r.EVENT_STORE_ID,
		r.VIP_STORE_ID,
		r.FK_CROSS_PROMO,
		r.CUSTOMER_GENDER,
		r.FINANCE_SUB_STORE,
		r.SLOT,
		r.EMPLOYEE_NUMBER

	------------------------------------------------------------------------
	-- get vip counts from source (most recent 3 days) & vip counts from warehouse (3 days & prior (back 13 months))
	IF OBJECT_ID('tempdb..#vips') IS NOT NULL
		DROP TABLE #vips

	SELECT
		cs.VIP_STORE_ID,
		cs.vip_store_id as EVENT_STORE_ID,
		CUSTOMER_GENDER,
		FK_CROSS_PROMO,
		FINANCE_SUB_STORE,
		CASE WHEN DATEPART(MINUTE,DATETIME_LOCAL_ACTIVATED) < 30
				THEN dateadd(hour, datediff(hour, 0, DATETIME_LOCAL_ACTIVATED), 0)
			ELSE DATEADD(MINUTE,30,DATEADD(hour, datediff(hour, 0, DATETIME_LOCAL_ACTIVATED), 0))
			END as SLOT,
		EMPLOYEE_NUMBER,
		COUNT(1) AS vips,
		MAX(cs.DATETIME_LOCAL_ACTIVATED) AS max_refresh_time
	INTO #vips
	FROM #customer_classification_from_source cs with(nolock)
	WHERE cs.DATETIME_LOCAL_ACTIVATED >= @realtime_start_date
		AND cs.is_test_account = 'FALSE'
	GROUP BY cs.VIP_STORE_ID,
		cs.vip_store_id,
		CUSTOMER_GENDER,
		FK_CROSS_PROMO,
		FINANCE_SUB_STORE,
		CASE WHEN DATEPART(MINUTE,DATETIME_LOCAL_ACTIVATED) < 30
				THEN dateadd(hour, datediff(hour, 0, DATETIME_LOCAL_ACTIVATED), 0)
			ELSE DATEADD(MINUTE,30,DATEADD(hour, datediff(hour, 0, DATETIME_LOCAL_ACTIVATED), 0))
			END,
		EMPLOYEE_NUMBER

	IF OBJECT_ID('tempdb..#scaffold') IS NOT NULL
		DROP TABLE #scaffold

	SELECT DISTINCT EVENT_STORE_ID, vip_store_id, CUSTOMER_GENDER, FK_CROSS_PROMO, FINANCE_SUB_STORE, SLOT,EMPLOYEE_NUMBER
	INTO #scaffold
	FROM (
		SELECT EVENT_STORE_ID, vip_store_id, CUSTOMER_GENDER, FK_CROSS_PROMO, FINANCE_SUB_STORE, SLOT,EMPLOYEE_NUMBER
		FROM #orders_final with(nolock)
		UNION
		SELECT EVENT_STORE_ID, vip_store_id, CUSTOMER_GENDER, FK_CROSS_PROMO, FINANCE_SUB_STORE, SLOT,EMPLOYEE_NUMBER
		FROM #refunds with(nolock)
		UNION
		SELECT EVENT_STORE_ID, vip_store_id, CUSTOMER_GENDER, FK_CROSS_PROMO, FINANCE_SUB_STORE, SLOT,EMPLOYEE_NUMBER
		FROM #vips with(nolock)
		) AS A

	TRUNCATE TABLE analytic.dbo.realtime_from_source

	INSERT INTO analytic.dbo.realtime_from_source WITH(UPDLOCK)
	(
	EVENT_STORE_BRAND,
	EVENT_STORE_REGION,
	EVENT_STORE_COUNTRY,
	EVENT_STORE_LOCATION,
	EVENT_STORE_NAME,
	VIP_ACTIVATION_LOCATION,
	VIP_STORE_NAME,
	CUSTOMER_GENDER,
	FK_CROSS_PROMO,
	FINANCE_SUB_STORE,
	SLOT,
	EMPLOYEE_NUMBER,
	STORE_CODE,
	[date],
	successful_billing_cash_gross_revenue,
	activating_order_count,
	activating_unit_count,
	activating_revenue,
	nonactivating_order_count,
	nonactivating_unit_count,
	nonactivating_revenue,
	first_guest_order_count,
	first_guest_unit_count,
	first_guest_revenue,
	repeat_guest_order_count,
	repeat_guest_unit_count,
	repeat_guest_revenue,
	repeat_vip_order_count,
	repeat_vip_unit_count,
	repeat_vip_revenue,
	billing_refund,
	activating_product_refund,
	nonactivating_product_refund,
	guest_order_count,
	order_count,
	unit_count,
	product_gross_revenue,
	refund_units,
	refund_orders,
	product_refund,
	vips,
	max_refresh_time
	)
	SELECT
		cs.STORE_BRAND AS EVENT_STORE_BRAND,
		cs.STORE_REGION AS EVENT_STORE_REGION,
		cs.STORE_COUNTRY AS EVENT_STORE_COUNTRY,
		cs.STORE_TYPE AS EVENT_STORE_LOCATION,
		cs.STORE_FULL_NAME AS EVENT_STORE_NAME,
		vs.STORE_TYPE AS VIP_ACTIVATION_LOCATION,
		vs.STORE_FULL_NAME AS VIP_STORE_NAME,
		s.CUSTOMER_GENDER,
		s.FK_CROSS_PROMO,
		s.FINANCE_SUB_STORE,
		s.SLOT,
		s.EMPLOYEE_NUMBER,
		cs.STORE_RETAIL_LOCATION_CODE AS STORE_CODE,
		DATEADD(DAY, DATEDIFF(DAY, 0, s.SLOT), 0) AS date,
		COALESCE(o.successful_billing_revenue_net_vat_usd,0) AS successful_billing_cash_gross_revenue,
		COALESCE(o.activating_order_count,0) AS activating_order_count,
		COALESCE(o.activating_unit_count,0) AS activating_unit_count,
		COALESCE(o.activating_revenue_net_vat_usd,0) AS activating_revenue,
		COALESCE(o.nonactivating_order_count,0) AS nonactivating_order_count,
		COALESCE(o.nonactivating_unit_count,0) AS nonactivating_unit_count,
		COALESCE(o.nonactivating_revenue_net_vat_usd,0) AS nonactivating_revenue,

		COALESCE(o.first_guest_order_count,0) AS first_guest_order_count,
		COALESCE(o.first_guest_unit_count,0) AS first_guest_unit_count,
		COALESCE(o.first_guest_revenue_net_vat_usd,0) AS first_guest_revenue,

		COALESCE(o.repeat_guest_order_count,0) AS repeat_guest_order_count,
		COALESCE(o.repeat_guest_unit_count,0) AS repeat_guest_unit_count,
		COALESCE(o.repeat_guest_revenue_net_vat_usd,0) AS repeat_guest_revenue,

		COALESCE(o.repeat_vip_order_count,0) AS repeat_vip_order_count,
		COALESCE(o.repeat_vip_unit_count,0) AS repeat_vip_unit_count,
		COALESCE(o.repeat_vip_revenue_net_vat_usd,0) AS repeat_vip_revenue,

		COALESCE(r.successful_billing_refund_net_vat_usd,0) AS billing_refund,
		COALESCE(r.activating_product_refund_net_vat_usd,0) AS activating_product_refund,
		COALESCE(r.nonactivating_product_refund_net_vat_usd,0) AS nonactivating_product_refund,

		COALESCE(o.first_guest_order_count,0) + COALESCE(o.repeat_guest_order_count,0) AS guest_order_count,
		COALESCE(o.activating_order_count,0) + COALESCE(o.nonactivating_order_count,0) AS order_count,
		COALESCE(o.activating_unit_count,0) + COALESCE(o.nonactivating_unit_count,0) AS unit_count,
		COALESCE(o.activating_revenue_net_vat_usd,0) + COALESCE(o.nonactivating_revenue_net_vat_usd,0) AS product_gross_revenue,
		COALESCE(r.units,0) AS refund_units,
		COALESCE(r.orders,0) AS refund_orders,
		COALESCE(r.nonactivating_product_refund_net_vat_usd,0) + COALESCE(r.activating_product_refund_net_vat_usd,0) AS product_refund,
		COALESCE(v.vips, 0) AS vips,
		CASE WHEN o.max_refresh_time >= v.max_refresh_time THEN v.max_refresh_time ELSE o.max_refresh_time END AS max_refresh_time
	FROM #scaffold s WITH(NOLOCK)
	JOIN analytic.dbo.STORE cs WITH(NOLOCK) ON s.EVENT_STORE_ID = cs.STORE_ID
	LEFT JOIN analytic.dbo.STORE vs WITH(NOLOCK) ON s.vip_store_id = vs.STORE_ID
	LEFT JOIN #orders_final o WITH(NOLOCK) ON s.EVENT_STORE_ID = o.EVENT_STORE_ID
		AND s.vip_store_id = o.VIP_STORE_ID
		AND s.CUSTOMER_GENDER = o.CUSTOMER_GENDER
		AND s.FK_CROSS_PROMO = o.FK_CROSS_PROMO
		AND s.FINANCE_SUB_STORE = o.FINANCE_SUB_STORE
		AND s.SLOT = o.SLOT
		AND s.EMPLOYEE_NUMBER = o.EMPLOYEE_NUMBER
	LEFT JOIN #vips v WITH(NOLOCK)
		ON v.EVENT_STORE_ID = s.EVENT_STORE_ID
		AND v.vip_store_id = s.vip_store_id
		AND v.CUSTOMER_GENDER = s.CUSTOMER_GENDER
		AND v.FK_CROSS_PROMO = s.FK_CROSS_PROMO
		AND v.FINANCE_SUB_STORE = s.FINANCE_SUB_STORE
		AND v.SLOT = s.SLOT
		AND v.EMPLOYEE_NUMBER = s.EMPLOYEE_NUMBER
	LEFT JOIN #refunds r WITH(NOLOCK)
		ON r.EVENT_STORE_ID = s.EVENT_STORE_ID
		AND r.vip_store_id = s.vip_store_id
		AND r.CUSTOMER_GENDER = s.CUSTOMER_GENDER
		AND r.FK_CROSS_PROMO = s.FK_CROSS_PROMO
		AND r.FINANCE_SUB_STORE = s.FINANCE_SUB_STORE
		AND r.SLOT = s.SLOT
		AND r.EMPLOYEE_NUMBER = s.EMPLOYEE_NUMBER
	WHERE cs.STORE_BRAND IN ('Savage X','Fabletics')

END




GO

