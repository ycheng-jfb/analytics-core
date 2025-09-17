USE [analytic]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[usp_rfid_inventory_export]
AS
BEGIN

	SET NOCOUNT ON
	DECLARE @realtime_start_date DATE
    SET @realtime_start_date  =  CONVERT(DATE,GETDATE())

    IF OBJECT_ID('tempdb..#orders_base') IS NOT NULL
        DROP TABLE #orders_base

    SELECT DISTINCT
        ORDER_ID,
        CUSTOMER_ID,
        DATETIME_ADDED
    INTO #orders_base
    FROM (
        SELECT
            ORDER_ID,
            CUSTOMER_ID,
            DATETIME_ADDED
        FROM
            ultramerchant.dbo.[ORDER] with(nolock)
        WHERE
            DATETIME_ADDED >= @realtime_start_date
    ) o

    IF OBJECT_ID('tempdb..#rfid_export') IS NOT NULL
        DROP TABLE #rfid_export

    SELECT DISTINCT
        relo.retail_location_code as site_code,
        o.order_id,
        ir.label AS product_id,
        SUM(ol.quantity) AS qty,
        o.DATETIME_ADDED AS timestamp,
        CASE WHEN rl.order_line_id IS NOT NULL
            THEN 'RETURN'
            ELSE 'SALE'
        END AS transaction_code,
        'Sales Floor' AS zone,
        NULL AS epc,
        NULL AS pos_id
    INTO #rfid_export
    FROM ultramerchant.dbo.ORDER_LINE ol WITH(NOLOCK)
    JOIN #orders_base o WITH(NOLOCK) ON o.ORDER_ID = ol.ORDER_ID
    JOIN ultramerchant.dbo.order_detail od ON od.ORDER_ID = o.ORDER_ID
        AND od.name = 'retail_store_id'
        AND od.value IS NOT NULL
        AND od.value in ('106','118','240', '257') --('106' = 0019, 118 = 0029, 240 = 0072, 257 = 0001)
    JOIN analytic.dbo.evolve_ultrawarehouse_retail_location relo ON relo.store_id = TRY_CAST(od.value AS INT)
        AND relo.RETAIL_LOCATION_CODE in ('0019','0029','0072','0001') --testing store codes, will need to edit this as we add more stores
    JOIN ultramerchant.dbo.product dp on dp.PRODUCT_ID = ol.PRODUCT_ID
    JOIN ultramerchant.dbo.item mi on dp.ITEM_ID = mi.ITEM_ID
    JOIN analytic.dbo.item wi on mi.ITEM_NUMBER = wi.ITEM_NUMBER
    LEFT JOIN analytic.dbo.item_reference ir on ir.item_id = wi.item_id
        AND ir.type_code_id = '1173'
    LEFT JOIN ultramerchant.dbo.return_line rl ON rl.order_line_id = ol.order_line_id
        AND rl.product_id = ol.product_id
    LEFT JOIN ultramerchant.dbo.retail_return rr on ol.order_id = rr.order_id
        AND rr.store_id in ('106','118','240','257') --keeps ONLY returns to one of the test stores, regardless of purchase origin
    GROUP BY
        relo.retail_location_code,
        o.order_id,
        ir.label,
        o.DATETIME_ADDED,
        CASE WHEN rl.order_line_id IS NOT NULL
            THEN 'RETURN'
            ELSE 'SALE'
        END

    TRUNCATE TABLE analytic.dbo.rfid_export

    INSERT INTO analytic.dbo.rfid_export WITH(UPDLOCK)
    (
        site_code,
        order_id,
        product_id,
        qty,
        timestamp,
        transaction_code,
        zone,
        epc,
        pos_id
    )
    SELECT
        site_code,
        order_id,
        product_id,
        qty,
        timestamp,
        transaction_code,
        zone,
        epc,
        pos_id
    FROM #rfid_export

END


GO
