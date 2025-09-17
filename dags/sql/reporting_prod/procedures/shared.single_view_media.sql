SET session_refresh_time = (SELECT CONVERT_TIMEZONE('America/Los_Angeles',MAX(session_local_datetime)) FROM reporting_base_prod.shared.session);

--aggregated version
CREATE OR REPLACE TRANSIENT TABLE shared.single_view_media AS
SELECT
     storebrand,
     storeregion,
     storecountry,
     sessionlocaldate,
     membershipstate,
     user_segment,
     --      ,leadtenuredaily
     leadtenuredailygroup,
     viptenuremonthlygroup,
     --      ,viptenuremonthly
     platform,
     platformraw,
     --      ,browser
     operatingsystem,
     hdyh,
     channel_type,
     channel,
     subchannel,
     --     ,utmmedium
     --     ,utmsource
     --     ,utmcampaign
     --     ,utmcontent
     --     ,utmterm
     gatewayid,
     dmgcode,
     gatewaytype,
     gatewaysubtype,
     gatewayname,
     gatewayoffer,
     gatewaygender,
     gatewayftvorrtv,
     gatewayinfluencername,
     gatewayexperience,
     gatewaytestname, --testsite
     gatewaytestid,
     gatewayteststartlocaldatetime,
     gatewaytestendlocaldatetime,
     gatewaytestdescription,
     gatewaytesthypothesis,
     gatewaytestresults,
     lptestcellid,
     gatewaytestlptrafficsplit,
     gatewaytestlptype, --testsitetype
     gatewaytestlpclass,
     gatewaytestlplocation,
     lpname,
     lpid,
     lptype,
     isyittygateway,
     isscrubsgateway,
     isscrubscustomer,
     isscrubsaction,
     isscrubssession,
     ismalegateway,
     ismalesessionaction,
     ismalesession,
     ismalecustomer,
     ismalecustomersessions,
     leadregistrationtype,
     activatingordermembershiptype,
     SUM(sessions) AS sessions,
     SUM(customers) AS customers,
     SUM(visitors) AS visitors,
     SUM(quizstarts) AS quizstarts,
     SUM(quizcompletes) AS quizcompletes,
     SUM(quizskips) AS quizskips,
     SUM(quizleads) AS quizleads,
     SUM(quizskipleads) AS quizskipleads,
     SUM(speedyleads) AS speedyleads,
     SUM(leads) AS leads, --lead registrations
     SUM(leadsreactivated) AS leadsreactivated,
     --FROM leads
     SUM(activatingorders24hrfromleads) AS activatingorders24hrfromleads,
     SUM(activatingordersd7fromleads) AS activatingordersd7fromleads,
     SUM(activatingordersm1fromleads) AS activatingordersm1fromleads,
     SUM(activatingordersfromleads) AS activatingordersfromleads,
     SUM(activatingunitsfromleads) AS activatingunitsfromleads,
     SUM(activatingsubtotalfromleads) AS activatingsubtotalfromleads,
     SUM(activatingdiscountfromleads) AS activatingdiscountfromleads,
     SUM(activatingproductgrossrevenuefromleads) AS activatingproductgrossrevenuefromleads,
     SUM(activatingproductmarginprereturnfromleads) AS activatingproductmarginprereturnfromleads,
     -- ON date
     SUM(activatingorders) AS activatingorders,
     SUM(nonactivatingorders) AS nonactivatingorders,
     SUM(activatingunits) AS activatingunits,
     SUM(nonactivatingunits) AS nonactivatingunits,
     SUM(activatingsubtotal) AS activatingsubtotal,
     SUM(nonactivatingsubtotal) AS nonactivatingsubtotal,
     SUM(activatingdiscount) AS activatingdiscount,
     SUM(nonactivatingdiscount) AS nonactivatingdiscount,
     SUM(activatingproductgrossrevenue) AS activatingproductgrossrevenue,
     SUM(nonactivatingproductgrossrevenue) AS nonactivatingproductgrossrevenue,
     SUM(activatingproductmarginprereturn) AS activatingproductmarginprereturn,
     SUM(nonactivatingproductmarginprereturn) AS nonactivatingproductmarginprereturn,
     SUM(activatingcashgrossrevenue) AS activatingcashgrossrevenue,
     SUM(nonactivatingcashgrossrevenue) AS nonactivatingcashgrossrevenue,
     $session_refresh_time AS refeshtime
FROM reporting_base_prod.shared.session_single_view_media
     WHERE
     sessionlocaldatetime >= ADD_MONTHS(CURRENT_DATE(), -14)
GROUP BY
     storebrand,
     storeregion,
     storecountry,
     sessionlocaldate,
     membershipstate,
     user_segment,
     --        ,leadtenuredaily
     leadtenuredailygroup,
     viptenuremonthlygroup,
     --        ,viptenuremonthly
     platform,
     platformraw,
     --        ,browser
     operatingsystem,
     hdyh,
     channel_type,
     channel,
     subchannel,
     --     ,utmmedium
     --     ,utmsource
     --     ,utmcampaign
     --     ,utmcontent
     --     ,utmterm
     gatewayid,
     dmgcode,
     gatewaytype,
     gatewaysubtype,
     gatewayname,
     gatewayoffer,
     gatewaygender,
     gatewayftvorrtv,
     gatewayinfluencername,
     gatewayexperience,
     gatewaytestname, --testsite
     gatewaytestid,
     gatewayteststartlocaldatetime,
     gatewaytestendlocaldatetime,
     gatewaytestdescription,
     gatewaytesthypothesis,
     gatewaytestresults,
     lptestcellid,
     gatewaytestlptrafficsplit,
     gatewaytestlptype, --testsitetype
     gatewaytestlpclass,
     gatewaytestlplocation,
     lpname,
     lpid,
     lptype,
     isyittygateway,
     isscrubsgateway,
     isscrubscustomer,
     isscrubsaction,
     isscrubssession,
     ismalegateway,
     ismalesessionaction,
     ismalesession,
     ismalecustomer,
     ismalecustomersessions,
     leadregistrationtype,
     activatingordermembershiptype;
