from dataclasses import dataclass
from os import path

from include.utils.snowflake import Column

__CLIENT_CUSTOMER_ID_LIST = [
    "1046930040",
    "1067576626",
    "1078773456",
    "1093066110",
    "1129517880",
    "1171153528",
    "1252593211",
    "1331571525",
    "1415117519",
    "1424621829",
    "1481129565",
    "1519834929",
    "1623957507",
    "1694461393",
    "1952697502",
    "2030598859",
    "2045579477",
    "2301369398",
    "2459076624",
    "2588374640",
    "2646630478",
    "2687788606",
    "2725998358",
    "2736939730",
    "2776663610",
    "2934744006",
    "2994356974",
    "3011963080",
    "3369356857",
    "3550823225",
    "3770136848",
    "3908895696",
    "4001625728",
    "4147668980",
    "4182532245",
    "4198702762",
    "4218001020",
    "4227169685",
    "4229566660",
    "4237059474",
    "4284407212",
    "4289892905",
    "4319551013",
    "4366543796",
    "4379953876",
    "4537385564",
    "4704223200",
    "4815246814",
    "4960914690",
    "4984247336",
    "5022510306",
    "5266020912",
    "5266138582",
    "5357456395",
    "5394677459",
    "5520534856",
    "5523881938",
    "5879051817",
    "6071703296",
    "6257478617",
    "6330248906",
    "6379047454",
    "6481990328",
    "6548940025",
    "6680131322",
    "6713316635",
    "6737613556",
    "6884065071",
    "6914056030",
    "7008879804",
    "7151147625",
    "7215965915",
    "7324677289",
    "7345865444",
    "7420765134",
    "7536756037",
    "7594458267",
    "7685409754",
    "7860570773",
    "7949460930",
    "8057638417",
    "8232294379",
    "8249204247",
    "8510227042",
    "8571201082",
    "8650666401",
    "8776541027",
    "8792796699",
    "8877050082",
    "8923721765",
    "9087959504",
    "9136425456",
    "9146220107",
    "9347951239",
    "9361675270",
    "9371780320",
    "9714044463",
    "9749480566",
    "9759265024",
    "6550793280",
    "9854334693",
    "8906069850",
    "2115752003",
    "2200344913",
    "6254711409",
    "6618553603",
    "3091715831",
    "2242724552",
    "8792006949",
    "3886420113",
    "2847550141",
    "5963559537",
    "5455736339",
    "3333439780",
    "9826732845",
    "5403949764",
    "1805095909",
    # "3029882952",  #as we are pulling this account spend from SA360
    "3006337841",
    "6198013680",
    "8543764201",
    "3002594752",
    "3918956943",
    "8009463419",
    "2127393533",
    "3734911229",
    "4587698470",
]
__EXCL_LIST = {
    "8792796699",
    "7860570773",
    "7151147625",
    # "6680131322",
    "6257478617",
    "1424621829",
    "1694461393",
    "1129517880",
}

CLIENT_CUSTOMER_ID_LIST = [x for x in __CLIENT_CUSTOMER_ID_LIST if x not in __EXCL_LIST]


@dataclass
class Advertiser:
    name: str
    id: str


ADVERTISER_LIST = [
    Advertiser("Fabkids", "21700000001224429"),
    Advertiser("Fabletics", "21700000001232575"),
    Advertiser("Fabletics AT", "21700000001348368"),
    Advertiser("Fabletics CA (New)", "21700000001461661"),
    Advertiser("Fabletics CA - OLD", "21700000001361394"),
    Advertiser("Fabletics DE", "21700000001345356"),
    Advertiser("Fabletics DK", "21700000001471771"),
    Advertiser("Fabletics ES", "21700000001345353"),
    Advertiser("Fabletics FR", "21700000001345350"),
    Advertiser("Fabletics NL", "21700000001360478"),
    Advertiser("Fabletics SE", "21700000001471774"),
    Advertiser("Fabletics UK", "21700000001345359"),
    Advertiser("JustFab DE", "21700000001338990"),
    Advertiser("JustFab ES", "21700000001342579"),
    Advertiser("JustFab FR", "21700000001338699"),
    Advertiser("JustFab NL", "21700000001348722"),
    Advertiser("JustFab UK", "21700000001348988"),
    Advertiser("Justfab", "21700000001232881"),
    Advertiser("Justfab AT", "21700000001348674"),
    Advertiser("Justfab BE", "21700000001348056"),
    Advertiser("Justfab DK", "21700000001380006"),
    Advertiser("Justfab SE", "21700000001394270"),
    Advertiser("Savage X Fenty", "21700000001497640"),
    Advertiser("Savage X Fenty DE", "21700000001510648"),
    Advertiser("Savage X Fenty ES", "21700000001497528"),
    Advertiser("Savage X Fenty FR", "21700000001495137"),
    Advertiser("Savage X Fenty UK", "21700000001498331"),
    Advertiser("ShoeDazzle", "21700000001228976"),
]


def download_files(service, report_id, report_fragment, out_filename):
    """
    Generate and print sample report.

    :param service: An authorized Doublelcicksearch service.
    :param report_id: The ID DS has assigned to a report.
    :param report_fragment: The 0-based index of the file fragment from the files array.
    """
    file_request = service.reports().getFile(
        reportId=report_id, reportFragment=report_fragment
    )
    download = file_request.execute()

    if not path.exists(out_filename):
        pass
    else:
        # get first occurence of a new line. This will be where header ends
        header_end = download.find("\n")

        # get rid of header row appearing in each report. Want to keep header row if file does not exist yet
        download = download[header_end + 1 :]

    with open(out_filename, "a") as f:
        f.write(download)


def gsa360_produce_column_list(report_columns, report_definition):
    """
    Tries to infer column data types based on report definition copy pasted from google api doc reference.

    Here's a sample report definition input:

    .. code-block:: python

        floodlight_report_definition = '''
            columnName	Description	Behavior	Type	Filterable
            status	The status of the Floodlight activity: Active or Removed.	attribute	Status	Yes
            creationTimestamp	Timestamp of the Floodlight activity's creation, formatted in ISO 8601.	attribute	Timestamp	Yes
            lastModifiedTimestamp	Timestamp of the Floodlight activity's most recent modification, formatted in ISO 8601.	attribute	Timestamp	Yes
            floodlightGroup	Floodlight group name.	attribute	String	Yes
            floodlightGroupConversionType	The type of conversions generated by Floodlight activities in this group: Action or Transaction.	attribute	Conversion type	Yes
            floodlightGroupId	DS Floodlight group ID.	attribute	ID	Yes
            floodlightGroupTag	Floodlight group tag.	attribute	String	Yes
            floodlightConfigurationId	Campaign manager Floodlight configuration ID.	attribute	ID	Yes
            floodlightActivity	Floodlight activity name.	attribute	String	Yes
            floodlightActivityId	DS Floodlight activity ID.	attribute	ID	Yes
            floodlightActivityTag	Floodlight activity tag.	attribute	String	Yes
            agency	Agency name.	attribute	String	Yes
            agencyId	DS agency ID.	attribute	ID	Yes
            advertiser	Advertiser name.	attribute	String	Yes
            advertiserId	DS advertiser ID.	attribute	ID	Yes
        '''

    :param report_columns:
    :param report_definition:
    :return:
    """  # noqa: E501

    from include.utils.string import camel_to_snake

    orig_col_names = [list(x.items())[0][1] for x in report_columns]

    col_list = []

    for line in report_definition.splitlines():
        col_list.append(line.split("\t"))

    type_map = {
        "String": "VARCHAR",
        "Timestamp": "TIMESTAMP_LTZ",
        "Impression share": "VARCHAR",
        "Number": "NUMBER(38,8)",
        "String list": "VARIANT",
        "ID": "VARCHAR",
        "Date": "DATE",
        "Money": "NUMBER(38,8)",
        "Campaign type": "VARCHAR",
        "Boolean": "BOOLEAN",
        "Engine type": "VARCHAR",
    }
    mapping = {x[0]: type_map.get(x[3], x[3]) for x in col_list if len(x) > 3}

    return [Column(camel_to_snake(x), mapping.get(x)) for x in orig_col_names]
