import pytest
from optparse import OptionParser
import concurrency_memcload as cm


def cases(cases_list):
    def deco(func):
        def wrapper(*args):
            for case in cases_list:
                func(case, *args)

        return wrapper

    return deco


op = OptionParser()
op.add_option("-t", "--test", action="store_true", default=False)
op.add_option("-l", "--log", action="store", default=None)
op.add_option("--dry", action="store_true", default=False)
op.add_option("--pattern", action="store", default="./data/appsinstalled/*.tsv.gz")
op.add_option("--idfa", action="store", default="127.0.0.1:33013")
op.add_option("--gaid", action="store", default="127.0.0.1:33014")
op.add_option("--adid", action="store", default="127.0.0.1:33015")
op.add_option("--dvid", action="store", default="127.0.0.1:33016")
options, _ = op.parse_args()

device_memc = {
    "idfa": options.idfa,
    "gaid": options.gaid,
    "adid": options.adid,
    "dvid": options.dvid,
}


def test_invalid_memcached_addr():
    client = cm.MyRetryingClient().get_client("128.1.1.1:8000")
    with pytest.raises(Exception):
        client.set("key", "value")


@cases(
    [
        {
            "test_line": "idfa\tjfkf65c0ck920dlzl\t-33.33\t55.55\t1,2,3,4,5,6,7,8,9,0\n",
            "result": cm.AppsInstalled(
                dev_type="idfa",
                dev_id="jfkf65c0ck920dlzl",
                lat=-33.33,
                lon=55.55,
                apps=[1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
            ),
        },
        {
            "test_line": "idfa\tf8abee8b1485bf8b0d0630f3f96c97f8\t3914,2301,5343,6292,7196,7849\n",
            "result": None,
        },
    ]
)
def test_parse_appsinstalled(data):
    assert cm.parse_appsinstalled(data["test_line"]) == data["result"]


@cases(
    [
        {
            "test_line": ["idfa\tjd9sdk5bf7s0skgj\t1,2,3,4,5,6,7,8,9,0\n"],
            "result": (
                0,
                1,
            ),
        },
        {
            "test_line": [
                "adid\txev45mvne6sjwb30s6fjk\t12,34,56,78,90\n",
                "gaid\tf29is74jnd5n237zn\t-22.22\t33.33\t123,456,7890\n",
                "dvid\tf12jd7alf9nef96c97f8\t-44.44\t55.55\t12345,67890\n",
                "abcd\tee8b1485b0f3f96c97f8\t-66.66\t77.77\t1,2,3,4,5,6,7,8,9,0\n",
            ],
            "result": (
                2,
                2,
            ),
        },
    ]
)
def test_process_batch(data):
    assert cm.process_batch(data["test_line"], options) == data["result"]


def test_insert_appsinstalled():
    appsinstalled = cm.AppsInstalled(
        dev_type="idfa",
        dev_id="f8abee8b1485bf8b0d0630f3f96c97f8",
        lat=-11.1111,
        lon=22.2222,
        apps=[333, 444, 555, 666, 777, 888],
    )
    assert cm.insert_appsinstalled("127.0.0.1:33013", appsinstalled) is True
    assert (
        cm.MyRetryingClient()
        .get_client("127.0.0.1:33013")
        .get("idfa:f8abee8b1485bf8b0d0630f3f96c97f8")
        is not None
    )
