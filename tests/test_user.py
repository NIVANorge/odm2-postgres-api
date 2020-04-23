import json
from base64 import b64encode

from odm2_postgres_api.queries.user import full_name_to_split_tuple, get_nivaport_user


def test_user_fullname_splitting():
    assert full_name_to_split_tuple("Åge Olsen") == ("Åge", None, "Olsen")
    assert full_name_to_split_tuple("Åge Drillo Olsen") == ("Åge", "Drillo", "Olsen")
    assert full_name_to_split_tuple(" Can handle trailing spaces ") == ("Can", "handle trailing", "spaces")


def test_should_parse_base64_encoded_user_string():
    user_obj = {"id": 221,
                "uid": "1ed200d3-f09a-4164-9110-a1f24f899bb3",
                "displayName": "Åge Olsen",
                "email": "devuser@someemail.com",
                "provider": "DevLogin",
                "createTime": "2020-04-20T11:45:21.241Z",
                "updateTime": "2020-04-20T11:45:21.241Z",
                "roles": ["apps:admin", "niva"]}

    user = get_nivaport_user(b64encode(json.dumps(user_obj).encode('utf-8')))

    assert user.id == "221"
    assert user.email == 'devuser@someemail.com'
    assert user.roles == ['apps:admin', 'niva']
    assert user.displayName == 'Åge Olsen'
