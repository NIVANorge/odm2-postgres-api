from odm2_postgres_api.queries.user import full_name_to_split_tuple, get_nivaport_user


def test_user_fullname_splitting():
    assert full_name_to_split_tuple("Åge Olsen") == ("Åge", None, "Olsen")
    assert full_name_to_split_tuple("Åge Drillo Olsen") == ("Åge", "Drillo", "Olsen")
    assert full_name_to_split_tuple(" Can handle trailing spaces ") == ("Can", "handle trailing", "spaces")


def test_should_parse_base64_encoded_user_string():
    user = get_nivaport_user(
        "eyJpZCI6MjIxLCJ1aWQiOiIxZWQyMDBkMy1mMDlhLTQxNjQtOTExMC1hMWYyNGY4OTliYjMiLCJkaXNwbGF5TmFtZSI6IsOFZ2UgT2xzZW4iLCJlbWFpbCI6ImRldnVzZXJAc29tZWVtYWlsLmNvbSIsInByb3ZpZGVyIjoiRGV2TG9naW4iLCJjcmVhdGVUaW1lIjoiMjAyMC0wNC0yMFQxMTo0NToyMS4yNDFaIiwidXBkYXRlVGltZSI6IjIwMjAtMDQtMjBUMTE6NDU6MjEuMjQxWiIsInJvbGVzIjpbImFwcHM6YWRtaW4iLCJuaXZhIl19")

    assert user.id == "221"
    assert user.email == 'devuser@someemail.com'
    assert user.roles == ['apps:admin', 'niva']
    assert user.displayName == 'Åge Olsen'
