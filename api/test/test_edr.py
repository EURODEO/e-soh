from unittest.mock import patch

import routers.edr as edr
from fastapi.testclient import TestClient
from main import app
from test.utilities import create_mock_obs_response
from test.utilities import load_json


client = TestClient(app)


def test_get_locations_id_with_single_parameter_query_without_format():
    with patch("routers.edr.getObsRequest") as mock_getObsRequest:
        test_data = load_json("test/test_data/test_single_proto.json")
        compare_data = load_json("test/test_data/test_single_covjson.json")

        mock_getObsRequest.return_value = create_mock_obs_response(test_data)

        response = client.get(
            "/collections/observations/locations/06260?"
            + "parameter-name=ff&datetime=2022-12-31T00:00:00Z/2022-12-31T01:00:00Z"
        )

        # Check that getObsRequest gets called with correct arguments given in query
        mock_getObsRequest.assert_called_once()
        m_args = mock_getObsRequest.call_args[0][0]

        assert {"ff"} == set(m_args.filter["instrument"].values)
        assert {"06260"} == set(m_args.filter["platform"].values)
        assert "2022-12-31 00:00:00" == m_args.temporal_interval.start.ToDatetime().strftime("%Y-%m-%d %H:%M:%S")
        assert "2022-12-31 01:00:01" == m_args.temporal_interval.end.ToDatetime().strftime("%Y-%m-%d %H:%M:%S")

        assert response.status_code == 200
        assert response.json()["type"] == "Coverage"
        assert response.json() == compare_data


def test_get_locations_id_without_parameter_names_query():
    with patch("routers.edr.getObsRequest") as mock_getObsRequest:
        test_data = load_json("test/test_data/test_multiple_proto.json")
        compare_data = load_json("test/test_data/test_multiple_covjson.json")

        mock_getObsRequest.return_value = create_mock_obs_response(test_data)

        response = client.get("/collections/observations/locations/06260?f=covjson")

        # Check that getObsRequest gets called with correct arguments when no parameter names are given
        # in query
        mock_getObsRequest.assert_called_once()
        m_args = mock_getObsRequest.call_args[0][0]

        assert {"*"} == set(m_args.filter["instrument"].values)
        assert response.status_code == 200
        assert response.json() == compare_data


def test_get_locations_id_with_incorrect_datetime_format():
    response = client.get("/collections/observations/locations/06260?datetime=20221231T000000Z/20221231T010000Z")

    assert response.status_code == 400
    assert response.json() == {"detail": {"datetime": "Invalid format: 20221231T000000Z/20221231T010000Z"}}


def test_get_locations_id_with_incorrect_datetime_range():
    response = client.get(
        "/collections/observations/locations/06260?datetime=2024-12-31T00:00:00Z/2022-12-31T01:00:00Z"
    )

    assert response.status_code == 400
    assert response.json() == {"detail": {"datetime": "Invalid range: 2024-12-31T00:00:00Z > 2022-12-31T01:00:00Z"}}


def test_get_locations_id_with_empty_response():
    with patch("routers.edr.getObsRequest") as mock_getObsRequest:
        test_data = load_json("test/test_data/test_empty_proto.json")

        mock_getObsRequest.return_value = create_mock_obs_response(test_data)

        response = client.get("/collections/observations/locations/10000?f=covjson")

        assert response.status_code == 404
        assert response.json() == {"detail": "No data found"}


def test_get_area_with_normal_query():
    with patch("routers.edr.getObsRequest") as mock_getObsRequest:
        test_data = load_json("test/test_data/test_coverages_proto.json")
        compare_data = load_json("test/test_data/test_coverages_covjson.json")

        mock_getObsRequest.return_value = create_mock_obs_response(test_data)

        response = client.get(
            "/collections/observations/area?coords=POLYGON((22.12 59.86, 24.39 60.41, "
            " 24.39 60.41, 24.39 59.86, 22.12 59.86))"
            "&parameter-name=TA_P1D_AVG&datetime=2022-12-31T00:00:00Z/2022-12-31T01:00:00Z"
        )

        # Check that getObsRequest gets called with correct arguments given in query
        mock_getObsRequest.assert_called_once()
        m_args = mock_getObsRequest.call_args[0][0]

        assert {"TA_P1D_AVG"} == set(m_args.filter["instrument"].values)
        assert len(m_args.spatial_area.points) == 5
        assert 22.12 == m_args.spatial_area.points[0].lon
        assert "2022-12-31 00:00:00" == m_args.temporal_interval.start.ToDatetime().strftime("%Y-%m-%d %H:%M:%S")
        assert "2022-12-31 01:00:01" == m_args.temporal_interval.end.ToDatetime().strftime("%Y-%m-%d %H:%M:%S")

        assert response.status_code == 200
        assert response.json() == compare_data


def test_get_area_with_incorrect_coords():
    response = client.get("/collections/observations/area?coords=POLYGON((22.12 59.86, 24.39 60.41))")

    assert response.status_code == 400
    assert response.json() == {
        "detail": {"coords": "Invalid or unparseable wkt provided: POLYGON((22.12 59.86, 24.39 60.41))"}
    }


def test_get_area_with_incorrect_geometry_type():
    response = client.get("/collections/observations/area?coords=POINT(22.12 59.86)")

    assert response.status_code == 400
    assert response.json() == {"detail": {"coords": "Invalid geometric type: Point"}}


def test_get_position_with_normal_query():
    # Wrap the original get_data_area to a mock so we can assert against the call values
    with patch("routers.edr.get_data_area", wraps=edr.get_data_area) as mock_get_data_area, patch(
        "routers.edr.getObsRequest"
    ) as mock_getObsRequest:
        test_data = load_json("test/test_data/test_coverages_proto.json")
        compare_data = load_json("test/test_data/test_coverages_covjson.json")

        mock_getObsRequest.return_value = create_mock_obs_response(test_data)

        response = client.get(
            "/collections/observations/position?coords=POINT(5.179705 52.0988218)"
            "&parameter-name=TA_P1D_AVG&datetime=2022-12-31T00:00Z/2022-12-31T00:00Z"
        )

        mock_get_data_area.assert_called_once()
        mock_get_data_area.assert_called_with(
            "POLYGON ((5.179805 52.0988218, 5.179705 52.0987218, 5.1796050000000005 52.0988218, "
            "5.179705 52.09892180000001, 5.179805 52.0988218))",
            "TA_P1D_AVG",
            "2022-12-31T00:00Z/2022-12-31T00:00Z",
            "covjson",
        )

        assert response.status_code == 200
        assert response.json() == compare_data


def test_get_position_with_incorrect_coords():
    response = client.get("/collections/observations/position?coords=POINT(60.41)")

    assert response.status_code == 400
    assert response.json() == {"detail": {"coords": "Invalid or unparseable wkt provided: POINT(60.41)"}}


def test_get_position_with_incorrect_geometry_type():
    response = client.get(
        "/collections/observations/position?coords=POLYGON((22.12 59.86, 24.39 60.41, "
        "24.39 60.41, 24.39 59.86, 22.12 59.86))"
    )

    assert response.status_code == 400
    assert response.json() == {"detail": {"coords": "Invalid geometric type: Polygon"}}