from unittest.mock import patch

import routers.edr as edr
from fastapi.testclient import TestClient
from main import app
from test.utilities import create_mock_obs_response
from test.utilities import load_json


client = TestClient(app)


def test_get_locations_id_with_single_parameter_query_without_format():
    with patch("routers.edr.get_obs_request") as mock_get_obs_request, patch(
        "routers.edr.verify_parameter_names"
    ) as mock_verify_parameter_names:
        test_data = load_json("test/test_data/test_single_proto.json")
        compare_data = load_json("test/test_data/test_single_covjson.json")

        mock_verify_parameter_names.return_value = None
        mock_get_obs_request.return_value = create_mock_obs_response(test_data)

        response = client.get(
            "/collections/observations/locations/0-20000-0-06280?"
            + "parameter-name=wind_speed:10:mean:PT10M&datetime=2022-12-31T00:00:00Z/2022-12-31T01:00:00Z"
        )

        # Check that getObsRequest gets called with correct arguments given in query
        mock_get_obs_request.assert_called_once()
        m_args = mock_get_obs_request.call_args[0][0]

        assert {"wind_speed:10:mean:PT10M"} == set(m_args.filter["parameter_name"].values)
        assert {"0-20000-0-06280"} == set(m_args.filter["platform"].values)
        assert "2022-12-31 00:00:00" == m_args.temporal_interval.start.ToDatetime().strftime("%Y-%m-%d %H:%M:%S")
        assert "2022-12-31 01:00:01" == m_args.temporal_interval.end.ToDatetime().strftime("%Y-%m-%d %H:%M:%S")

        assert response.status_code == 200
        assert response.json()["type"] == "Coverage"
        assert response.json() == compare_data


def test_get_locations_id_without_parameter_names_query():
    with patch("routers.edr.get_obs_request") as mock_get_obs_request:
        test_data = load_json("test/test_data/test_multiple_proto.json")
        compare_data = load_json("test/test_data/test_multiple_covjson.json")

        mock_get_obs_request.return_value = create_mock_obs_response(test_data)

        response = client.get("/collections/observations/locations/0-20000-0-06280?f=CoverageJSON")

        # Check that getObsRequest gets called with correct arguments when no parameter names are given
        # in query
        mock_get_obs_request.assert_called_once()
        m_args = mock_get_obs_request.call_args[0][0]

        assert "instrument" not in m_args.filter
        assert {"0-20000-0-06280"} == set(m_args.filter["platform"].values)
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
    with patch("routers.edr.get_obs_request") as mock_get_obs_request:
        test_data = load_json("test/test_data/test_empty_proto.json")

        mock_get_obs_request.return_value = create_mock_obs_response(test_data)

        response = client.get("/collections/observations/locations/10000?f=CoverageJSON")

        assert response.status_code == 404
        assert response.json() == {"detail": "Requested data not found."}


def test_get_area_with_normal_query():
    with patch("routers.edr.get_obs_request") as mock_get_obs_request, patch(
        "routers.edr.verify_parameter_names"
    ) as mock_verify_parameter_names:
        test_data = load_json("test/test_data/test_coverages_proto.json")
        compare_data = load_json("test/test_data/test_coverages_covjson.json")

        mock_verify_parameter_names.return_value = None
        mock_get_obs_request.return_value = create_mock_obs_response(test_data)

        response = client.get(
            "/collections/observations/area?coords=POLYGON((4.0 52.4,5.8 52.4,5.8 52.6,4.0 52.6,4.0 52.4))&"
            "datetime=2022-12-31T00:00:00Z&parameter-name=air_temperature:2.0:mean:PT1M"
        )

        # Check that getObsRequest gets called with correct arguments given in query
        mock_get_obs_request.assert_called_once()
        m_args = mock_get_obs_request.call_args[0][0]

        assert {"air_temperature:2.0:mean:PT1M"}.issubset(set(m_args.filter["parameter_name"].values))
        assert len(m_args.spatial_polygon.points) == 5
        assert m_args.spatial_polygon.points[0].lon == 4.0
        assert "2022-12-31 00:00:00" == m_args.temporal_interval.start.ToDatetime().strftime("%Y-%m-%d %H:%M:%S")
        assert "2022-12-31 00:00:01" == m_args.temporal_interval.end.ToDatetime().strftime("%Y-%m-%d %H:%M:%S")

        assert response.status_code == 200
        assert response.json() == compare_data


def test_get_area_with_without_parameter_names_query():
    with patch("routers.edr.get_obs_request") as mock_get_obs_request:
        test_data = load_json("test/test_data/test_coverages_proto.json")
        compare_data = load_json("test/test_data/test_coverages_covjson.json")

        mock_get_obs_request.return_value = create_mock_obs_response(test_data)

        response = client.get(
            "/collections/observations/area?coords=POLYGON((4.0 52.4,5.8 52.4,5.8 52.6,4.0 52.6,4.0 52.4))"
        )

        mock_get_obs_request.assert_called_once()
        m_args = mock_get_obs_request.call_args[0][0]

        assert "instrument" not in m_args.filter
        response.status_code == 200
        response.json() == compare_data


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
        "routers.edr.get_obs_request"
    ) as mock_get_obs_request, patch("routers.edr.verify_parameter_names") as mock_verify_parameter_names:
        test_data = load_json("test/test_data/test_coverages_proto.json")
        compare_data = load_json("test/test_data/test_coverages_covjson.json")

        mock_verify_parameter_names.return_value = None
        mock_get_obs_request.return_value = create_mock_obs_response(test_data)

        response = client.get(
            "/collections/observations/position?coords=POINT(5.179705 52.0988218)"
            "&parameter-name=air_temperature:2.0:mean:PT1M&datetime=2022-12-31T00:00Z/2022-12-31T00:00Z"
        )

        mock_get_data_area.assert_called_once()
        mock_get_data_area.assert_called_with(
            coords="POLYGON ((5.179805 52.0988218, 5.179705 52.0987218, 5.1796050000000005 52.0988218, "
            "5.179705 52.09892180000001, 5.179805 52.0988218))",
            parameter_name="air_temperature:2.0:mean:PT1M",
            datetime="2022-12-31T00:00Z/2022-12-31T00:00Z",
            f="CoverageJSON",
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
