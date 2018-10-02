defmodule GeometryTest do
  use ExUnit.Case, async: true
  import Mariaex.TestHelper

  setup do
    opts = [
      database: "mariaex_test",
      username: "mariaex_user",
      password: "mariaex_pass",
      cache_size: 2,
      backoff_type: :stop
    ]

    {:ok, pid} = Mariaex.Connection.start_link(opts)

    # remove all modes for this session to have the same behaviour on different versions of mysql/mariadb
    {:ok, _} = Mariaex.Connection.query(pid, "SET SESSION sql_mode = \"\";")
    {:ok, [pid: pid]}
  end

  @tag :geometry
  test "inserts point with geometry column type", context do
    table = "geometry_test_insert_point_geometry_type"
    :ok = query("CREATE TABLE #{table} (id serial, point geometry)", [])

    point = %Mariaex.Geometry.Point{srid: 0, coordinates: {1, 1}}

    :ok = query(~s{INSERT INTO #{table} (id, point) VALUES (?, ?)}, [1, point])

    assert query("SELECT point from #{table} WHERE id = ?", [1]) == [
             [%Mariaex.Geometry.Point{coordinates: {1.0, 1.0}, srid: 0}]
           ]
  end

  @tag :geometry
  test "inserts point with point column type", context do
    table = "geometry_test_insert_point_point_type"
    :ok = query("CREATE TABLE #{table} (id serial, point point)", [])

    point = %Mariaex.Geometry.Point{srid: 42, coordinates: {1, -1}}

    :ok = query(~s{INSERT INTO #{table} (id, point) VALUES (?, ?)}, [1, point])

    assert query("SELECT point from #{table} WHERE id = ?", [1]) == [
             [%Mariaex.Geometry.Point{coordinates: {1.0, -1.0}, srid: 42}]
           ]
  end

  @tag :geometry
  test "inserts point with floats with more precision", context do
    table = "geometry_test_insert_point_with_more_precision"
    :ok = query("CREATE TABLE #{table} (id serial, point geometry)", [])

    point = %Mariaex.Geometry.Point{
      srid: 42,
      coordinates: {51.43941067083624, -0.2010107200625902}
    }

    :ok = query(~s{INSERT INTO #{table} (id, point) VALUES (?, ?)}, [1, point])

    assert query("SELECT point from #{table} WHERE id = ?", [1]) == [[point]]
  end

  @tag :geometry
  test "selects point with geometry column type", context do
    table = "geometry_test_select_point_geometry_type"
    :ok = query("CREATE TABLE #{table} (id serial, point geometry)", [])

    :ok =
      query(~s{INSERT INTO #{table} (id, point) VALUES (?, ST_GeomFromText(?))}, [1, "POINT(1 1)"])

    assert query("SELECT point from #{table} WHERE id = ?", [1]) == [
             [%Mariaex.Geometry.Point{srid: 0, coordinates: {1.0, 1.0}}]
           ]
  end

  @tag :geometry
  test "selects point with point column type", context do
    table = "geometry_test_select_point_point_type"
    :ok = query("CREATE TABLE #{table} (id serial, point point)", [])

    :ok =
      query(~s{INSERT INTO #{table} (id, point) VALUES (?, ST_GeomFromText(?))}, [1, "POINT(1 1)"])

    assert query("SELECT point from #{table} WHERE id = ?", [1]) == [
             [%Mariaex.Geometry.Point{srid: 0, coordinates: {1.0, 1.0}}]
           ]
  end

  @tag :geometry
  test "point with WGS84 srid and negative coordinates", context do
    table = "geometry_test_insert_point_wgs_srid"
    :ok = query("CREATE TABLE #{table} (id serial, point geometry)", [])

    point = %Mariaex.Geometry.Point{srid: 4326, coordinates: {-1, -1}}

    :ok = query(~s{INSERT INTO #{table} (id, point) VALUES (?, ?)}, [1, point])

    assert query("SELECT point from #{table} WHERE id = ?", [1]) == [
             [%Mariaex.Geometry.Point{coordinates: {-1.0, -1.0}, srid: 4326}]
           ]
  end

  @tag :geometry
  test "nil srid gets read as 0", context do
    table = "geometry_test_insert_point_nil_srid"
    :ok = query("CREATE TABLE #{table} (id serial, point geometry)", [])
    point = %Mariaex.Geometry.Point{coordinates: {1, 1}, srid: nil}

    :ok = query(~s{INSERT INTO #{table} (id, point) VALUES (?, ?)}, [1, point])

    assert query("SELECT point from #{table} WHERE id = ?", [1]) == [
             [%Mariaex.Geometry.Point{coordinates: {1.0, 1.0}, srid: 0}]
           ]
  end

  @tag :geometry
  test "inserts linestring with geometry column type", context do
    table = "geometry_test_insert_linestring_geometry_type"
    :ok = query("CREATE TABLE #{table} (id serial, linestring geometry)", [])

    linestring = %Mariaex.Geometry.LineString{
      coordinates: [{0.0, 0.0}, {10.0, 10.0}, {20.0, 25.0}, {50.0, 60.0}],
      srid: 42
    }

    :ok = query(~s{INSERT INTO #{table} (id, linestring) VALUES (?, ?)}, [1, linestring])

    assert query("SELECT linestring from #{table} WHERE id = ?", [1]) == [
             [
               %Mariaex.Geometry.LineString{
                 coordinates: [{0.0, 0.0}, {10.0, 10.0}, {20.0, 25.0}, {50.0, 60.0}],
                 srid: 42
               }
             ]
           ]
  end

  @tag :geometry
  test "inserts linestring with linestring column type", context do
    table = "geometry_test_insert_linestring_linestring_type"
    :ok = query("CREATE TABLE #{table} (id serial, linestring linestring)", [])

    linestring = %Mariaex.Geometry.LineString{
      coordinates: [{0.0, 0.0}, {10.0, 10.0}, {20.0, 25.0}, {50.0, 60.0}],
      srid: nil
    }

    :ok = query(~s{INSERT INTO #{table} (id, linestring) VALUES (?, ?)}, [1, linestring])

    assert query("SELECT linestring from #{table} WHERE id = ?", [1]) == [
             [
               %Mariaex.Geometry.LineString{
                 coordinates: [{0.0, 0.0}, {10.0, 10.0}, {20.0, 25.0}, {50.0, 60.0}],
                 srid: 0
               }
             ]
           ]
  end

  @tag :geometry
  test "selects linestring with geometry column type", context do
    table = "geometry_test_select_linestring_geometry_type"
    :ok = query("CREATE TABLE #{table} (id serial, linestring geometry)", [])

    :ok =
      query(~s{INSERT INTO #{table} (id, linestring) VALUES (?, ST_GeomFromText(?))}, [
        1,
        "LINESTRING(0 0, 10 10, 20 25, 50 60)"
      ])

    assert query("SELECT linestring from #{table} WHERE id = ?", [1]) == [
             [
               %Mariaex.Geometry.LineString{
                 coordinates: [{0.0, 0.0}, {10.0, 10.0}, {20.0, 25.0}, {50.0, 60.0}],
                 srid: 0
               }
             ]
           ]
  end

  @tag :geometry
  test "inserts polygon", context do
    table = "geometry_test_insert_polygon"
    :ok = query("CREATE TABLE #{table} (id serial, polygon geometry)", [])

    polygon = %Mariaex.Geometry.Polygon{
      coordinates: [
        [{0.0, 0.0}, {10.0, 0.0}, {10.0, 10.0}, {0.0, 10.0}, {0.0, 0.0}],
        [{5.0, 5.0}, {7.0, 5.0}, {7.0, 7.0}, {5.0, 7.0}, {5.0, 5.0}]
      ],
      srid: 42
    }

    :ok = query(~s{INSERT INTO #{table} (id, polygon) VALUES (?, ?)}, [1, polygon])

    assert query("SELECT polygon from #{table} WHERE id = ?", [1]) == [[polygon]]
  end

  @tag :geometry
  test "selects polygon", context do
    table = "geometry_test_select_polygon"
    :ok = query("CREATE TABLE #{table} (id serial, polygon geometry)", [])

    :ok =
      query(~s{INSERT INTO #{table} (id, polygon) VALUES (?, ST_GeomFromText(?))}, [
        1,
        "POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))"
      ])

    assert query("SELECT polygon from #{table} WHERE id = ?", [1]) == [
             [
               %Mariaex.Geometry.Polygon{
                 coordinates: [
                   [{0.0, 0.0}, {10.0, 0.0}, {10.0, 10.0}, {0.0, 10.0}, {0.0, 0.0}],
                   [{5.0, 5.0}, {7.0, 5.0}, {7.0, 7.0}, {5.0, 7.0}, {5.0, 5.0}]
                 ],
                 srid: 0
               }
             ]
           ]
  end

  @tag :geometry
  test "inserts large polygon", context do
    table = "geometry_test_insert_large_polygon"
    :ok = query("CREATE TABLE #{table} (id serial, polygon geometry)", [])

    polygon = %Mariaex.Geometry.Polygon{
      coordinates: [
        [
          {4.375889897346497, 51.20387196361823},
          {4.381211400032043, 51.19935444755919},
          {4.393399357795715, 51.19709552339094},
          {4.399235844612122, 51.19515921451808},
          {4.40902054309845, 51.19257734276737},
          {4.416401982307434, 51.19182426958834},
          {4.426015019416809, 51.19300766477249},
          {4.434083104133606, 51.19903175088785},
          {4.438546299934387, 51.20494749738101},
          {4.445756077766418, 51.21312073325066},
          {4.449189305305481, 51.21849707104884},
          {4.449532628059387, 51.22226013396642},
          {4.44197952747345, 51.22763540452407},
          {4.430821537971497, 51.2349447650688},
          {4.424641728401184, 51.23698687887106},
          {4.421551823616028, 51.2403185541701},
          {4.410393834114075, 51.24128576954669},
          {4.40352737903595, 51.24171563651943},
          {4.397862553596497, 51.23924384656603},
          {4.399750828742981, 51.23311753378148},
          {4.398205876350403, 51.2251628580361},
          {4.375889897346497, 51.20387196361823}
        ]
      ],
      srid: 0
    }

    :ok = query(~s{INSERT INTO #{table} (id, polygon) VALUES (?, ?)}, [1, polygon])

    assert query("SELECT polygon from #{table} WHERE id = ?", [1]) == [[polygon]]

    polygon = %Mariaex.Geometry.Polygon{
      coordinates: [
        [
          {4.375889897346497, 51.20387196361823},
          {4.381211400032043, 51.19935444755919},
          {4.399750828742981, 51.23311753378148},
          {4.398205876350403, 51.2251628580361},
          {4.375889897346497, 51.20387196361823}
        ]
      ],
      srid: 0
    }

    :ok = query(~s{INSERT INTO #{table} (id, polygon) VALUES (?, ?)}, [2, polygon])

    assert query("SELECT polygon from #{table} WHERE id = ?", [2]) == [[polygon]]
  end
end
