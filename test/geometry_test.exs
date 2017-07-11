defmodule GeometryTest do
  use ExUnit.Case, async: true
  import Mariaex.TestHelper

  setup do
    opts = [database: "mariaex_test", username: "mariaex_user", password: "mariaex_pass", cache_size: 2, backoff_type: :stop]
    {:ok, pid} = Mariaex.Connection.start_link(opts)
    # remove all modes for this session to have the same behaviour on different versions of mysql/mariadb
    {:ok, _} = Mariaex.Connection.query(pid, "SET SESSION sql_mode = \"\";")
    {:ok, [pid: pid]}
  end

  test "inserts point type with geometry column type", context do
    table = "geometry_test_insert_point_geometry_type"
    :ok = query("CREATE TABLE #{table} (id serial, point geometry)", [])

    point = %Mariaex.Geometry.Point{srid: 0, coordinates: {1, 1}}

    :ok = query(~s{INSERT INTO #{table} (id, point) VALUES (?, ?)}, [1, point])

    assert query("SELECT point from #{table} WHERE id = ?", [1]) == [[%Mariaex.Geometry.Point{coordinates: {1.0, 1.0}, srid: 0}]]
  end

  test "inserts point type with point column type", context do
    table = "geometry_test_insert_point_point_type"
    :ok = query("CREATE TABLE #{table} (id serial, point point)", [])

    point = %Mariaex.Geometry.Point{srid: 42, coordinates: {1, -1}}

    :ok = query(~s{INSERT INTO #{table} (id, point) VALUES (?, ?)}, [1, point])

    assert query("SELECT point from #{table} WHERE id = ?", [1]) == [[%Mariaex.Geometry.Point{coordinates: {1.0, -1.0}, srid: 42}]]
  end

  test "selects point type with geometry column type", context do
    table = "geometry_test_select_point_geometry_type"
    :ok = query("CREATE TABLE #{table} (id serial, point geometry)", [])
    :ok = query(~s{INSERT INTO #{table} (id, point) VALUES (?, ST_GeomFromText(?))}, [1, "POINT(1 1)"])

    assert query("SELECT point from #{table} WHERE id = ?", [1]) == [[%Mariaex.Geometry.Point{srid: 0, coordinates: {1.0, 1.0}}]]
  end

  test "selects point type with point column type", context do
    table = "geometry_test_select_point_point_type"
    :ok = query("CREATE TABLE #{table} (id serial, point point)", [])
    :ok = query(~s{INSERT INTO #{table} (id, point) VALUES (?, ST_GeomFromText(?))}, [1, "POINT(1 1)"])

    assert query("SELECT point from #{table} WHERE id = ?", [1]) == [[%Mariaex.Geometry.Point{srid: 0, coordinates: {1.0, 1.0}}]]
  end

  test "point with WGS84 srid and negative coordinates", context do
    table = "geometry_test_insert_point_wgs_srid"
    :ok = query("CREATE TABLE #{table} (id serial, point geometry)", [])

    point = %Mariaex.Geometry.Point{srid: 4326, coordinates: {-1, -1}}

    :ok = query(~s{INSERT INTO #{table} (id, point) VALUES (?, ?)}, [1, point])

    assert query("SELECT point from #{table} WHERE id = ?", [1]) == [[%Mariaex.Geometry.Point{coordinates: {-1.0, -1.0}, srid: 4326}]]
  end

  test "nil srid gets read as 0", context do
    table = "geometry_test_insert_point_nil_srid"
    :ok = query("CREATE TABLE #{table} (id serial, point geometry)", [])
    point = %Mariaex.Geometry.Point{coordinates: {1, 1}, srid: nil}

    :ok = query(~s{INSERT INTO #{table} (id, point) VALUES (?, ?)}, [1, point])

    assert query("SELECT point from #{table} WHERE id = ?", [1]) == [[%Mariaex.Geometry.Point{coordinates: {1.0, 1.0}, srid: 0}]]
  end
end
