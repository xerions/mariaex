defmodule StartTest do
  use ExUnit.Case, async: true

  test "connection_errors" do
    Process.flag :trap_exit, true
    assert {:error, {%Mariaex.Error{mariadb: %{message: "Unknown database 'non_existing'"}}, _}} =
      Mariaex.Connection.start_link(username: "mariaex_user", password: "mariaex_pass", database: "non_existing", sync_connect: true, backoff_type: :stop)
    assert {:error, {%Mariaex.Error{mariadb: %{message: "Access denied for user " <> _}}, _}} =
      Mariaex.Connection.start_link(username: "non_existing", database: "mariaex_test", sync_connect: true, backoff_type: :stop)
    assert {:error, {%Mariaex.Error{message: "tcp connect: econnrefused"}, _}} =
      Mariaex.Connection.start_link(username: "mariaex_user", password: "mariaex_pass", database: "mariaex_test", port: 60999, sync_connect: true, backoff_type: :stop)
  end

  ## Tests tagged with :ssl_tests are excluded from running by default (see test_helper.exs)
  ## as they require that your Mariaex/MySQL server instance be configured for SSL logins:
  ## https://dev.mysql.com/doc/refman/5.7/en/creating-ssl-files-using-openssl.html
  @tag :ssl_tests
  test "ssl_connection_errors" do
    test_opts = [username: "mariaex_user",
                     password: "mariaex_pass",
                     database: "mariaex_test",
                     sync_connect: true,
                     ssl: true,
                     ssl_opts: [cacertfile: "",
                                verify: :verify_peer,
                                versions: [:"tlsv1.2"]],
                     backoff_type: :stop]

    Process.flag :trap_exit, true
    assert {:error, {%Mariaex.Error{message: "failed to upgraded socket: {:tls_alert, 'unknown ca'}"}, _}} =
      Mariaex.Connection.start_link(test_opts)
    assert {:error, {%Mariaex.Error{message: "failed to upgraded socket: {:options, {:cacertfile, []}}"}, _}}  =
      Mariaex.Connection.start_link(Keyword.put(test_opts, :ssl_opts, Keyword.drop(test_opts[:ssl_opts], [:cacertfile])))
  end

  @tag :socket
  test "unix domain socket connection" do
    parent = self()
    test_opts = [
      username: "mariaex_user",
      password: "mariaex_pass",
      database: "mariaex_test",
      sync_connect: true,
      socket: System.get_env("MDBSOCKET") || "/tmp/mysql.sock",
      backoff_type: :stop,
      after_connect: fn _ -> send(parent, :hi) end
    ]

    assert {:ok, _} = Mariaex.start_link(test_opts)
    assert_receive :hi
  end
end
