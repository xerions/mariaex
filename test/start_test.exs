defmodule StartTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  test "connection_errors" do
    Process.flag(:trap_exit, true)
    opts = [sync_connect: true, backoff_type: :stop, max_restarts: 0]

    assert capture_log(fn ->
      {:ok, pid} = Mariaex.start_link([username: "mariaex_user", password: "mariaex_pass", database: "non_existing"] ++ opts)
      assert_receive {:EXIT, ^pid, :killed}, 5000
    end) =~ "** (Mariaex.Error) (1049): Unknown database 'non_existing'"

    assert capture_log(fn ->
      {:ok, pid} = Mariaex.start_link([username: "non_existing", database: "mariaex_test"] ++ opts)
      assert_receive {:EXIT, ^pid, :killed}, 5000
    end) =~ "** (Mariaex.Error) (1045): Access denied for user 'non_existing'"

    assert capture_log(fn ->
      {:ok, pid} = Mariaex.start_link([username: "mariaex_user", password: "mariaex_pass", database: "mariaex_test", port: 60999] ++ opts)
      assert_receive {:EXIT, ^pid, :killed}, 5000
    end) =~ "** (Mariaex.Error) tcp connect: econnrefused"
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
