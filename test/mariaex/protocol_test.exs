defmodule Mariaex.ProtocolTest do
  use ExUnit.Case
  import ExUnit.CaptureLog

  describe "Integration test" do
    test "Expect to disconnect from the database when it goes down" do
      Process.flag(:trap_exit, true)
      opts = [database: "information_schema", username: "mariaex_user", password: "mariaex_pass", backoff_type: :stop, sync_connect: true]

      log = capture_log(fn ->
        assert {:ok, pid} = Mariaex.start_link(opts)
        # Restart the docker container to assure that mariadb will stop
        # responding to ping(s)
        System.cmd("docker", ["restart", "mariadb"])
      end)

      # Check if we disconnected
      assert log =~ "disconnected"
    end
  end
end
