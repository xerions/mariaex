ExUnit.start()

run_cmd = fn cmd ->
  key = :ecto_setup_cmd_output
  Process.put(key, "")
  status = Mix.Shell.cmd(cmd, fn(data) ->
    current = Process.get(key)
    Process.put(key, current <> data)
  end)
  output = Process.get(key)
  Process.put(key, "")
  {status, output}
end

sql = """
  CREATE TABLE test1 (id serial, title text);
  INSERT INTO test1 VALUES(1, 'test');
  INSERT INTO test1 VALUES(2, 'test2');
  DROP TABLE test1;
"""

cmds = [
  ~s(mysql -u root -e "GRANT ALL ON *.* TO 'mariaex_user'@'localhost' IDENTIFIED BY 'mariaex_pass';"),
  ~s(mysql -u root -e "DROP DATABASE IF EXISTS mariaex_test;"),
  ~s(mysql -u root -e "CREATE DATABASE mariaex_test DEFAULT CHARACTER SET 'utf8' COLLATE 'utf8_general_ci'";),
  ~s(mysql -u root mariaex_test -e "#{sql}"),
  ~s(mysql -u mariaex_user -pmariaex_pass mariaex_test -e "#{sql}")
]

Enum.each(cmds, fn cmd ->
  {status, output} = run_cmd.(cmd)

  if status != 0 do
    IO.puts """
    Command:
    #{cmd}
    error'd with:
    #{output}
    Please verify the user "root" exists and it has permissions to
    create databases and users.
    """
    System.halt(1)
  end
end)

defmodule Mariaex.TestHelper do
  defmacro query(stat, params, opts \\ []) do
    quote do
      case Mariaex.Connection.query(var!(context)[:pid], unquote(stat),
                                     unquote(params), unquote(opts)) do
        {:ok, %Mariaex.Result{rows: nil}} -> :ok
        {:ok, %Mariaex.Result{rows: rows}} -> rows
        {:error, %Mariaex.Error{} = err} -> err
      end
    end
  end
end